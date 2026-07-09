use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionState;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use futures::stream::once;
use object_store::ObjectStoreExt;
use sail_common_datafusion::catalog::LakehouseExecutionContext;

use crate::catalog_support::commit_helper::{commit_iceberg_changes, CommitResult};
use crate::io::StoreContext;
use crate::operations::snapshot::SnapshotProduceOperation;
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::operations::{SnapshotProducer, Transaction};
use crate::spec::manifest_list::ManifestFile;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, Manifest, ManifestEntry,
    ManifestList, ManifestStatus, PartitionSpec, TableMetadata,
};
use crate::table::find_latest_metadata_file;
use crate::table::metadata_loader::{load_metadata_file_bytes, metadata_file_version_from_path};
use crate::utils::get_object_store_from_context;
use crate::utils::metadata::metadata_files_for_version;

const MAX_COMMIT_RETRIES: usize = 5;
const DEFAULT_TARGET_FILE_SIZE: u64 = 128 * 1024 * 1024; // 128 MB
const SMALL_FILE_THRESHOLD: f64 = 0.75; // Files below 75% of target are candidates

/// A batch of small files to compact into one output file.
struct CompactBatch {
    partition_dir: String,
    partition_values: Vec<Option<crate::spec::types::values::Literal>>,
    files: Vec<DataFile>,
    total_size: u64,
    partition_spec_id: i32,
}

#[derive(Debug)]
pub struct IcebergCompactExec {
    table_url: String,
    target_file_size: u64,
    schema: SchemaRef,
    session_state: SessionState,
    lakehouse_table: Option<LakehouseExecutionContext>,
    table_properties: Vec<(String, String)>,
    cache: Arc<PlanProperties>,
}

impl IcebergCompactExec {
    pub fn new(
        table_url: String,
        target_file_size: Option<u64>,
        session_state: SessionState,
        lakehouse_table: Option<LakehouseExecutionContext>,
        table_properties: Vec<(String, String)>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let cache = Self::compute_properties(schema.clone());
        Self {
            table_url,
            target_file_size: target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE),
            schema,
            session_state,
            lakehouse_table,
            table_properties,
            cache,
        }
    }

    pub fn table_url(&self) -> &str {
        &self.table_url
    }

    pub fn target_file_size(&self) -> u64 {
        self.target_file_size
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.lakehouse_table.as_ref()
    }

    pub fn table_properties(&self) -> &[(String, String)] {
        &self.table_properties
    }

    fn compute_properties(schema: SchemaRef) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl DisplayAs for IcebergCompactExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergCompactExec(table={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: iceberg")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for IcebergCompactExec {
    fn name(&self) -> &'static str {
        "IcebergCompactExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let table_url = self.table_url.clone();
        let target_file_size = self.target_file_size;
        let schema = self.schema();
        let session_state = self.session_state.clone();
        let lakehouse_table = self.lakehouse_table.clone();
        let table_properties = self.table_properties.clone();
        let future = async move {
            let table_url_parsed = url::Url::parse(&table_url)
                .map_err(|e| DataFusionError::Plan(format!("Invalid URL: {e}")))?;
            let object_store = get_object_store_from_context(&context, &table_url_parsed)?;

            run_compaction(
                &table_url_parsed,
                object_store,
                target_file_size,
                &session_state,
                Some(&context),
                lakehouse_table.as_ref(),
                &table_properties,
            )
            .await?;

            let array = Arc::new(UInt64Array::from(vec![0u64]));
            let batch = RecordBatch::try_new(schema, vec![array])
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            Ok(batch)
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Run table compaction on the specified Iceberg table.
/// Returns Ok(()) on success.
pub(crate) async fn run_compaction(
    table_url: &url::Url,
    object_store: Arc<dyn object_store::ObjectStore>,
    target_file_size: u64,
    _session_state: &SessionState,
    context: Option<&Arc<TaskContext>>,
    lakehouse_table: Option<&LakehouseExecutionContext>,
    table_properties: &[(String, String)],
) -> Result<()> {
    let store_ctx = StoreContext::new(object_store.clone(), table_url)?;

    let latest_meta = find_latest_metadata_file(&object_store, table_url).await?;
    let bytes = load_metadata_file_bytes(&object_store, &latest_meta).await?;
    let table_meta =
        TableMetadata::from_json(&bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;

    let snapshot = table_meta
        .current_snapshot()
        .cloned()
        .ok_or_else(|| DataFusionError::Plan("No current snapshot found".to_string()))?;
    let format_version = table_meta.format_version;
    let iceberg_schema = table_meta
        .current_schema()
        .cloned()
        .ok_or_else(|| DataFusionError::Plan("No current schema".to_string()))?;
    let partition_spec = table_meta
        .default_partition_spec()
        .cloned()
        .unwrap_or_else(PartitionSpec::unpartitioned_spec);

    let tx = Transaction::new(table_url.to_string(), snapshot.clone());

    // Load manifest list and categorize files
    let parent_list = IcebergCompactExec::load_manifest_list(
        &store_ctx,
        snapshot.manifest_list(),
        format_version,
    )
    .await?;

    let mut large_files: Vec<DataFile> = Vec::new();
    let mut small_files: Vec<DataFile> = Vec::new();

    for manifest_file in parent_list.entries() {
        let entries =
            IcebergCompactExec::load_manifest_entries(&store_ctx, manifest_file, format_version)
                .await?;
        for entry in &entries {
            if entry.status == ManifestStatus::Deleted {
                continue;
            }
            let df = &entry.data_file;
            if df.file_size_in_bytes >= (target_file_size as f64 * SMALL_FILE_THRESHOLD) as u64 {
                large_files.push(df.clone());
            } else {
                small_files.push(df.clone());
            }
        }
    }

    if small_files.is_empty() {
        return Ok(());
    }

    let batches =
        IcebergCompactExec::bin_pack_files(&small_files, target_file_size, &partition_spec);

    let mut rewritten_files: Vec<DataFile> = Vec::new();
    for compact_batch in &batches {
        let new_file = IcebergCompactExec::merge_and_write(
            &store_ctx,
            compact_batch,
            table_url.as_str(),
            &iceberg_schema,
        )
        .await?;
        rewritten_files.push(new_file);
    }

    // Build commit with retry loop
    let mut attempt = 0;
    loop {
        attempt += 1;
        let current_latest = if attempt == 1 {
            latest_meta.clone()
        } else {
            find_latest_metadata_file(&object_store, table_url).await?
        };

        let current_bytes = load_metadata_file_bytes(&object_store, &current_latest).await?;
        let mut current_table_meta = TableMetadata::from_json(&current_bytes)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if attempt > 1 {
            let current_snapshot_id = current_table_meta
                .current_snapshot()
                .map(|s| s.snapshot_id());
            if current_snapshot_id != Some(snapshot.snapshot_id()) {
                if attempt >= MAX_COMMIT_RETRIES {
                    return Err(DataFusionError::Execution(
                        "Compaction failed: concurrent modification".to_string(),
                    ));
                }
                continue;
            }
        }

        let current_version = metadata_file_version_from_path(&current_latest).unwrap_or(0);
        let next_version = current_version + 1;
        let existing_for_next = metadata_files_for_version(&store_ctx, next_version).await?;
        if !existing_for_next.is_empty() {
            if attempt >= MAX_COMMIT_RETRIES {
                return Err(DataFusionError::Execution(
                    "Compaction commit conflict".to_string(),
                ));
            }
            continue;
        }

        let current_schema = current_table_meta
            .current_schema()
            .cloned()
            .ok_or_else(|| DataFusionError::Plan("No current schema".to_string()))?;

        let manifest_meta = tx.default_manifest_metadata(
            &current_schema,
            &partition_spec,
            current_table_meta.format_version,
        );

        let mut all_data_files = large_files.clone();
        all_data_files.extend(rewritten_files.clone());

        let producer = SnapshotProducer::new(
            &tx,
            all_data_files,
            Some(store_ctx.clone()),
            Some(manifest_meta),
        );

        struct CompactOperation;
        impl SnapshotProduceOperation for CompactOperation {
            fn operation(&self) -> &'static str {
                "replace"
            }
        }

        let action_commit = producer
            .commit(CompactOperation)
            .await
            .map_err(DataFusionError::Execution)?;

        match commit_iceberg_changes(
            context,
            &store_ctx,
            table_url,
            &mut current_table_meta,
            action_commit,
            lakehouse_table,
            table_properties,
            &current_latest,
            None,
        )
        .await
        {
            Ok(CommitResult::Committed { .. }) => {
                return Ok(());
            }
            Err(e) => {
                if attempt >= MAX_COMMIT_RETRIES {
                    return Err(e);
                }
                continue;
            }
        }
    }
}

impl IcebergCompactExec {
    async fn load_manifest_list(
        store_ctx: &StoreContext,
        manifest_list_path: &str,
        format_version: FormatVersion,
    ) -> Result<ManifestList> {
        if manifest_list_path.is_empty() {
            return Ok(ManifestList { entries: vec![] });
        }
        let (store_ref, resolved_path) = store_ctx
            .resolve(manifest_list_path)
            .map_err(|e| DataFusionError::Execution(format!("{e}")))?;
        let data = store_ref
            .get(&resolved_path)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to get manifest list: {e}")))?
            .bytes()
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to read manifest list: {e}"))
            })?;
        ManifestList::parse_with_version(&data, format_version)
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse manifest list: {e}")))
    }

    async fn load_manifest_entries(
        store_ctx: &StoreContext,
        manifest_file: &ManifestFile,
        _format_version: FormatVersion,
    ) -> Result<Vec<std::sync::Arc<ManifestEntry>>> {
        let (store_ref, resolved_path) = store_ctx
            .resolve(&manifest_file.manifest_path)
            .map_err(|e| DataFusionError::Execution(format!("{e}")))?;
        let data = store_ref
            .get(&resolved_path)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to get manifest: {e}")))?
            .bytes()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to read manifest: {e}")))?;
        let manifest = Manifest::parse_avro(&data)
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse manifest: {e}")))?;
        Ok(manifest.entries)
    }

    /// Bin-pack small files into compaction batches grouped by partition.
    fn bin_pack_files(
        files: &[DataFile],
        target_file_size: u64,
        _partition_spec: &PartitionSpec,
    ) -> Vec<CompactBatch> {
        use std::collections::HashMap;

        // Group files by partition directory (string representation)
        let mut groups: HashMap<String, Vec<&DataFile>> = HashMap::new();
        for file in files {
            let key = format!("{:?}", file.partition);
            groups.entry(key).or_default().push(file);
        }

        let mut batches: Vec<CompactBatch> = Vec::new();
        for (_partition_key, group_files) in groups {
            // Sort by file size descending for better bin-packing
            let mut sorted = group_files;
            sorted.sort_by(|a, b| b.file_size_in_bytes.cmp(&a.file_size_in_bytes));

            let mut current_batch = CompactBatch {
                partition_dir: String::new(),
                partition_values: Vec::new(),
                files: Vec::new(),
                total_size: 0,
                partition_spec_id: 0,
            };

            for file in sorted {
                if current_batch.total_size + file.file_size_in_bytes > target_file_size
                    && !current_batch.files.is_empty()
                {
                    // Finalize current batch
                    let batch = std::mem::replace(
                        &mut current_batch,
                        CompactBatch {
                            partition_dir: String::new(),
                            partition_values: Vec::new(),
                            files: Vec::new(),
                            total_size: 0,
                            partition_spec_id: 0,
                        },
                    );
                    batches.push(batch);
                }

                if current_batch.files.is_empty() {
                    current_batch.partition_values = file.partition.clone();
                    current_batch.partition_spec_id = file.partition_spec_id;
                }
                current_batch.files.push(file.clone());
                current_batch.total_size += file.file_size_in_bytes;
            }

            if !current_batch.files.is_empty() {
                batches.push(current_batch);
            }
        }

        batches
    }

    /// Merge small files in a batch into a single large file.
    async fn merge_and_write(
        store_ctx: &StoreContext,
        batch: &CompactBatch,
        _table_url: &str,
        _iceberg_schema: &crate::spec::Schema,
    ) -> Result<DataFile> {
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut file_schema: Option<SchemaRef> = None;

        for df in &batch.files {
            let (store_ref, resolved_path) = store_ctx
                .resolve(&df.file_path)
                .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

            let store_path = object_store::path::Path::from(resolved_path.as_ref());
            let file_size = df.file_size_in_bytes;

            let reader = parquet::arrow::async_reader::ParquetObjectReader::new(
                store_ref.clone(),
                store_path.clone(),
            )
            .with_file_size(file_size);

            let builder =
                parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Failed to open Parquet: {e}"))
                    })?;

            if file_schema.is_none() {
                file_schema = Some(builder.schema().clone());
            }

            let stream = builder
                .build()
                .map_err(|e| DataFusionError::Execution(format!("Failed to build stream: {e}")))?;
            let mut stream = std::pin::pin!(stream);
            use futures::StreamExt;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result
                    .map_err(|e| DataFusionError::Execution(format!("Parquet read: {e}")))?;
                all_batches.push(batch);
            }
        }

        let file_schema = file_schema.ok_or_else(|| {
            DataFusionError::Execution("No schema found in compacted files".to_string())
        })?;

        // Concatenate all batches
        let merged_batch = if all_batches.len() > 1 {
            concat_batches(&file_schema, &all_batches)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
        } else if all_batches.len() == 1 {
            all_batches.into_iter().next().unwrap()
        } else {
            return Err(DataFusionError::Execution("No data to compact".to_string()));
        };

        // Write merged data as new Parquet file
        let mut writer = ArrowParquetWriter::try_new(
            &file_schema,
            parquet::file::properties::WriterProperties::default(),
        )
        .map_err(|e| DataFusionError::Execution(format!("Writer: {e}")))?;

        writer
            .write_batch(&merged_batch)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Write: {e}")))?;

        let (parquet_bytes, parquet_meta) = writer
            .close()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Close: {e}")))?;

        let file_name = format!("{}.parquet", uuid::Uuid::new_v4());
        let data_rel = format!("data/{}", file_name);
        let data_path = object_store::path::Path::from(data_rel.as_str());
        store_ctx
            .prefixed
            .put(&data_path, object_store::PutPayload::from(parquet_bytes))
            .await
            .map_err(|e| DataFusionError::Execution(format!("Write data file: {e}")))?;

        // Build DataFile metadata
        let num_rows = merged_batch.num_rows() as u64;
        let mut column_sizes = std::collections::HashMap::new();
        let mut value_counts = std::collections::HashMap::new();
        let mut null_value_counts = std::collections::HashMap::new();
        for (col_idx, _) in file_schema.fields().iter().enumerate() {
            let field_id = (col_idx + 1) as i32;
            column_sizes.insert(
                field_id,
                parquet_meta.file_size / (file_schema.fields().len() as u64).max(1),
            );
            value_counts.insert(field_id, num_rows);
            null_value_counts.insert(field_id, 0);
        }

        Ok(DataFile {
            content: DataContentType::Data,
            file_path: data_rel,
            file_format: DataFileFormat::Parquet,
            partition: batch.partition_values.clone(),
            record_count: num_rows,
            file_size_in_bytes: parquet_meta.file_size,
            column_sizes,
            value_counts,
            null_value_counts,
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: batch.partition_spec_id,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        })
    }
}

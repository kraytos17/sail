use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::scalar::ScalarValue;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::context::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::ObjectMeta;
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;
use url::Url;

use crate::io::StoreContext;
use crate::physical_plan::manifest_scan_exec::{COL_FILE_PATH, COL_FILE_SIZE_IN_BYTES};

/// How many files to accumulate before building a DataSourceExec scan batch.
const SCAN_CHUNK_FILES: usize = 1024;

/// State machine for the streaming scan-by-data-files loop.
struct ScanByDataFilesState {
    /// Upstream metadata stream (from IcebergManifestScanExec).
    input: SendableRecordBatchStream,
    /// Task execution context.
    context: Arc<TaskContext>,
    /// Table URL for object store resolution.
    table_url: Url,
    /// The Arrow schema of the actual user data.
    output_schema: SchemaRef,
    /// When set, each output row is tagged with its source file path in a column
    /// of this name (materialized via the Parquet scan partition columns).
    file_path_column: Option<String>,
    /// Pending file entries (path, size_in_bytes) accumulated from the metadata stream.
    pending_files: Vec<(String, u64)>,
    /// Currently active scan stream (draining Parquet data).
    current_scan: Option<SendableRecordBatchStream>,
    /// Whether the upstream input has been fully consumed.
    input_done: bool,
    /// Whether we've emitted at least one (possibly empty) batch.
    emitted_empty: bool,
}

impl ScanByDataFilesState {
    fn new(
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
        table_url: Url,
        output_schema: SchemaRef,
        file_path_column: Option<String>,
    ) -> Self {
        Self {
            input,
            context,
            table_url,
            output_schema,
            file_path_column,
            pending_files: Vec::new(),
            current_scan: None,
            input_done: false,
            emitted_empty: false,
        }
    }

    /// Extract file paths and sizes from a metadata RecordBatch.
    fn extract_file_info(&self, batch: &RecordBatch) -> Result<Vec<(String, u64)>> {
        let path_col = batch
            .column_by_name(COL_FILE_PATH)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "IcebergScanByDataFilesExec: missing or invalid '{}' column",
                    COL_FILE_PATH
                ))
            })?;

        let size_col = batch
            .column_by_name(COL_FILE_SIZE_IN_BYTES)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "IcebergScanByDataFilesExec: missing or invalid '{}' column",
                    COL_FILE_SIZE_IN_BYTES
                ))
            })?;

        let mut files = Vec::with_capacity(path_col.len());
        for i in 0..path_col.len() {
            if !path_col.is_null(i) {
                files.push((path_col.value(i).to_string(), size_col.value(i)));
            }
        }
        Ok(files)
    }

    /// Build and start a Parquet scan for the accumulated file entries.
    async fn build_next_scan(&mut self) -> Result<()> {
        if self.pending_files.is_empty() {
            return Ok(());
        }

        let files = std::mem::take(&mut self.pending_files);

        let object_store = self
            .context
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store, &self.table_url)?;

        // Build PartitionedFile entries using file size from manifest metadata,
        // avoiding a per-file HEAD request to the object store.
        // `last_modified` is not available from Iceberg manifest metadata, so we
        // use a placeholder (current time). DataFusion's Parquet reader uses this
        // field only for cache invalidation (ETag/mtime logic), which is not
        // exercised in this streaming path. The actual file size from the manifest
        // is accurate and is the only metadata field that matters for scan planning.
        let mut partitioned_files = Vec::with_capacity(files.len());
        for (raw_path, file_size) in &files {
            let file_path = store_ctx.resolve_to_absolute_path(raw_path)?;
            // The file path column must carry the EXACT string stored in the manifest
            // (`data_file.file_path()`), not a re-resolved object path. Row-level
            // operations compare this value against manifest paths when deciding which
            // files to rewrite, so the two must be identical.
            let partition_values = if self.file_path_column.is_some() {
                vec![ScalarValue::Utf8(Some(raw_path.clone()))]
            } else {
                vec![]
            };
            partitioned_files.push(PartitionedFile {
                object_meta: ObjectMeta {
                    location: file_path,
                    last_modified: chrono::Utc::now(),
                    size: *file_size,
                    e_tag: None,
                    version: None,
                },
                partition_values,
                range: None,
                statistics: None,
                ordering: None,
                extensions: Default::default(),
                metadata_size_hint: None,
                table_reference: None,
            });
        }

        let file_groups = vec![FileGroup::from(partitioned_files)];

        let object_store_url = ObjectStoreUrl::parse(&self.table_url[..url::Position::BeforePath])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Use session Parquet options for parity with the driver-based scan path.
        let parquet_options = TableParquetOptions {
            global: self
                .context
                .session_config()
                .options()
                .execution
                .parquet
                .clone(),
            ..Default::default()
        };

        // When a file path column is requested, materialize it through the Parquet
        // scan's partition columns: the file schema is the user data schema and the
        // file path is appended as a synthetic partition column.
        let parquet_source = if let Some(file_path_column) = &self.file_path_column {
            let file_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(
                self.output_schema
                    .fields()
                    .iter()
                    .filter(|f| f.name() != file_path_column)
                    .cloned()
                    .collect::<Vec<_>>(),
            ));
            let table_schema = TableSchema::new(
                file_schema,
                vec![Arc::new(datafusion::arrow::datatypes::Field::new(
                    file_path_column.clone(),
                    DataType::Utf8,
                    true,
                ))],
            );
            Arc::new(ParquetSource::new(table_schema).with_table_parquet_options(parquet_options))
        } else {
            let parquet_source = ParquetSource::new(Arc::clone(&self.output_schema))
                .with_table_parquet_options(parquet_options);
            Arc::new(parquet_source)
        };
        let parquet_source: Arc<dyn datafusion::datasource::physical_plan::FileSource> =
            parquet_source;

        let file_scan_config = FileScanConfigBuilder::new(object_store_url, parquet_source)
            .with_file_groups(file_groups)
            .with_expr_adapter(Some(Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {})
                as Arc<dyn PhysicalExprAdapterFactory>))
            .build();

        let scan_exec = DataSourceExec::from_data_source(file_scan_config);
        let output_schema = Arc::clone(&self.output_schema);

        // Execute all partitions of the scan and flatten into a single stream.
        let partitions = scan_exec
            .properties()
            .output_partitioning()
            .partition_count()
            .max(1);
        let mut scans = Vec::with_capacity(partitions);
        for partition in 0..partitions {
            scans.push(scan_exec.execute(partition, Arc::clone(&self.context))?);
        }
        let combined = stream::iter(scans)
            .map(Ok::<_, DataFusionError>)
            .try_flatten();

        self.current_scan = Some(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            combined,
        )));
        Ok(())
    }
}

/// Physical execution node that scans Iceberg data files based on file metadata
/// from the upstream `IcebergManifestScanExec`.
#[derive(Debug, Clone)]
pub struct IcebergScanByDataFilesExec {
    /// Upstream plan that produces file metadata (IcebergManifestScanExec).
    input: Arc<dyn ExecutionPlan>,
    /// Table URL for object store access.
    table_url: String,
    /// The Arrow schema of the actual user data.
    output_schema: SchemaRef,
    /// When set, each output row is tagged with its source file path in a column
    /// of this name (materialized via the Parquet scan partition columns).
    file_path_column: Option<String>,
    /// Cached plan properties.
    cache: Arc<PlanProperties>,
}

impl IcebergScanByDataFilesExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table_url: String, output_schema: SchemaRef) -> Self {
        Self::new_with_file_path_column(input, table_url, output_schema, None)
    }

    pub fn new_with_file_path_column(
        input: Arc<dyn ExecutionPlan>,
        table_url: String,
        output_schema: SchemaRef,
        file_path_column: Option<String>,
    ) -> Self {
        let output_schema = if let Some(column) = &file_path_column {
            let mut builder =
                datafusion::arrow::datatypes::SchemaBuilder::from(output_schema.as_ref().clone());
            builder.push(datafusion::arrow::datatypes::Field::new(
                column.clone(),
                DataType::Utf8,
                true,
            ));
            Arc::new(builder.finish())
        } else {
            output_schema
        };
        let partition_count = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            table_url: table_url.to_string(),
            output_schema,
            file_path_column,
            cache,
        }
    }

    pub fn file_path_column(&self) -> &Option<String> {
        &self.file_path_column
    }

    pub fn table_url(&self) -> &str {
        &self.table_url
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for IcebergScanByDataFilesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergScanByDataFilesExec: table_url={}",
                    self.table_url
                )
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for IcebergScanByDataFilesExec {
    fn name(&self) -> &str {
        "IcebergScanByDataFilesExec"
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("IcebergScanByDataFilesExec requires exactly one child");
        }
        let mut cloned = (*self).clone();
        cloned.input = children[0].clone();
        cloned.cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(cloned.output_schema.clone()),
            Partitioning::UnknownPartitioning(
                cloned.input.output_partitioning().partition_count().max(1),
            ),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(cloned))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let table_url =
            Url::parse(&self.table_url).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let output_schema = self.output_schema.clone();

        let state = ScanByDataFilesState::new(
            input_stream,
            context,
            table_url,
            Arc::clone(&output_schema),
            self.file_path_column.clone(),
        );

        let s = stream::try_unfold(state, |mut st| async move {
            loop {
                // Phase 1: Drain current scan stream.
                if let Some(scan) = &mut st.current_scan {
                    match scan.try_next().await? {
                        Some(batch) => return Ok(Some((batch, st))),
                        None => {
                            st.current_scan = None;
                            continue;
                        }
                    }
                }

                // Phase 2: If we have enough pending files (or input done), build a scan.
                if !st.pending_files.is_empty()
                    && (st.pending_files.len() >= SCAN_CHUNK_FILES || st.input_done)
                {
                    st.build_next_scan().await?;
                    continue;
                }

                // Phase 3: Pull more file metadata from upstream.
                match st.input.try_next().await? {
                    Some(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        let files = st.extract_file_info(&batch)?;
                        st.pending_files.extend(files);
                        continue;
                    }
                    None => {
                        st.input_done = true;
                        // Build final scan from remaining files.
                        if !st.pending_files.is_empty() {
                            st.build_next_scan().await?;
                            continue;
                        }
                        // No files at all: emit empty batch.
                        if !st.emitted_empty {
                            st.emitted_empty = true;
                            return Ok(Some((
                                RecordBatch::new_empty(st.output_schema.clone()),
                                st,
                            )));
                        }
                        return Ok(None);
                    }
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, s)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::Result;
    use datafusion::execution::context::TaskContext;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::physical_plan::{
        common, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
    };
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::operations::write::arrow_parquet::ArrowParquetWriter;
    use crate::physical_plan::manifest_scan_exec::manifest_scan_schema;

    const PATH_COL: &str = "__sail_file_path";

    /// A minimal plan that yields a single fixed batch; used as the metadata
    /// source for `IcebergScanByDataFilesExec` in tests.
    #[derive(Debug, Clone)]
    struct FixedBatchExec {
        schema: SchemaRef,
        batch: RecordBatch,
        cache: Arc<PlanProperties>,
    }

    impl FixedBatchExec {
        fn new(schema: SchemaRef, batch: RecordBatch) -> Self {
            let cache = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ));
            Self {
                schema,
                batch,
                cache,
            }
        }
    }

    impl DisplayAs for FixedBatchExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "FixedBatchExec")
                }
                DisplayFormatType::TreeRender => Ok(()),
            }
        }
    }

    impl ExecutionPlan for FixedBatchExec {
        fn name(&self) -> &'static str {
            "FixedBatchExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            Ok(Box::pin(MemoryStream::try_new(
                vec![self.batch.clone()],
                Arc::clone(&self.schema),
                None,
            )?))
        }
    }

    async fn write_parquet_file(path: &str, batch: &RecordBatch) -> u64 {
        let mut writer =
            ArrowParquetWriter::try_new(batch.schema().as_ref(), WriterProperties::default())
                .expect("create parquet writer");
        writer.write_batch(batch).await.expect("write batch");
        let (bytes, meta) = writer.close().await.expect("close writer");
        std::fs::write(path, bytes.to_vec()).expect("write file");
        meta.file_size
    }

    fn data_batch(rows: &[(i32, &str)]) -> RecordBatch {
        let ids = Int32Array::from(rows.iter().map(|(i, _)| *i).collect::<Vec<_>>());
        let values = StringArray::from(rows.iter().map(|(_, v)| Some(*v)).collect::<Vec<_>>());
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            vec![Arc::new(ids), Arc::new(values)],
        )
        .expect("data batch")
    }

    #[tokio::test]
    async fn file_path_column_is_materialized_per_file() -> Result<()> {
        let dir = std::env::temp_dir().join(format!(
            "sail-scan-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(dir.join("data")).expect("create temp dir");

        let file0 = format!("file://{}/data/file0.parquet", dir.display());
        let file1 = format!("file://{}/data/file1.parquet", dir.display());

        let size0 = write_parquet_file(
            dir.join("data/file0.parquet").to_str().unwrap(),
            &data_batch(&[(1, "a"), (2, "b")]),
        )
        .await;
        let size1 = write_parquet_file(
            dir.join("data/file1.parquet").to_str().unwrap(),
            &data_batch(&[(3, "c"), (4, "d")]),
        )
        .await;

        let meta = manifest_scan_schema();
        let meta_batch = RecordBatch::try_new(
            meta.clone(),
            vec![
                Arc::new(StringArray::from(vec![file0.clone(), file1.clone()])),
                Arc::new(StringArray::from(vec!["PARQUET", "PARQUET"])),
                Arc::new(UInt64Array::from(vec![2u64, 2u64])),
                Arc::new(UInt64Array::from(vec![size0, size1])),
                Arc::new(Int32Array::from(vec![0i32, 0i32])),
                Arc::new(StringArray::from(vec!["DATA", "DATA"])),
            ],
        )
        .expect("manifest metadata batch");

        let input: Arc<dyn ExecutionPlan> = Arc::new(FixedBatchExec::new(meta.clone(), meta_batch));

        let table_url = format!("file://{}/", dir.display());
        let data_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let scan = IcebergScanByDataFilesExec::new_with_file_path_column(
            input,
            table_url,
            data_schema,
            Some(PATH_COL.to_string()),
        );

        let ctx = datafusion::execution::context::SessionContext::new();
        let stream = scan.execute(0, ctx.task_ctx())?;
        let batches = common::collect(stream).await?;

        let mut paths: HashSet<String> = HashSet::new();
        for batch in &batches {
            let col = batch
                .column_by_name(PATH_COL)
                .expect("path column present")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("path column is utf8");
            for i in 0..col.len() {
                paths.insert(col.value(i).to_string());
            }
        }

        let expected: HashSet<String> = vec![file0, file1].into_iter().collect();
        assert_eq!(
            paths, expected,
            "each data file must yield its own __sail_file_path value"
        );

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}

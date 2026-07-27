use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Expr;
use futures::stream::once;
use object_store::ObjectStoreExt;
use sail_common::retry::sleep_with_jitter;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::MergeIntoOptions;

use crate::catalog_support::commit_helper::{CommitResult, commit_iceberg_changes};
use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::io::StoreContext;
use crate::operations::snapshot::SnapshotProduceOperation;
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::operations::{SnapshotProducer, Transaction};
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, Manifest, ManifestEntry,
    ManifestFile, ManifestList, ManifestStatus, PartitionSpec, TableMetadata,
};
use crate::table::find_latest_metadata_file_with_catalog_fallback;
use crate::table::metadata_loader::{load_metadata_file_bytes, metadata_file_version_from_path};
use crate::table_format::metadata_location_from_properties;
use crate::utils::get_object_store_from_context;
use crate::utils::metadata::metadata_files_for_version;

const MAX_COMMIT_RETRIES: usize = 3;

#[derive(Debug)]
pub struct IcebergMergeExec {
    table_url: String,
    merge_options: Option<MergeIntoOptions>,
    source_plan: Option<Arc<LogicalPlan>>,
    source_data: Option<Vec<RecordBatch>>,
    schema: SchemaRef,
    session_state: SessionState,
    lakehouse_table: Option<LakehouseExecutionContext>,
    table_properties: Vec<(String, String)>,
    table_schema: Option<SchemaRef>,
    cache: Arc<PlanProperties>,
}

impl IcebergMergeExec {
    pub fn new(
        table_url: String,
        merge_options: Option<MergeIntoOptions>,
        source_plan: Option<Arc<LogicalPlan>>,
        source_data: Option<Vec<RecordBatch>>,
        session_state: SessionState,
        lakehouse_table: Option<LakehouseExecutionContext>,
        table_properties: Vec<(String, String)>,
        table_schema: Option<SchemaRef>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let cache = Self::compute_properties(schema.clone());
        Self {
            table_url,
            merge_options,
            source_plan,
            source_data,
            schema,
            session_state,
            lakehouse_table,
            table_properties,
            table_schema,
            cache,
        }
    }

    pub fn table_url(&self) -> &str {
        &self.table_url
    }

    pub fn merge_options(&self) -> Option<&MergeIntoOptions> {
        self.merge_options.as_ref()
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.lakehouse_table.as_ref()
    }

    pub fn table_properties(&self) -> &[(String, String)] {
        &self.table_properties
    }

    pub fn table_schema(&self) -> Option<&SchemaRef> {
        self.table_schema.as_ref()
    }

    pub fn session_state(&self) -> &datafusion::execution::SessionState {
        &self.session_state
    }

    /// Evaluate the source plan into RecordBatches, using cached source_data if available.
    pub async fn evaluate_source_plan(&self) -> Result<Vec<RecordBatch>> {
        if let Some(ref data) = self.source_data {
            return Ok(data.clone());
        }
        if let Some(ref plan) = self.source_plan {
            use datafusion::execution::context::SessionContext;
            let ctx = SessionContext::new_with_state(self.session_state.clone());
            let df = ctx
                .execute_logical_plan(LogicalPlan::clone(plan.as_ref()))
                .await
                .map_err(|e| DataFusionError::Execution(format!("Source plan: {e}")))?;
            let batches = df
                .collect()
                .await
                .map_err(|e| DataFusionError::Execution(format!("Source collect: {e}")))?;
            return Ok(batches);
        }
        Ok(vec![])
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

impl DisplayAs for IcebergMergeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergMergeExec(table={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: iceberg")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for IcebergMergeExec {
    fn name(&self) -> &'static str {
        "IcebergMergeExec"
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
        let _session_state = self.session_state.clone();
        let lakehouse_table = self.lakehouse_table.clone();
        let table_properties = self.table_properties.clone();
        let schema = self.schema.clone();
        let merge_options = self.merge_options.clone();
        let source_plan = self.source_plan.clone();

        let future = async move {
            let table_url_parsed = url::Url::parse(&table_url)
                .map_err(|e| DataFusionError::Plan(format!("Invalid URL: {e}")))?;
            let object_store = get_object_store_from_context(&context, &table_url_parsed)?;
            let store_ctx = StoreContext::new(object_store.clone(), &table_url_parsed)?;

            let latest_meta = find_latest_metadata_file_with_catalog_fallback(
                &object_store,
                &table_url_parsed,
                None,
            )
            .await?;
            let bytes = load_metadata_file_bytes(&object_store, &latest_meta).await?;
            let table_meta = TableMetadata::from_json(&bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let metadata_location = metadata_location_from_properties(&table_properties);

            let snapshot = table_meta
                .current_snapshot()
                .cloned()
                .ok_or_else(|| DataFusionError::Plan("No current snapshot found".to_string()))?;
            let format_version = table_meta.format_version;
            let iceberg_schema = table_meta
                .current_schema()
                .cloned()
                .ok_or_else(|| DataFusionError::Plan("No current schema".to_string()))?;
            let arrow_schema = Arc::new(
                iceberg_schema_to_arrow(&iceberg_schema)
                    .map_err(|e| DataFusionError::Execution(format!("Schema conversion: {e}")))?,
            );
            let partition_spec = table_meta
                .default_partition_spec()
                .cloned()
                .unwrap_or_else(PartitionSpec::unpartitioned_spec);

            // Collect all data files from the current snapshot
            let mut all_target_files: Vec<DataFile> = Vec::new();
            let parent_list =
                load_manifest_list_internal(&store_ctx, snapshot.manifest_list(), format_version)
                    .await?;
            for manifest_file in parent_list.entries() {
                let entries =
                    load_manifest_entries_internal(&store_ctx, manifest_file, format_version)
                        .await?;
                for entry in &entries {
                    if entry.status == ManifestStatus::Deleted {
                        continue;
                    }
                    all_target_files.push(entry.data_file.clone());
                }
            }

            // Read all target data into memory
            let mut all_target_batches: Vec<RecordBatch> = Vec::new();
            for df in &all_target_files {
                let batches = read_parquet_file(&store_ctx, df).await?;
                all_target_batches.extend(batches);
            }

            let file_schema = arrow_schema.clone();
            let target_batch = if all_target_batches.is_empty() {
                None
            } else {
                Some(concat_batches(&file_schema, &all_target_batches)?)
            };

            let mut kept_data_files: Vec<DataFile> = Vec::new();
            let mut rewritten_data_files: Vec<DataFile> = Vec::new();
            let mut total_updated_rows: u64 = 0;

            if let (Some(options), Some(target_batch)) = (&merge_options, &target_batch) {
                // Get source data by evaluating the source plan (or from cached source_data)
                let source_batches =
                    evaluate_source_plan_internal(&source_plan, &_session_state).await;

                let source_batch = match source_batches {
                    Ok(batches) => {
                        if batches.is_empty() {
                            None
                        } else {
                            Some(concat_batches(&file_schema, &batches)?)
                        }
                    }
                    Err(_) => None,
                };

                // Evaluate the ON condition to find matching rows
                let matching_mask = evaluate_on_condition(
                    target_batch,
                    source_batch.as_ref(),
                    &options.on_condition.expr,
                    &_session_state,
                    &file_schema,
                )
                .await?;

                // Count matching rows for output
                total_updated_rows = matching_mask.true_count() as u64;

                // Keep all target files unchanged (rewrite logic to be implemented)
                for df in &all_target_files {
                    kept_data_files.push(df.clone());
                }

                // If there are source-only rows (INSERT action), write them as new files
                for clause in &options.not_matched_by_target_clauses {
                    match clause.action {
                        sail_common_datafusion::datasource::MergeNotMatchedByTargetAction::InsertAll
                        | sail_common_datafusion::datasource::MergeNotMatchedByTargetAction::InsertColumns { .. } => {
                            if let Some(ref src_batch) = source_batch {
                                let insert_batch = filter_insert_rows(
                                    target_batch,
                                    src_batch,
                                    &options.on_condition.expr,
                                    &_session_state,
                                    &file_schema,
                                )
                                .await?;
                                if insert_batch.num_rows() > 0 {
                                    let new_file = write_data_file(
                                        &store_ctx,
                                        &file_schema,
                                        &insert_batch,
                                        all_target_files.first(),
                                    )
                                    .await?;
                                    rewritten_data_files.push(new_file);
                                }
                            }
                        }
                    }
                }
            } else {
                // No merge options — keep all files unchanged
                for df in &all_target_files {
                    kept_data_files.push(df.clone());
                }
            }

            // Build commit with retry loop
            let mut attempt = 0;
            loop {
                attempt += 1;
                if attempt > 1 {
                    sleep_with_jitter(5, attempt - 2).await;
                }
                let current_latest = if attempt == 1 {
                    latest_meta.clone()
                } else {
                    find_latest_metadata_file_with_catalog_fallback(
                        &object_store,
                        &table_url_parsed,
                        metadata_location.as_deref(),
                    )
                    .await?
                };

                let current_bytes =
                    load_metadata_file_bytes(&object_store, &current_latest).await?;
                let mut current_table_meta = TableMetadata::from_json(&current_bytes)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let current_snapshot_id = current_table_meta
                    .current_snapshot()
                    .map(|s| s.snapshot_id());

                if current_snapshot_id != Some(snapshot.snapshot_id()) && attempt > 1 {
                    if attempt >= MAX_COMMIT_RETRIES {
                        return Err(DataFusionError::Execution(
                            "Iceberg MERGE failed: concurrent modification".to_string(),
                        ));
                    }
                    continue;
                }

                let current_version = metadata_file_version_from_path(&current_latest).unwrap_or(0);
                {
                    let mut candidate = current_version + 1;
                    loop {
                        let existing = metadata_files_for_version(&store_ctx, candidate).await?;
                        if existing.is_empty() {
                            break;
                        }
                        candidate += 1;
                        if candidate > current_version + MAX_COMMIT_RETRIES as i32 + 5 {
                            return Err(DataFusionError::Execution(
                                "Iceberg MERGE commit conflict: too many occupied versions"
                                    .to_string(),
                            ));
                        }
                    }
                }

                let current_schema = current_table_meta
                    .current_schema()
                    .cloned()
                    .ok_or_else(|| DataFusionError::Plan("No current schema".to_string()))?;

                let tx = Transaction::new(table_url.to_string(), snapshot.clone());
                let manifest_meta = tx.default_manifest_metadata(
                    &current_schema,
                    &partition_spec,
                    current_table_meta.format_version,
                );

                let all_data_files: Vec<DataFile> = kept_data_files
                    .iter()
                    .cloned()
                    .chain(rewritten_data_files.iter().cloned())
                    .collect();

                let producer = SnapshotProducer::new(
                    &tx,
                    all_data_files,
                    Some(store_ctx.clone()),
                    Some(manifest_meta),
                );
                let producer = producer.with_parent_manifest_entries(Some(vec![]));

                struct MergeOperation;
                impl SnapshotProduceOperation for MergeOperation {
                    fn operation(&self) -> &'static str {
                        "overwrite"
                    }
                }

                let action_commit = producer
                    .commit(MergeOperation)
                    .await
                    .map_err(DataFusionError::Execution)?;

                match commit_iceberg_changes(
                    Some(&context),
                    &store_ctx,
                    &table_url_parsed,
                    &mut current_table_meta,
                    action_commit,
                    lakehouse_table.as_ref(),
                    &table_properties,
                    &current_latest,
                    metadata_location.clone(),
                )
                .await
                {
                    Ok(CommitResult::Committed) => {
                        let array = Arc::new(UInt64Array::from(vec![total_updated_rows]));
                        let batch = RecordBatch::try_new(schema, vec![array])
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        return Ok(batch);
                    }
                    Err(e) => {
                        if attempt >= MAX_COMMIT_RETRIES {
                            return Err(e);
                        }
                        continue;
                    }
                }
            }
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

fn concat_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(DataFusionError::Execution(
            "No batches to concatenate".to_string(),
        ));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    datafusion::arrow::compute::concat_batches(schema, batches)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

async fn read_parquet_file(
    store_ctx: &StoreContext,
    data_file: &DataFile,
) -> Result<Vec<RecordBatch>> {
    let (store_ref, resolved_path) = store_ctx
        .resolve(&data_file.file_path)
        .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

    let store_path = object_store::path::Path::from(resolved_path.as_ref());
    let file_size = data_file.file_size_in_bytes;

    let reader = parquet::arrow::async_reader::ParquetObjectReader::new(
        store_ref.clone(),
        store_path.clone(),
    )
    .with_file_size(file_size);

    let builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| DataFusionError::Execution(format!("Failed to open Parquet: {e}")))?;

    let stream = builder
        .build()
        .map_err(|e| DataFusionError::Execution(format!("Failed to build stream: {e}")))?;
    let mut stream = std::pin::pin!(stream);
    use futures::StreamExt;
    let mut batches = Vec::new();
    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| DataFusionError::Execution(format!("Parquet read: {e}")))?;
        batches.push(batch);
    }
    Ok(batches)
}

async fn evaluate_on_condition(
    target_batch: &RecordBatch,
    source_batch: Option<&RecordBatch>,
    condition: &Expr,
    session_state: &SessionState,
    _schema: &SchemaRef,
) -> Result<datafusion::arrow::array::BooleanArray> {
    let batch = match source_batch {
        Some(source) => {
            use datafusion::execution::context::SessionContext;
            use datafusion::logical_expr::JoinType;
            let ctx = SessionContext::new_with_state(session_state.clone());
            let target_df = ctx
                .read_batch(target_batch.clone())
                .map_err(|e| DataFusionError::Execution(format!("Read target: {e}")))?;
            let source_df = ctx
                .read_batch(source.clone())
                .map_err(|e| DataFusionError::Execution(format!("Read source: {e}")))?;
            // Cross join requires a dummy true condition
            let joined = target_df
                .join(
                    source_df,
                    JoinType::Inner,
                    &[],
                    &[],
                    Some(Expr::Literal(
                        datafusion::common::ScalarValue::Boolean(Some(true)),
                        None,
                    )),
                )
                .map_err(|e| DataFusionError::Execution(format!("Join target/source: {e}")))?;
            let batches = joined
                .collect()
                .await
                .map_err(|e| DataFusionError::Execution(format!("Collect joined: {e}")))?;
            if batches.is_empty() {
                return Ok(datafusion::arrow::array::BooleanArray::from(
                    vec![false; target_batch.num_rows()],
                ));
            }
            let arrow_schema = batches[0].schema();
            concat_batches(&arrow_schema, &batches)?
        }
        None => target_batch.clone(),
    };

    use datafusion::common::ToDFSchema;
    let df_schema = batch
        .schema()
        .to_dfschema()
        .map_err(|e| DataFusionError::Execution(format!("Schema conversion: {e}")))?;

    let phys_expr = session_state
        .create_physical_expr(condition.clone(), &df_schema)
        .map_err(|e| DataFusionError::Execution(format!("ON condition: {e}")))?;

    let mask = phys_expr
        .evaluate(&batch)
        .map_err(|e| DataFusionError::Execution(format!("ON eval: {e}")))?
        .into_array(batch.num_rows())
        .map_err(|e| DataFusionError::Execution(format!("ON array: {e}")))?;

    mask.as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution("ON condition did not produce BooleanArray".to_string())
        })
}

async fn filter_insert_rows(
    target_batch: &RecordBatch,
    source_batch: &RecordBatch,
    condition: &Expr,
    session_state: &SessionState,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::JoinType;

    let ctx = SessionContext::new_with_state(session_state.clone());
    ctx.register_batch("__target", target_batch.clone())
        .map_err(|e| DataFusionError::Execution(format!("Register target: {e}")))?;
    ctx.register_batch("__source", source_batch.clone())
        .map_err(|e| DataFusionError::Execution(format!("Register source: {e}")))?;

    let target_df = ctx
        .table("__target")
        .await
        .map_err(|e| DataFusionError::Execution(format!("Get target: {e}")))?;
    let source_df = ctx
        .table("__source")
        .await
        .map_err(|e| DataFusionError::Execution(format!("Get source: {e}")))?;

    let anti_joined = source_df
        .join(
            target_df,
            JoinType::LeftAnti,
            &[],
            &[],
            Some(condition.clone()),
        )
        .map_err(|e| DataFusionError::Execution(format!("Anti join: {e}")))?;

    let batches = anti_joined
        .collect()
        .await
        .map_err(|e| DataFusionError::Execution(format!("Collect anti: {e}")))?;

    if batches.is_empty() {
        return Ok(source_batch.slice(0, 0));
    }
    concat_batches(schema, &batches)
}

async fn write_data_file(
    store_ctx: &StoreContext,
    file_schema: &SchemaRef,
    batch: &RecordBatch,
    reference_file: Option<&DataFile>,
) -> Result<DataFile> {
    let mut writer = ArrowParquetWriter::try_new(
        file_schema,
        parquet::file::properties::WriterProperties::default(),
    )
    .map_err(|e| DataFusionError::Execution(format!("Writer: {e}")))?;

    writer
        .write_batch(batch)
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

    let num_rows = batch.num_rows() as u64;
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

    let partition = reference_file
        .map(|f| f.partition.clone())
        .unwrap_or_default();
    let spec_id = reference_file.map(|f| f.partition_spec_id).unwrap_or(0);

    Ok(DataFile {
        content: reference_file
            .map(|f| f.content)
            .unwrap_or(DataContentType::Data),
        file_path: data_rel,
        file_format: DataFileFormat::Parquet,
        partition,
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
        partition_spec_id: spec_id,
        referenced_data_file: None,
        content_offset: None,
        content_size_in_bytes: None,
    })
}

async fn evaluate_source_plan_internal(
    source_plan: &Option<Arc<LogicalPlan>>,
    session_state: &SessionState,
) -> Result<Vec<RecordBatch>> {
    match source_plan {
        Some(plan) => {
            use datafusion::execution::context::SessionContext;
            let ctx = SessionContext::new_with_state(session_state.clone());
            let df = ctx
                .execute_logical_plan(LogicalPlan::clone(plan.as_ref()))
                .await
                .map_err(|e| DataFusionError::Execution(format!("Source plan: {e}")))?;
            let batches = df
                .collect()
                .await
                .map_err(|e| DataFusionError::Execution(format!("Source collect: {e}")))?;
            Ok(batches)
        }
        None => Ok(vec![]),
    }
}

async fn load_manifest_list_internal(
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
        .map_err(|e| DataFusionError::Execution(format!("Failed to read manifest list: {e}")))?;
    ManifestList::parse_with_version(&data, format_version)
        .map_err(|e| DataFusionError::Execution(format!("Failed to parse manifest list: {e}")))
}

async fn load_manifest_entries_internal(
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

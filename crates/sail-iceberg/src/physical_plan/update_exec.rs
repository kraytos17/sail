use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ToDFSchema;
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
use sail_common_datafusion::logical_expr::ExprWithSource;

use crate::catalog_support::commit_helper::{commit_iceberg_changes, CommitResult};
use crate::datasource::pruning::data_file_might_match;
use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::io::StoreContext;
use crate::operations::snapshot::SnapshotProduceOperation;
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::operations::{SnapshotProducer, Transaction};
use crate::spec::manifest_list::ManifestFile;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, Manifest, ManifestEntry, ManifestList, ManifestStatus,
    PartitionSpec, TableMetadata,
};
use crate::table::find_latest_metadata_file_with_catalog_fallback;
use crate::table::metadata_loader::{load_metadata_file_bytes, metadata_file_version_from_path};
use crate::table_format::metadata_location_from_properties;
use crate::utils::get_object_store_from_context;
use crate::utils::metadata::{
    get_metadata_file_timestamp, is_stale_metadata_file, metadata_files_for_version,
};

const MAX_COMMIT_RETRIES: usize = 5;

#[derive(Debug)]
pub struct IcebergUpdateExec {
    table_url: String,
    assignments: Vec<(String, ExprWithSource)>,
    condition: Option<ExprWithSource>,
    schema: SchemaRef,
    session_state: SessionState,
    lakehouse_table: Option<LakehouseExecutionContext>,
    table_properties: Vec<(String, String)>,
    table_schema: Option<SchemaRef>,
    cache: Arc<PlanProperties>,
}

impl IcebergUpdateExec {
    pub fn new(
        table_url: String,
        assignments: Vec<(String, ExprWithSource)>,
        condition: Option<ExprWithSource>,
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
            assignments,
            condition,
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

    pub fn condition(&self) -> &Option<ExprWithSource> {
        &self.condition
    }

    pub fn assignments(&self) -> &[(String, ExprWithSource)] {
        &self.assignments
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

    fn compute_properties(schema: SchemaRef) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl DisplayAs for IcebergUpdateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergUpdateExec(table={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: iceberg")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for IcebergUpdateExec {
    fn name(&self) -> &'static str {
        "IcebergUpdateExec"
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
        let condition = self.condition.clone();
        let assignments = self.assignments.clone();
        let schema = self.schema();
        let session_state = self.session_state.clone();
        let lakehouse_table = self.lakehouse_table.clone();
        let table_properties = self.table_properties.clone();
        let catalog_metadata_location = metadata_location_from_properties(&table_properties);
        let future = async move {
            let table_url_parsed = url::Url::parse(&table_url)
                .map_err(|e| DataFusionError::Plan(format!("Invalid URL: {e}")))?;
            let object_store = get_object_store_from_context(&context, &table_url_parsed)?;
            let store_ctx = StoreContext::new(object_store.clone(), &table_url_parsed)?;

            let latest_meta = find_latest_metadata_file_with_catalog_fallback(
                &object_store,
                &table_url_parsed,
                catalog_metadata_location.as_deref(),
            )
            .await?;
            let bytes = load_metadata_file_bytes(&object_store, &latest_meta).await?;
            let table_meta = TableMetadata::from_json(&bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let snapshot = table_meta
                .current_snapshot()
                .cloned()
                .ok_or_else(|| DataFusionError::Plan("No current snapshot found".to_string()))?;
            let format_version = table_meta.format_version;
            let iceberg_schema = table_meta
                .current_schema()
                .cloned()
                .ok_or_else(|| DataFusionError::Plan("No current schema".to_string()))?;
            let arrow_schema = Arc::new(iceberg_schema_to_arrow(&iceberg_schema)?);
            let partition_spec = table_meta
                .default_partition_spec()
                .cloned()
                .unwrap_or_else(PartitionSpec::unpartitioned_spec);

            let tx = Transaction::new(table_url.to_string(), snapshot.clone());

            let mut kept_data_files: Vec<DataFile> = Vec::new();
            let mut files_to_rewrite: Vec<DataFile> = Vec::new();

            if let Some(ref cond) = condition {
                let parent_list =
                    Self::load_manifest_list(&store_ctx, snapshot.manifest_list(), format_version)
                        .await?;

                for manifest_file in parent_list.entries() {
                    let entries =
                        Self::load_manifest_entries(&store_ctx, manifest_file, format_version)
                            .await?;
                    for entry in &entries {
                        if entry.status == ManifestStatus::Deleted {
                            continue;
                        }
                        let df = &entry.data_file;
                        let might_match = data_file_might_match(
                            &session_state,
                            df,
                            &cond.expr,
                            arrow_schema.clone(),
                            &iceberg_schema,
                        )?;
                        if might_match {
                            files_to_rewrite.push(df.clone());
                        } else {
                            kept_data_files.push(df.clone());
                        }
                    }
                }
            } else {
                // UPDATE without WHERE: all rows match, rewrite all files
                let parent_list =
                    Self::load_manifest_list(&store_ctx, snapshot.manifest_list(), format_version)
                        .await?;
                for manifest_file in parent_list.entries() {
                    let entries =
                        Self::load_manifest_entries(&store_ctx, manifest_file, format_version)
                            .await?;
                    for entry in &entries {
                        if entry.status == ManifestStatus::Deleted {
                            continue;
                        }
                        files_to_rewrite.push(entry.data_file.clone());
                    }
                }
            }

            // Rewrite files applying SET expressions
            let mut rewritten_data_files: Vec<DataFile> = Vec::new();
            let mut total_updated_rows: u64 = 0;
            let true_expr = datafusion_expr::Expr::Literal(
                datafusion_common::ScalarValue::Boolean(Some(true)),
                None,
            );
            let update_condition = condition.as_ref().map(|c| &c.expr).unwrap_or(&true_expr);
            for df in &files_to_rewrite {
                let (rewritten, updated) = Self::rewrite_and_update_file(
                    &store_ctx,
                    df,
                    update_condition,
                    &assignments,
                    &arrow_schema,
                    &session_state,
                )
                .await?;
                total_updated_rows += updated;
                rewritten_data_files.push(rewritten);
            }

            // Build commit with retry loop
            let mut attempt = 0;
            loop {
                attempt += 1;
                let current_latest = if attempt == 1 {
                    latest_meta.clone()
                } else {
                    find_latest_metadata_file_with_catalog_fallback(
                        &object_store,
                        &table_url_parsed,
                        catalog_metadata_location.as_deref(),
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
                            "Iceberg UPDATE failed: concurrent modification".to_string(),
                        ));
                    }
                    continue;
                }

                let current_version = metadata_file_version_from_path(&current_latest).unwrap_or(0);
                let next_version = current_version + 1;

                let existing_for_next =
                    metadata_files_for_version(&store_ctx, next_version).await?;
                if !existing_for_next.is_empty() {
                    let current_ts =
                        get_metadata_file_timestamp(&store_ctx, &current_latest).await?;
                    let has_real_conflict = existing_for_next
                        .iter()
                        .any(|(_, ts)| !is_stale_metadata_file(*ts, current_ts));
                    if has_real_conflict {
                        if attempt >= MAX_COMMIT_RETRIES {
                            return Err(DataFusionError::Execution(
                                "Iceberg UPDATE commit conflict".to_string(),
                            ));
                        }
                        continue;
                    }
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
                // Discard all parent manifest entries — the single new manifest
                // (kept + rewritten files) replaces every old file.
                let producer = producer.with_parent_manifest_entries(Some(vec![]));

                struct UpdateOperation;
                impl SnapshotProduceOperation for UpdateOperation {
                    fn operation(&self) -> &'static str {
                        "overwrite"
                    }
                }

                let action_commit = producer
                    .commit(UpdateOperation)
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
                    None,
                )
                .await
                {
                    Ok(CommitResult::Committed { .. }) => {
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

impl IcebergUpdateExec {
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

    async fn rewrite_and_update_file(
        store_ctx: &StoreContext,
        data_file: &DataFile,
        condition: &datafusion_expr::Expr,
        assignments: &[(String, ExprWithSource)],
        _arrow_schema: &SchemaRef,
        session_state: &SessionState,
    ) -> Result<(DataFile, u64)> {
        let (store_ref, resolved_path) = store_ctx
            .resolve(&data_file.file_path)
            .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

        use datafusion::arrow::array::BooleanArray;
        use datafusion::arrow::compute::kernels::zip::zip;

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

        let file_schema = builder.schema().clone();
        let stream = builder.build().map_err(|e| {
            DataFusionError::Execution(format!("Failed to build Parquet stream: {e}"))
        })?;

        // Build condition physical expression
        let df_schema = file_schema
            .clone()
            .to_dfschema()
            .map_err(|e| DataFusionError::Execution(format!("Schema conversion: {e}")))?;
        let condition_expr = session_state
            .create_physical_expr(condition.clone(), &df_schema)
            .map_err(|e| DataFusionError::Execution(format!("Condition creation: {e}")))?;

        // Build assignment physical expressions
        let mut assignment_exprs: Vec<(String, Arc<dyn datafusion::physical_expr::PhysicalExpr>)> =
            Vec::new();
        for (col_name, expr_with_source) in assignments {
            let phys_expr = session_state
                .create_physical_expr(expr_with_source.expr.clone(), &df_schema)
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Assignment expr creation for '{}': {e}",
                        col_name
                    ))
                })?;
            assignment_exprs.push((col_name.clone(), phys_expr));
        }

        // Read all batches and apply SET expressions
        let mut updated_batches: Vec<RecordBatch> = Vec::new();
        let mut total_matching_rows: u64 = 0;
        let mut stream = std::pin::pin!(stream);
        use futures::StreamExt;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| DataFusionError::Execution(format!("Parquet read: {e}")))?;

            // Evaluate WHERE condition → boolean mask
            let mask = condition_expr
                .evaluate(&batch)
                .map_err(|e| DataFusionError::Execution(format!("Condition eval: {e}")))?
                .into_array(batch.num_rows())
                .map_err(|e| DataFusionError::Execution(format!("Condition array: {e}")))?;

            let decision_mask = mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Condition did not produce a BooleanArray".to_string(),
                    )
                })?;

            // If no rows match the condition, keep batch unchanged
            let matching = decision_mask.true_count() as u64;
            if matching == 0 {
                updated_batches.push(batch);
                continue;
            }
            total_matching_rows += matching;

            // Apply each assignment: replace column values for matching rows
            let mut columns: Vec<datafusion::arrow::array::ArrayRef> = batch.columns().to_vec();
            let schema = batch.schema();

            for (col_name, assignment_expr) in &assignment_exprs {
                // Find the column index
                let col_idx = schema.index_of(col_name).map_err(|_| {
                    DataFusionError::Execution(format!("Column '{}' not found in schema", col_name))
                })?;

                // Evaluate the SET expression on the entire batch
                let new_values = assignment_expr
                    .evaluate(&batch)
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Assignment eval for '{}': {e}",
                            col_name
                        ))
                    })?
                    .into_array(batch.num_rows())
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Assignment array for '{}': {e}",
                            col_name
                        ))
                    })?;

                // Combine: for matching rows use new_values, for others use original
                let combined = zip(decision_mask, &new_values, &columns[col_idx]).map_err(|e| {
                    DataFusionError::Execution(format!("Zip failed for '{}': {e}", col_name))
                })?;

                columns[col_idx] = combined;
            }

            let updated_batch = RecordBatch::try_new(schema.clone(), columns)
                .map_err(|e| DataFusionError::Execution(format!("Rebuild batch: {e}")))?;
            updated_batches.push(updated_batch);
        }

        if updated_batches.is_empty() {
            return Ok((data_file.clone(), total_matching_rows));
        }

        // Write updated data as new Parquet file
        let mut writer = ArrowParquetWriter::try_new(
            &file_schema,
            parquet::file::properties::WriterProperties::default(),
        )
        .map_err(|e| DataFusionError::Execution(format!("Writer: {e}")))?;

        for batch in &updated_batches {
            writer
                .write_batch(batch)
                .await
                .map_err(|e| DataFusionError::Execution(format!("Write: {e}")))?;
        }

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

        let mut column_sizes = std::collections::HashMap::new();
        let mut value_counts = std::collections::HashMap::new();
        let mut null_value_counts = std::collections::HashMap::new();

        let num_rows = parquet_meta.num_rows;
        for (col_idx, _) in file_schema.fields().iter().enumerate() {
            let field_id = (col_idx + 1) as i32;
            column_sizes.insert(
                field_id,
                parquet_meta.file_size / (file_schema.fields().len() as u64).max(1),
            );
            value_counts.insert(field_id, num_rows);
            null_value_counts.insert(field_id, 0);
        }

        let rewritten_file = DataFile {
            content: data_file.content,
            file_path: data_rel,
            file_format: DataFileFormat::Parquet,
            partition: data_file.partition.clone(),
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
            partition_spec_id: data_file.partition_spec_id,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };

        Ok((rewritten_file, total_matching_rows))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{BooleanArray, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ToDFSchema;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::logical_expr::{col, lit, Expr};

    fn count_matching_rows(batch: &RecordBatch, condition: Expr) -> usize {
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let df_schema = batch.schema().to_dfschema().unwrap();
        let physical_expr = session_state
            .create_physical_expr(condition, &df_schema)
            .unwrap();
        let mask = physical_expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let bool_mask = mask.as_any().downcast_ref::<BooleanArray>().unwrap();
        bool_mask.true_count()
    }

    #[test]
    fn test_update_count_matching_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let matching = count_matching_rows(&batch, col("id").gt(lit(3i32)));
        assert_eq!(matching, 2, "id=4,5 should match id>3");
    }

    #[test]
    fn test_update_count_all_rows_match() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let matching = count_matching_rows(&batch, col("id").gt(lit(0i32)));
        assert_eq!(matching, 3);
    }

    #[test]
    fn test_update_count_no_rows_match() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let matching = count_matching_rows(&batch, col("id").gt(lit(100i32)));
        assert_eq!(matching, 0);
    }

    #[test]
    fn test_update_count_date_condition() {
        use datafusion::arrow::array::Date32Array;
        use datafusion::arrow::datatypes::DataType as ArrowDataType;
        use datafusion::common::ScalarValue;

        let schema = Arc::new(Schema::new(vec![
            Field::new("event_date", ArrowDataType::Date32, true),
            Field::new("name", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Date32Array::from(vec![
                    Some(19747),
                    Some(19747),
                    Some(19748),
                    Some(19749),
                ])),
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie", "dave"])),
            ],
        )
        .unwrap();

        let condition = col("event_date").eq(Expr::Literal(ScalarValue::Date32(Some(19747)), None));
        let matching = count_matching_rows(&batch, condition);
        assert_eq!(
            matching, 2,
            "alice and bob should match event_date = 2024-01-15"
        );
    }
}

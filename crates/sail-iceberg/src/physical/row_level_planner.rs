use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result, not_impl_err, plan_err};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SessionState;
use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion::physical_expr::expressions::{Column, IsNotNullExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_planner::PhysicalPlanner;
use log::debug;
use sail_common_datafusion::datasource::{MERGE_FILE_COLUMN, PhysicalSinkMode, RowLevelCommand};
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::merge::RowLevelWriteNode;

use crate::operations::SnapshotUpdateKind;
use crate::options::r#gen::IcebergWriteOptions;
use crate::physical_plan::equality_delete_writer_exec::validate_equality_delete_schema;
use crate::physical_plan::merge_row_projection::IcebergMergeRowProjection;
use crate::physical_plan::{
    IcebergCommitExec, IcebergEqualityDeleteWriterExec, IcebergWriterExec,
    IcebergWriterExecOptions, prepare_iceberg_write_context,
};
use crate::table::Table;
use crate::table_format::{
    IcebergTableFormat, catalog_managed_iceberg_from_options, metadata_location_from_options,
    split_iceberg_write_options_and_table_properties,
};

pub(crate) async fn plan_iceberg_row_level_write(
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    match node.command() {
        RowLevelCommand::Delete => plan_iceberg_delete(session_state, planner, node).await,
        RowLevelCommand::Update => {
            plan_iceberg_update(session_state, planner, node, physical_inputs).await
        }
        RowLevelCommand::Merge => plan_iceberg_merge(session_state, node, physical_inputs).await,
    }
}

async fn plan_iceberg_merge(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let write_plan = physical_inputs.first().cloned().ok_or_else(|| {
        DataFusionError::Internal("Iceberg MERGE missing write plan input".to_string())
    })?;
    if node.touched_files_plan().is_some() && physical_inputs.len() < 2 {
        return plan_err!("Iceberg MERGE missing touched-file plan input");
    }
    let table_url =
        IcebergTableFormat::parse_table_url(vec![node.target_location().to_string()]).await?;
    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let metadata_location_for_load = catalog_managed_table
        .then_some(metadata_location.clone())
        .flatten();
    let table = Table::load_with_metadata_location(
        session_state,
        table_url.clone(),
        metadata_location_for_load,
    )
    .await?;
    ensure_current_row_level_mode(&table, RowLevelCommand::Merge)?;
    let partition_columns = IcebergTableFormat::partition_columns_from_metadata(&table)?;
    let writer_options = resolve_row_level_writer_options(session_state, node)?;

    let merge_projection = IcebergMergeRowProjection::try_new(write_plan.schema())?;
    let data_rows_schema = merge_projection.data_schema();
    let write_context = prepare_iceberg_write_context(
        &table_url,
        Some(table.metadata()),
        &writer_options,
        &partition_columns,
        &PhysicalSinkMode::Append,
        data_rows_schema.as_ref(),
    )?;
    let writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergWriterExec::new_merge(
        write_plan,
        table_url.clone(),
        partition_columns,
        PhysicalSinkMode::Append,
        true,
        writer_options.clone(),
        write_context,
    )?);

    Ok(Arc::new(
        IcebergCommitExec::new(
            writer,
            table_url,
            writer_options.lakehouse_table.clone(),
            SnapshotUpdateKind::RowDelta,
        )
        .with_expected_snapshot_id(node.expected_snapshot_id()),
    ))
}

async fn plan_iceberg_delete(
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    // TRUNCATE TABLE (and conditionless DELETE) removes every row.
    if node.condition().is_none() {
        return plan_iceberg_truncate(session_state, node).await;
    }
    let condition = node.condition().ok_or_else(|| {
        DataFusionError::Plan(
            "Iceberg equality-delete MOR DELETE requires a WHERE condition".to_string(),
        )
    })?;

    let table_url =
        IcebergTableFormat::parse_table_url(vec![node.target_location().to_string()]).await?;
    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let metadata_location_for_load = catalog_managed_table.then_some(metadata_location).flatten();
    let table = Table::load_with_metadata_location(
        session_state,
        table_url.clone(),
        metadata_location_for_load,
    )
    .await?;
    ensure_current_row_level_mode(&table, RowLevelCommand::Delete)?;
    let current_schema = table.metadata().current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
    })?;
    validate_equality_delete_schema(current_schema)?;

    let delete_plan = LogicalPlanBuilder::from(node.raw_target().as_ref().clone())
        .filter(condition.expr.clone())?
        .build()?;
    let physical_delete = planner
        .create_physical_plan(&delete_plan, session_state)
        .await?;

    let writer_options = resolve_row_level_writer_options(session_state, node)?;
    let partition_columns = IcebergTableFormat::partition_columns_from_metadata(&table)?;
    let current_arrow_schema =
        crate::datasource::type_converter::iceberg_schema_to_arrow(current_schema)?;
    let write_context = prepare_iceberg_write_context(
        &table_url,
        Some(table.metadata()),
        &writer_options,
        &partition_columns,
        &PhysicalSinkMode::Append,
        &current_arrow_schema,
    )?;

    let delete_input: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(physical_delete));
    let delete_writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergEqualityDeleteWriterExec::new(
        delete_input,
        table_url.clone(),
        writer_options.table_properties.clone(),
        writer_options.write_data_path.clone(),
        writer_options.write_folder_storage_path.clone(),
        write_context,
        writer_options.lakehouse_table.clone(),
    )?);

    Ok(Arc::new(
        IcebergCommitExec::new(
            Arc::new(CoalescePartitionsExec::new(delete_writer)),
            table_url,
            writer_options.lakehouse_table.clone(),
            SnapshotUpdateKind::RowDelta,
        )
        .with_expected_snapshot_id(node.expected_snapshot_id()),
    ))
}

/// Plans `TRUNCATE TABLE` (and conditionless `DELETE`) for Iceberg.
///
/// - A table that was created but never written (no current snapshot) has nothing to
///   delete: an empty input commits nothing and the commit exec reports `count = 0`.
/// - Otherwise all rows are dropped by committing an **empty full overwrite**: the writer
///   emits a `commit_meta` with no data files and `FullOverwrite` drops every parent
///   manifest, leaving an empty snapshot. Affected rows are reported as `0`, matching
///   `TRUNCATE TABLE` semantics.
async fn plan_iceberg_truncate(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table_url =
        IcebergTableFormat::parse_table_url(vec![node.target_location().to_string()]).await?;
    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let metadata_location_for_load = catalog_managed_table.then_some(metadata_location).flatten();
    let table = Table::load_with_metadata_location(
        session_state,
        table_url.clone(),
        metadata_location_for_load,
    )
    .await?;
    let current_schema = table.metadata().current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
    })?;
    let current_arrow_schema =
        crate::datasource::type_converter::iceberg_schema_to_arrow(current_schema)?;

    if table.metadata().current_snapshot().is_none() {
        let empty: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::new(current_arrow_schema)));
        return Ok(Arc::new(IcebergCommitExec::new(
            empty,
            table_url,
            node.target_lakehouse_table().cloned(),
            SnapshotUpdateKind::FastAppend,
        )));
    }

    let writer_options = resolve_row_level_writer_options(session_state, node)?;
    let partition_columns = IcebergTableFormat::partition_columns_from_metadata(&table)?;
    let write_context = prepare_iceberg_write_context(
        &table_url,
        Some(table.metadata()),
        &writer_options,
        &partition_columns,
        &PhysicalSinkMode::Append,
        &current_arrow_schema,
    )?;
    let empty_input: Arc<dyn ExecutionPlan> =
        Arc::new(EmptyExec::new(Arc::new(current_arrow_schema)));
    let writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergWriterExec::new(
        empty_input,
        table_url.clone(),
        partition_columns,
        PhysicalSinkMode::Append,
        true,
        writer_options.clone(),
        write_context,
    )?);

    Ok(Arc::new(
        IcebergCommitExec::new(
            writer,
            table_url,
            writer_options.lakehouse_table.clone(),
            SnapshotUpdateKind::FullOverwrite,
        )
        .with_expected_snapshot_id(node.expected_snapshot_id()),
    ))
}

fn ensure_current_row_level_mode(table: &Table, command: RowLevelCommand) -> Result<()> {
    let (operation, property) = match command {
        RowLevelCommand::Delete => ("DELETE", "write.delete.mode"),
        RowLevelCommand::Merge => ("MERGE", "write.merge.mode"),
        RowLevelCommand::Update => ("UPDATE", "write.update.mode"),
    };
    let mode = table
        .metadata()
        .properties
        .get(property)
        .map_or("copy-on-write", String::as_str);
    if mode.eq_ignore_ascii_case("merge-on-read") {
        return Ok(());
    }
    if mode.eq_ignore_ascii_case("copy-on-write") {
        return not_impl_err!(
            "Iceberg {operation} with `{property}=copy-on-write` is not supported yet; set `{property}=merge-on-read`"
        );
    }
    plan_err!(
        "Unknown Iceberg row-level operation mode for `{property}`: {mode}; expected `copy-on-write` or `merge-on-read`"
    )
}

fn resolve_row_level_writer_options(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
) -> Result<IcebergWriterExecOptions> {
    let (clean_options, table_properties) =
        split_iceberg_write_options_and_table_properties(node.target_options().to_vec())?;
    let variant_presence =
        IcebergWriterExecOptions::variant_shredding_option_presence(&clean_options);
    let iceberg_options = IcebergWriteOptions::resolve(session_state, clean_options)?;
    let mut writer_options = IcebergWriterExecOptions::from(iceberg_options);
    writer_options.apply_variant_shredding_option_presence(variant_presence);
    writer_options.table_properties = table_properties;
    writer_options.lakehouse_table = node.target_lakehouse_table().cloned();
    Ok(writer_options)
}

async fn plan_iceberg_update(
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let write_plan = physical_inputs.first().cloned().ok_or_else(|| {
        DataFusionError::Internal("Iceberg UPDATE missing write plan input".to_string())
    })?;
    if node.touched_files_plan().is_some() && physical_inputs.len() < 2 {
        return plan_err!("Iceberg UPDATE missing touched-file plan input");
    }

    let table_url =
        IcebergTableFormat::parse_table_url(vec![node.target_location().to_string()]).await?;
    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let metadata_location_for_load = catalog_managed_table
        .then_some(metadata_location.clone())
        .flatten();
    let table = Table::load_with_metadata_location(
        session_state,
        table_url.clone(),
        metadata_location_for_load,
    )
    .await?;
    ensure_current_row_level_mode(&table, RowLevelCommand::Update)?;

    let current_schema = table.metadata().current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
    })?;
    let arrow_schema = Arc::new(crate::datasource::type_converter::iceberg_schema_to_arrow(
        current_schema,
    )?);

    let writer_options = resolve_row_level_writer_options(session_state, node)?;
    let partition_columns = IcebergTableFormat::partition_columns_from_metadata(&table)?;
    let write_context = prepare_iceberg_write_context(
        &table_url,
        Some(table.metadata()),
        &writer_options,
        &partition_columns,
        &PhysicalSinkMode::Append,
        &arrow_schema,
    )?;

    // Physicalize the touched_files_plan if present.
    let touched_files_physical = if let Some(touched_plan) = node.touched_files_plan() {
        let touched_logical = LogicalPlanBuilder::from((*touched_plan).clone()).build()?;
        Some(
            planner
                .create_physical_plan(&touched_logical, session_state)
                .await?,
        )
    } else {
        None
    };

    let (touched_file_paths, matched_row_count) =
        collect_touched_file_paths(session_state, &touched_files_physical).await?;

    debug!("UPDATE touched file paths: {:?}", touched_file_paths);

    let writer_input: Arc<dyn ExecutionPlan> = if touched_file_paths.is_empty() {
        strip_internal_columns(write_plan, &arrow_schema)?
    } else {
        let (untouched_rows, touched_rows) =
            build_targeted_writer_input(write_plan, &touched_file_paths)?;
        let unioned: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![untouched_rows, touched_rows])?;
        strip_internal_columns(unioned, &arrow_schema)?
    };

    debug!(
        "UPDATE touched {} files, matched {} rows: {:?}",
        touched_file_paths.len(),
        matched_row_count,
        touched_file_paths
    );

    let writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergWriterExec::new(
        writer_input,
        table_url.clone(),
        partition_columns,
        PhysicalSinkMode::Append,
        true,
        writer_options.clone(),
        write_context,
    )?);

    Ok(Arc::new(
        IcebergCommitExec::new(
            writer,
            table_url,
            writer_options.lakehouse_table.clone(),
            SnapshotUpdateKind::RowDelta,
        )
        .with_expected_snapshot_id(node.expected_snapshot_id()),
    ))
}

async fn collect_touched_file_paths(
    session: &dyn datafusion::catalog::Session,
    touched_files_plan: &Option<Arc<dyn ExecutionPlan>>,
) -> Result<(Vec<String>, u64)> {
    let Some(plan) = touched_files_plan else {
        return Ok((vec![], 0));
    };
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::common;

    let task_ctx = Arc::new(TaskContext::new(
        None,
        "sail".to_string(),
        session.config().clone(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        session.runtime_env().clone(),
    ));
    let stream = plan.execute(0, task_ctx)?;

    let batches = common::collect(stream)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut matched_row_count = 0u64;
    let mut paths: HashSet<String> = HashSet::new();
    for batch in &batches {
        matched_row_count += u64::try_from(batch.num_rows())
            .map_err(|e| DataFusionError::Execution(format!("Row count overflow: {}", e)))?;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("touched_files_plan output must be a Utf8 column".into())
            })?;
        for i in 0..batch.num_rows() {
            let val = col.value(i);
            if !val.is_empty() {
                paths.insert(val.to_string());
            }
        }
    }

    let mut result: Vec<String> = paths.into_iter().collect();
    result.sort();
    Ok((result, matched_row_count))
}

fn touched_paths_source(paths: &[String]) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        MERGE_FILE_COLUMN,
        DataType::Utf8,
        false,
    )]));
    let array = StringArray::from(paths.iter().map(|s| s.as_str()).collect::<Vec<&str>>());
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])?;
    let source = MemorySourceConfig::try_new(&[vec![batch]], schema, None)?;
    Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
}

fn build_targeted_writer_input(
    write_plan: Arc<dyn ExecutionPlan>,
    touched_file_paths: &[String],
) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
    let file_path_idx = write_plan
        .schema()
        .index_of(MERGE_FILE_COLUMN)
        .map_err(|_| DataFusionError::Plan("write_plan missing __sail_file_path column".into()))?;

    let is_not_null = Arc::new(IsNotNullExpr::new(Arc::new(Column::new(
        MERGE_FILE_COLUMN,
        file_path_idx,
    ))));
    let non_insert: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(is_not_null, Arc::clone(&write_plan))?);

    let touched_source: Arc<dyn ExecutionPlan> = touched_paths_source(touched_file_paths)?;
    let touch_idx = touched_source.schema().index_of(MERGE_FILE_COLUMN)?;
    let left_width = touched_source.schema().fields().len();

    let untouched_join = Arc::new(HashJoinExec::try_new(
        Arc::clone(&touched_source),
        Arc::clone(&non_insert),
        vec![(
            Arc::new(Column::new(MERGE_FILE_COLUMN, touch_idx)),
            Arc::new(Column::new(MERGE_FILE_COLUMN, file_path_idx)),
        )],
        None,
        &JoinType::RightAnti,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let untouched_rows = Arc::new(ProjectionExec::try_new(
        (0..untouched_join.schema().fields().len())
            .map(|i| {
                (
                    Arc::new(Column::new(untouched_join.schema().field(i).name(), i))
                        as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                    untouched_join.schema().field(i).name().clone(),
                )
            })
            .collect::<Vec<_>>(),
        untouched_join,
    )?);

    let touched_join = Arc::new(HashJoinExec::try_new(
        touched_source,
        non_insert,
        vec![(
            Arc::new(Column::new(MERGE_FILE_COLUMN, touch_idx)),
            Arc::new(Column::new(MERGE_FILE_COLUMN, file_path_idx)),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let touched_rows = Arc::new(ProjectionExec::try_new(
        (left_width..touched_join.schema().fields().len())
            .map(|i| {
                (
                    Arc::new(Column::new(touched_join.schema().field(i).name(), i))
                        as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                    touched_join.schema().field(i).name().clone(),
                )
            })
            .collect::<Vec<_>>(),
        touched_join,
    )?);

    Ok((untouched_rows, touched_rows))
}

fn strip_internal_columns(
    input: Arc<dyn ExecutionPlan>,
    table_schema: &Schema,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    let projections: Vec<_> = table_schema
        .fields()
        .iter()
        .filter_map(|field| {
            input_schema.index_of(field.name()).ok().map(|idx| {
                (
                    Arc::new(Column::new(field.name(), idx))
                        as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                    field.name().clone(),
                )
            })
        })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(projections, input)?))
}

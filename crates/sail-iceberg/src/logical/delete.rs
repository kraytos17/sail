use std::sync::Arc;

use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::{MERGE_FILE_COLUMN, OptionLayer};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_logical_plan::merge::{RowLevelWriteNode, expand_delete};

use crate::logical::update::ensure_update_metadata_columns;

/// Expand a DELETE into a `RowLevelWriteNode` with the file-path column enabled
/// on the Iceberg table scan.
///
/// Conditional DELETE carries rewrite plans for copy-on-write execution;
/// conditionless DELETE carries no plans and is planned as truncate downstream.
pub fn expand_delete_node(
    target_scan: LogicalPlan,
    condition: Option<ExprWithSource>,
    format: String,
    location: String,
    table_name: Vec<String>,
    options: Vec<OptionLayer>,
    lakehouse_table: Option<LakehouseExecutionContext>,
    expected_snapshot_id: Option<Option<i64>>,
) -> Result<LogicalPlan> {
    let target_with_file_column = ensure_update_metadata_columns(target_scan)?;
    let raw_input_schema = target_with_file_column.schema().clone();
    let expansion = expand_delete(
        target_with_file_column.clone(),
        condition.clone(),
        MERGE_FILE_COLUMN,
    )?;

    let node = RowLevelWriteNode::new_delete(
        Arc::new(target_with_file_column),
        raw_input_schema,
        condition,
        expansion.write_plan.map(Arc::new),
        expansion.touched_files_plan.map(Arc::new),
        format,
        location,
        table_name,
        options,
        lakehouse_table,
    )
    .with_expected_snapshot_id(expected_snapshot_id);

    Ok(LogicalPlan::Extension(
        datafusion::logical_expr::Extension {
            node: Arc::new(node),
        },
    ))
}

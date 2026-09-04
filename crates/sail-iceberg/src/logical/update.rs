use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::LogicalPlan;
use sail_common_datafusion::datasource::{MERGE_FILE_COLUMN, MergeCapableSource, UpdateInfo};
use sail_logical_plan::merge::{RowLevelWriteNode, expand_update};

use crate::logical::IcebergTableSource;

/// Expand an UPDATE into a `RowLevelWriteNode` with the file-path column enabled
/// on the Iceberg table scan.
pub fn expand_update_node(info: UpdateInfo) -> Result<LogicalPlan> {
    let target_with_file_column = ensure_update_metadata_columns((*info.target).clone())?;

    let target_with_file_column = if !target_with_file_column
        .schema()
        .has_column_with_unqualified_name(MERGE_FILE_COLUMN)
    {
        let mut proj_exprs: Vec<datafusion_expr::Expr> = target_with_file_column
            .schema()
            .fields()
            .iter()
            .map(|f| {
                datafusion_expr::Expr::Column(datafusion_common::Column::new_unqualified(f.name()))
            })
            .collect();
        proj_exprs.push(datafusion_expr::Expr::Column(
            datafusion_common::Column::new_unqualified(MERGE_FILE_COLUMN),
        ));
        datafusion::logical_expr::LogicalPlanBuilder::from(target_with_file_column)
            .project(proj_exprs)?
            .build()?
    } else {
        target_with_file_column
    };

    let raw_input_schema = target_with_file_column.schema().clone();
    let path = info.path.clone();
    let table_name = info.table_name.clone();
    let options = info.options.clone();
    let lakehouse_table = info.lakehouse_table.clone();
    let condition = info.condition.clone();

    let expansion = expand_update(
        UpdateInfo {
            target: Arc::new(target_with_file_column.clone()),
            ..info
        },
        MERGE_FILE_COLUMN,
    )?;

    let node = RowLevelWriteNode::new_update(
        Arc::new(target_with_file_column),
        raw_input_schema,
        Arc::new(expansion.write_plan),
        Arc::new(expansion.touched_files_plan),
        condition,
        "iceberg".to_string(),
        path,
        table_name,
        options,
        lakehouse_table,
    );

    Ok(LogicalPlan::Extension(
        datafusion::logical_expr::Extension {
            node: Arc::new(node),
        },
    ))
}

/// Walk the logical plan tree and enable the file-path metadata column on any
/// `IcebergTableSource` found in `TableScan` nodes.
fn ensure_update_metadata_columns(plan: LogicalPlan) -> Result<LogicalPlan> {
    use datafusion::common::tree_node::TreeNode;

    let result = plan.transform_up(|node| match node {
        LogicalPlan::TableScan(scan) => {
            if let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() {
                if source.file_column_name().is_some() {
                    return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
                }
                let new_source = source.with_file_column(MERGE_FILE_COLUMN)?;
                let new_schema = new_source.schema();
                let mut new_scan = scan.clone();
                new_scan.source = new_source;
                // Add the file column to the projection if present.
                if let Some(proj) = &new_scan.projection {
                    let mut new_proj = proj.clone();
                    let file_col_idx = new_schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == MERGE_FILE_COLUMN);
                    if let Some(idx) = file_col_idx {
                        if !new_proj.contains(&idx) {
                            new_proj.push(idx);
                        }
                    }
                    new_scan.projection = Some(new_proj);
                }
                new_scan.projected_schema = Arc::new(DFSchema::try_from_qualified_schema(
                    new_scan.table_name.clone(),
                    &new_schema,
                )?);
                Ok(Transformed::no(LogicalPlan::TableScan(new_scan)))
            } else {
                Ok(Transformed::no(LogicalPlan::TableScan(scan)))
            }
        }
        _ => Ok(Transformed::no(node)),
    })?;
    Ok(result.data)
}

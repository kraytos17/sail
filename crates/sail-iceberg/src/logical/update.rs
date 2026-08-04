use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result};
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::{Expr, LogicalPlan, TableScan};
use log::trace;
use sail_common_datafusion::datasource::{MergeCapableSource, UpdateInfo, MERGE_FILE_COLUMN};
use sail_logical_plan::merge::{expand_update, RowLevelWriteNode};

use crate::logical::IcebergTableSource;

/// Expand UPDATE information into a unified row-level write node for Iceberg.
pub fn expand_update_node(info: UpdateInfo) -> Result<LogicalPlan> {
    let mut target_plan = ensure_update_metadata_columns(info.target.as_ref().clone())?;
    let target_fields: Vec<String> = target_plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    trace!(
        "rewrite target_plan schema after ensure_update_metadata_columns: {:?}",
        &target_fields
    );

    if !target_fields.iter().any(|n| n == MERGE_FILE_COLUMN) {
        let exprs: Vec<Expr> = target_fields
            .iter()
            .map(|name| Expr::Column(Column::from_name(name.clone())))
            .chain(std::iter::once(
                Expr::Column(Column::from_name(MERGE_FILE_COLUMN)).alias(MERGE_FILE_COLUMN),
            ))
            .collect();
        target_plan = LogicalPlanBuilder::from(target_plan)
            .project(exprs)?
            .build()?;
    }

    let raw_target = Arc::clone(&info.target);
    let raw_input_schema = info.target.schema().clone();
    let location = info.path.clone();
    let table_name = info.table_name.clone();
    let options = info.options.clone();
    let lakehouse_table = info.lakehouse_table.clone();
    let condition = info.condition.clone();
    let assignments = info.assignments.clone();

    let info = UpdateInfo {
        target: Arc::new(target_plan),
        ..info
    };
    let expansion = expand_update(info, MERGE_FILE_COLUMN)?;

    let write_node = RowLevelWriteNode::new_update(
        raw_target,
        raw_input_schema,
        Arc::new(expansion.write_plan),
        Arc::new(expansion.touched_files_plan),
        condition,
        assignments,
        "iceberg".to_string(),
        location,
        table_name,
        options,
        lakehouse_table,
    );

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(write_node),
    }))
}

/// Reconfigure the Iceberg table scan in a logical plan to expose the per-row
/// file path column (via `MergeCapableSource::with_file_column`), and keep the
/// column through parent projections.
fn ensure_update_metadata_columns(plan: LogicalPlan) -> Result<LogicalPlan> {
    let transformed = plan
        .transform_up(|plan| {
            if let LogicalPlan::TableScan(scan) = &plan {
                if let Some((new_source, schema)) = try_enable_metadata_column(&scan.source)? {
                    trace!(
                        "ensure_update_metadata_columns (scan) before - table_name: {:?}, projection: {:?}",
                        &scan.table_name,
                        &scan.projection
                    );

                    let mut projection: Option<Vec<usize>> = scan.projection.clone();
                    if projection.is_none() {
                        projection = Some((0..schema.fields().len()).collect::<Vec<usize>>());
                    }
                    if let Some(proj) = projection.as_mut() {
                        if let Some(idx) = schema.column_with_name(MERGE_FILE_COLUMN).map(|(idx, _)| idx)
                        {
                            if !proj.contains(&idx) {
                                proj.push(idx);
                            }
                        }
                    }

                    let new_scan = LogicalPlan::TableScan(TableScan::try_new(
                        scan.table_name.clone(),
                        new_source,
                        projection,
                        scan.filters.clone(),
                        scan.fetch,
                    )?);
                    trace!(
                        "ensure_update_metadata_columns (scan) after - schema_fields: {:?}",
                        new_scan
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect::<Vec<_>>(),
                    );

                    return Ok(Transformed::yes(new_scan));
                }
            }

            if let LogicalPlan::Projection(proj) = &plan {
                let input_schema = proj.input.schema();
                let has_in_input = input_schema
                    .fields()
                    .iter()
                    .any(|f| f.name() == MERGE_FILE_COLUMN);
                let has_in_projection = proj.expr.iter().any(|e| match e {
                    Expr::Column(c) => c.name == MERGE_FILE_COLUMN,
                    Expr::Alias(a) => a.name == MERGE_FILE_COLUMN,
                    _ => false,
                });
                if has_in_input && !has_in_projection {
                    let mut new_exprs = proj.expr.clone();
                    new_exprs.push(
                        Expr::Column(Column::from_name(MERGE_FILE_COLUMN))
                            .alias(MERGE_FILE_COLUMN),
                    );
                    let new_proj = LogicalPlanBuilder::from(proj.input.as_ref().clone())
                        .project(new_exprs)?
                        .build()?;
                    return Ok(Transformed::yes(new_proj));
                }
            }

            Ok(Transformed::no(plan))
        })
        .map(|t| t.data)?;

    Ok(transformed)
}

/// Enable the file path column on an Iceberg table scan source.
fn try_enable_metadata_column(
    source: &Arc<dyn datafusion::logical_expr::TableSource>,
) -> Result<
    Option<(
        Arc<dyn datafusion::logical_expr::TableSource>,
        datafusion::arrow::datatypes::SchemaRef,
    )>,
> {
    let Some(iceberg_source) = source.downcast_ref::<IcebergTableSource>() else {
        return Ok(None);
    };
    if iceberg_source.file_column_name().is_some() {
        return Ok(None);
    }
    let new_source = iceberg_source.with_file_column(MERGE_FILE_COLUMN)?;
    let schema = new_source.schema();
    Ok(Some((new_source, schema)))
}

use std::sync::Arc;

use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, Result};
use log::debug;

use super::commit::assemble_iceberg_commit_plan;
use super::context::PlannerContext;
use super::helpers::{
    build_targeted_writer_input, collect_touched_file_paths, strip_internal_columns,
};
use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::spec::Operation;

pub async fn plan_merge(
    ctx: &PlannerContext<'_>,
    write_plan: Arc<dyn ExecutionPlan>,
    touched_files_plan: Arc<dyn ExecutionPlan>,
    is_insert_only: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.table();
    let arrow_schema = Arc::new(iceberg_schema_to_arrow(
        table
            .metadata()
            .current_schema()
            .ok_or_else(|| DataFusionError::Plan("Table has no current schema".to_string()))?,
    )?);

    let (touched_file_paths, _matched_row_count) =
        collect_touched_file_paths(ctx.session(), &touched_files_plan).await?;

    if is_insert_only || touched_file_paths.is_empty() {
        // Full write: all rows are new (insert path), no file rewrite needed.
        let writer_input = strip_internal_columns(write_plan, &arrow_schema)?;

        return assemble_iceberg_commit_plan(
            ctx,
            writer_input,
            None,
            arrow_schema,
            Operation::Append,
            vec![],
            None,
        )
        .await;
    }

    let (untouched_rows, touched_rows) =
        build_targeted_writer_input(write_plan, &touched_file_paths)?;

    let writer_input: Arc<dyn ExecutionPlan> =
        UnionExec::try_new(vec![untouched_rows, touched_rows])?;
    let writer_input = strip_internal_columns(writer_input, &arrow_schema)?;

    debug!(
        "MERGE touched {} files: {:?}",
        touched_file_paths.len(),
        touched_file_paths
    );

    assemble_iceberg_commit_plan(
        ctx,
        writer_input,
        None,
        arrow_schema,
        Operation::Overwrite,
        touched_file_paths,
        None,
    )
    .await
}

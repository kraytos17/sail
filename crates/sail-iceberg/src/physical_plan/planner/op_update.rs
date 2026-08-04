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

/// Plan an UPDATE from its expanded logical plans using targeted rewrite:
/// - The logical write plan already projects `CASE WHEN condition THEN assignment
///   ELSE current END` for each assigned column and carries `__sail_file_path`.
/// - Collect the touched files (files with rows matching the condition).
/// - Rewrite only touched files via `build_targeted_writer_input`; untouched files
///   stay in the parent manifests (via touched_file_paths at commit).
pub async fn plan_update(
    ctx: &PlannerContext<'_>,
    write_plan: Arc<dyn ExecutionPlan>,
    touched_files_plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.table();
    let arrow_schema = Arc::new(iceberg_schema_to_arrow(
        table
            .metadata()
            .current_schema()
            .ok_or_else(|| DataFusionError::Plan("Table has no current schema".to_string()))?,
    )?);

    let (touched_file_paths, matched_row_count) =
        collect_touched_file_paths(ctx.session(), &touched_files_plan).await?;

    debug!("UPDATE touched file paths: {:?}", touched_file_paths);

    if touched_file_paths.is_empty() {
        // No rows matched: nothing changes. Rewrite all rows unchanged (full replacement).
        let writer_input = strip_internal_columns(write_plan, &arrow_schema)?;
        return assemble_iceberg_commit_plan(
            ctx,
            writer_input,
            None,
            arrow_schema,
            Operation::Overwrite,
            vec![],
            Some(0),
        )
        .await;
    }

    let (untouched_rows, touched_rows) =
        build_targeted_writer_input(write_plan, &touched_file_paths)?;

    let writer_input: Arc<dyn ExecutionPlan> =
        UnionExec::try_new(vec![untouched_rows, touched_rows])?;
    let writer_input = strip_internal_columns(writer_input, &arrow_schema)?;

    debug!(
        "UPDATE touched {} files, matched {} rows: {:?}",
        touched_file_paths.len(),
        matched_row_count,
        touched_file_paths
    );

    assemble_iceberg_commit_plan(
        ctx,
        writer_input,
        None,
        arrow_schema,
        Operation::Overwrite,
        touched_file_paths,
        Some(matched_row_count),
    )
    .await
}

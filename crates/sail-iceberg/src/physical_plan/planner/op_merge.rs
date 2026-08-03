// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::physical_expr::expressions::{Column, IsNotNullExpr, IsNullExpr};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, JoinType, NullEquality, Result};
use log::debug;
use sail_common_datafusion::datasource::MERGE_FILE_COLUMN;

use super::commit::assemble_iceberg_commit_plan;
use super::context::PlannerContext;
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

    // Execute touched_files_plan to collect all touched file paths.
    // These are the DISTINCT file paths of files that had at least one matched row.
    // At commit time, parent manifests containing these files will be replaced.
    let touched_file_paths = collect_touched_file_paths(&touched_files_plan)?;

    // If no files were actually touched (matched clauses exist but no rows matched,
    // or insert-only), use Append instead of Overwrite to avoid dropping existing data.
    if is_insert_only || touched_file_paths.is_empty() {
        // Only write new source rows, append to existing table.
        let file_path_idx = write_plan
            .schema()
            .index_of(MERGE_FILE_COLUMN)
            .map_err(|_| {
                DataFusionError::Plan("merge write_plan missing __sail_file_path column".into())
            })?;
        let is_null = Arc::new(IsNullExpr::new(Arc::new(Column::new(
            MERGE_FILE_COLUMN,
            file_path_idx,
        ))));
        let insert_only = Arc::new(FilterExec::try_new(is_null, write_plan)?);
        let writer_input = strip_merge_internal_columns(insert_only, &arrow_schema)?;

        return assemble_iceberg_commit_plan(
            ctx,
            writer_input,
            None,
            arrow_schema,
            Operation::Append,
            vec![], // no files replaced for insert-only / no-matched-rows
        )
        .await;
    }

    // Matched-clause MERGE: targeted rewrite.
    // Only rewrite files that were touched; untouched files stay in parent manifests.
    let (insert_rows, touched_rows) = build_targeted_writer_input(write_plan, touched_files_plan)?;

    let writer_input: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![insert_rows, touched_rows])?;
    let writer_input = strip_merge_internal_columns(writer_input, &arrow_schema)?;

    debug!(
        "MERGE touched {} files: {:?}",
        touched_file_paths.len(),
        touched_file_paths
    );

    // Pass touched file paths to the commit assembly.
    // The commit exec uses these to compute which parent manifests to keep vs replace.
    assemble_iceberg_commit_plan(
        ctx,
        writer_input,
        None,
        arrow_schema,
        Operation::Overwrite,
        touched_file_paths,
    )
    .await
}

/// Collect all distinct file paths from the touched_files_plan.
/// This executes the plan synchronously at plan time — the set is typically small
/// (one path per file that had matched rows).
fn collect_touched_file_paths(touched_files_plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<String>> {
    use datafusion::arrow::array::StringArray;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::common;

    let task_ctx = Arc::new(TaskContext::default());
    let stream = touched_files_plan.execute(0, task_ctx)?;

    // Collect all batches synchronously at plan time.
    let batches = futures::executor::block_on(common::collect(stream))
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut paths: HashSet<String> = HashSet::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("touched_files_plan output must be a Utf8 column".into())
            })?;
        for i in 0..batch.num_rows() {
            let val = col.value(i);
            if val.is_empty() {
                continue;
            }
            paths.insert(val.to_string());
        }
    }

    let mut result: Vec<String> = paths.into_iter().collect();
    result.sort();
    Ok(result)
}

fn build_targeted_writer_input(
    write_plan: Arc<dyn ExecutionPlan>,
    touched_files_plan: Arc<dyn ExecutionPlan>,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
    let file_path_idx = write_plan
        .schema()
        .index_of(MERGE_FILE_COLUMN)
        .map_err(|_| {
            DataFusionError::Plan("merge write_plan missing __sail_file_path column".into())
        })?;

    let is_null = Arc::new(IsNullExpr::new(Arc::new(Column::new(
        MERGE_FILE_COLUMN,
        file_path_idx,
    ))));
    let insert_rows = Arc::new(FilterExec::try_new(is_null, Arc::clone(&write_plan))?);

    let is_not_null = Arc::new(IsNotNullExpr::new(Arc::new(Column::new(
        MERGE_FILE_COLUMN,
        file_path_idx,
    ))));
    let non_insert = Arc::new(FilterExec::try_new(is_not_null, write_plan)?);

    let touch_idx = touched_files_plan.schema().index_of(MERGE_FILE_COLUMN)?;
    let left_width = touched_files_plan.schema().fields().len();
    let join = Arc::new(HashJoinExec::try_new(
        touched_files_plan,
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

    // Project only right-side columns (merged row data).
    let projections: Vec<_> = join
        .schema()
        .fields()
        .iter()
        .enumerate()
        .skip(left_width)
        .map(|(i, f)| {
            (
                Arc::new(Column::new(f.name(), i))
                    as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                f.name().clone(),
            )
        })
        .collect();
    let touched_rows = Arc::new(ProjectionExec::try_new(projections, join)?);

    Ok((insert_rows, touched_rows))
}

fn strip_merge_internal_columns(
    input: Arc<dyn ExecutionPlan>,
    table_schema: &arrow_schema::Schema,
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

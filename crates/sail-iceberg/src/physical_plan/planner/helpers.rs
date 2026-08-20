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

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::expressions::{Column, IsNotNullExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use sail_common_datafusion::datasource::MERGE_FILE_COLUMN;

/// Collect all distinct file paths from a plan that produces a single-string Utf8 column.
/// Executes the plan asynchronously at plan time, using a TaskContext built from the
/// session's runtime environment so that object stores resolve correctly.
///
/// Must be awaited from inside the caller's tokio runtime (physical planning runs on
/// the driver's tokio runtime): the touched plan contains `RepartitionExec` and parquet
/// `DataSourceExec`, which spawn tokio tasks internally. Driving them with
/// `futures::executor::block_on` would park a runtime worker and starve those tasks,
/// deadlocking the driver.
///
/// Returns `(touched_file_paths, matched_row_count)`: the distinct file paths, plus the
/// number of rows produced by `touched_files_plan` before path dedup. The touched plan is
/// built as `scan -> filter(condition) -> project(file_path)` (one row per matching row),
/// so `matched_row_count` is exactly the number of rows affected by the operation.
pub async fn collect_touched_file_paths(
    session: &dyn datafusion::catalog::Session,
    touched_files_plan: &Arc<dyn ExecutionPlan>,
) -> Result<(Vec<String>, u64)> {
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
    let stream = touched_files_plan.execute(0, task_ctx)?;

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
            if val.is_empty() {
                continue;
            }
            paths.insert(val.to_string());
        }
    }

    let mut result: Vec<String> = paths.into_iter().collect();
    result.sort();
    Ok((result, matched_row_count))
}

/// Build a one-column in-memory DataSourceExec from a list of file path strings.
fn touched_paths_source(paths: &[String]) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
        MERGE_FILE_COLUMN,
        DataType::Utf8,
        false,
    )]));
    let array = StringArray::from(paths.iter().map(|s| s.as_str()).collect::<Vec<&str>>());
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])?;
    let source = MemorySourceConfig::try_new(&[vec![batch]], schema, None)?;
    Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
}

/// Collect the touched-file rows and the untouched (carry-through) rows from the write plan.
///
/// Builds an in-memory source from the already-collected (deduped) touched paths. The
/// in-memory source is used as the build side for both joins, avoiding a second data scan.
///
/// Returns `(untouched_rows, touched_rows)`:
/// - `untouched_rows`: Anti join — rows whose file path is NOT in the touched set.
/// - `touched_rows`: Inner join — rows whose file path IS in the touched set,
///   with only the right-side (write plan) columns projected.
pub fn build_targeted_writer_input(
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

    // Untouched rows: Anti join — right-side rows NOT matching any touched path.
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

    // Touched rows: Inner join — right-side rows matching a touched path.
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

/// Strip internal MERGE columns (file path, row ID, etc.) that are not part of the
/// table schema. Keeps only columns whose name matches a field in `table_schema`.
pub fn strip_internal_columns(
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

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

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result as DFResult;
use sail_common_datafusion::catalog::CatalogPartitionField;
use sail_common_datafusion::datasource::PhysicalSinkMode;

use super::context::PlannerContext;
use crate::physical_plan::{IcebergCommitExec, IcebergWriterExec, IcebergWriterExecOptions};
use crate::spec::Operation;
use crate::utils::partition_transform::catalog_partition_field_from_iceberg;

pub async fn assemble_iceberg_commit_plan(
    ctx: &PlannerContext<'_>,
    writer_input: Arc<dyn ExecutionPlan>,
    remove_source: Option<Arc<dyn ExecutionPlan>>,
    output_schema: SchemaRef,
    operation: Operation,
    touched_file_paths: Vec<String>,
    reported_row_count: Option<u64>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let table = ctx.table();
    let table_url = ctx.table_url().clone();

    let partition_columns: Vec<CatalogPartitionField> = {
        let metadata = table.metadata();
        match metadata.default_partition_spec() {
            Some(spec) => spec
                .fields()
                .iter()
                .map(|f| catalog_partition_field_from_iceberg(f.name.clone(), f.transform))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| datafusion_common::DataFusionError::Plan(e))?,
            None => Vec::new(),
        }
    };

    let mut options = IcebergWriterExecOptions::from(ctx.options().clone());
    options.commit_operation = Some(operation);
    options.lakehouse_table = ctx.lakehouse_table().cloned();
    options.touched_file_paths = touched_file_paths;

    let writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergWriterExec::new(
        writer_input,
        table_url.clone(),
        partition_columns,
        PhysicalSinkMode::Append,
        true,
        options,
        Some(output_schema.clone()),
    ));

    let commit_input: Arc<dyn ExecutionPlan> = if let Some(remove_src) = remove_source {
        UnionExec::try_new(vec![writer, remove_src])?
    } else {
        writer
    };

    Ok(Arc::new(IcebergCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(commit_input)),
        table_url,
        ctx.lakehouse_table().cloned(),
        reported_row_count,
    )))
}

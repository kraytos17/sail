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
use datafusion::common::Result;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::catalog::CatalogPartitionField;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use crate::physical_plan::writer_exec::IcebergWriterExec;
use crate::physical_plan::writer_options::IcebergWriterExecOptions;
use crate::utils::partition_transform::format_partition_expr;

pub struct IcebergTableConfig {
    pub table_url: Url,
    pub partition_columns: Vec<CatalogPartitionField>,
    pub table_exists: bool,
    pub options: IcebergWriterExecOptions,
}

pub struct IcebergPlanBuilder {
    input: Arc<dyn ExecutionPlan>,
    table_config: IcebergTableConfig,
    sink_mode: PhysicalSinkMode,
    sort_order: Option<Vec<PhysicalSortExpr>>,
    logical_input_schema: Option<SchemaRef>,
}

impl IcebergPlanBuilder {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_config: IcebergTableConfig,
        sink_mode: PhysicalSinkMode,
        sort_order: Option<Vec<PhysicalSortExpr>>,
        logical_input_schema: Option<SchemaRef>,
    ) -> Self {
        Self {
            input,
            table_config,
            sink_mode,
            sort_order,
            logical_input_schema,
        }
    }

    pub async fn build(self) -> Result<Arc<dyn ExecutionPlan>> {
        self.add_projection_node(self.input.clone())
            .and_then(|plan| self.add_sort_node(plan))
            .and_then(|plan| self.add_writer_node(plan))
            .and_then(|plan| self.add_commit_node(plan))
    }

    fn add_projection_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        // Validate that partition transform expressions refer to real source columns.
        // Do not reorder columns here: BDD "query result ordered" checks expect the original
        // table column order from `SELECT *`.
        let schema = input.schema();
        for field in &self.table_config.partition_columns {
            if schema.index_of(&field.column).is_err() {
                return Err(datafusion::common::DataFusionError::Plan(format!(
                    "Partition column '{}' not found in schema",
                    format_partition_expr(field)
                )));
            }
        }
        Ok(input)
    }

    fn add_sort_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(sort_exprs) = self.sort_order.clone() {
            let lex = LexOrdering::new(sort_exprs).ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Invalid sort order".to_string())
            })?;
            Ok(Arc::new(SortExec::new(lex, input)))
        } else {
            Ok(input)
        }
    }

    fn add_writer_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IcebergWriterExec::new(
            input,
            self.table_config.table_url.clone(),
            self.table_config.partition_columns.clone(),
            self.sink_mode.clone(),
            self.table_config.table_exists,
            self.table_config.options.clone(),
            self.logical_input_schema.clone(),
        )))
    }

    fn add_commit_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        // IcebergCommitExec is single-partition; gather the writer's partitions first so
        // every writer task's action batch reaches the commit.
        let coalesced = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input),
        );
        Ok(Arc::new(
            crate::physical_plan::commit::commit_exec::IcebergCommitExec::new(
                coalesced,
                self.table_config.table_url.clone(),
                self.table_config.options.lakehouse_table.clone(),
                None,
            ),
        ))
    }
}

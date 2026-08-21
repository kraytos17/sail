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

use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use sail_common_datafusion::datasource::RowLevelCommand;
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::merge::RowLevelWriteNode;

use crate::options::r#gen::IcebergWriteOptions;
use crate::physical_plan::planner::{self, PlannerContext};
use crate::table_format::{
    IcebergTableFormat, catalog_managed_iceberg_from_options, metadata_location_from_options,
};

pub async fn plan_iceberg_row_level_write(
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let options = IcebergWriteOptions::resolve(session_state, node.target_options().to_vec())?;

    let table_url =
        IcebergTableFormat::parse_table_url(vec![node.target_location().to_string()]).await?;

    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());

    let ctx = PlannerContext::new(
        session_state,
        options,
        table_url,
        node.target_lakehouse_table().cloned(),
        metadata_location,
        catalog_managed_table,
    )
    .await?;

    match node.command() {
        RowLevelCommand::Delete => {
            let condition = node.condition().cloned();
            planner::plan_delete(&ctx, condition).await
        }
        RowLevelCommand::Merge => {
            let write_plan = node.write_plan().ok_or_else(|| {
                DataFusionError::Internal("MERGE node must have a write_plan".into())
            })?;
            let physical_write = planner
                .create_physical_plan(write_plan, session_state)
                .await?;

            let physical_touched = if let Some(plan) = node.touched_files_plan() {
                planner.create_physical_plan(plan, session_state).await?
            } else {
                return datafusion_common::internal_err!(
                    "MERGE node must have a touched_files_plan"
                );
            };

            let is_insert_only = node
                .merge_options()
                .map(|opts| {
                    opts.matched_clauses.is_empty() && opts.not_matched_by_source_clauses.is_empty()
                })
                .unwrap_or(false);

            planner::plan_merge(&ctx, physical_write, physical_touched, is_insert_only).await
        }
        RowLevelCommand::Update => {
            let write_plan = node.write_plan().ok_or_else(|| {
                DataFusionError::Internal("UPDATE node must have a write_plan".into())
            })?;
            let physical_write = planner
                .create_physical_plan(write_plan, session_state)
                .await?;

            let physical_touched = if let Some(plan) = node.touched_files_plan() {
                planner.create_physical_plan(plan, session_state).await?
            } else {
                return datafusion_common::internal_err!(
                    "UPDATE node must have a touched_files_plan"
                );
            };

            planner::plan_update(&ctx, physical_write, physical_touched).await
        }
    }
}

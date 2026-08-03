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

use datafusion::physical_expr::expressions::NotExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_common::{DataFusionError, Result, ToDFSchema};
use sail_common_datafusion::logical_expr::ExprWithSource;

use super::commit::assemble_iceberg_commit_plan;
use super::context::PlannerContext;
use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::physical_plan::{
    IcebergDiscoveryExec, IcebergManifestScanExec, IcebergScanByDataFilesExec,
};
use crate::spec::Operation;

pub async fn plan_delete(
    ctx: &PlannerContext<'_>,
    condition: Option<ExprWithSource>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.table();
    let table_url = ctx.table_url().clone();

    let snapshot = table
        .metadata()
        .current_snapshot()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Plan("Cannot delete from empty Iceberg table".to_string())
        })?;

    let iceberg_schema = table
        .metadata()
        .current_schema()
        .ok_or_else(|| DataFusionError::Plan("Table has no current schema".to_string()))?;
    let arrow_schema = Arc::new(iceberg_schema_to_arrow(iceberg_schema)?);

    // TRUNCATE: no WHERE clause → empty snapshot, no data files written
    if condition.is_none() {
        let empty_scan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(arrow_schema.clone()));
        return assemble_iceberg_commit_plan(
            ctx,
            empty_scan,
            None,
            arrow_schema,
            Operation::Delete,
            vec![],
        )
        .await;
    }
    let condition = condition.unwrap();

    let df_schema = arrow_schema.clone().to_dfschema()?;
    let physical_condition = ctx
        .session()
        .create_physical_expr(condition.expr.clone(), &df_schema)?;

    // Writer branch: scan → keep survivors
    let writer_scan = Arc::new(IcebergManifestScanExec::new(
        table_url.to_string(),
        snapshot.clone(),
    ));
    let writer_discovery = Arc::new(IcebergDiscoveryExec::new(
        writer_scan,
        table_url.to_string(),
        snapshot.snapshot_id(),
        false,
    )?);

    let target_parts = ctx.session().config().target_partitions().max(1);
    let repartitioned: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        writer_discovery,
        Partitioning::RoundRobinBatch(target_parts),
    )?);

    let data_scan = Arc::new(IcebergScanByDataFilesExec::new(
        repartitioned,
        table_url.to_string(),
        arrow_schema.clone(),
    ));

    let negated = Arc::new(NotExpr::new(physical_condition));
    let survivors: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(negated, data_scan)?);

    // DELETE always does a full replacement — new files replace all parent manifests.
    assemble_iceberg_commit_plan(
        ctx,
        survivors,
        None,
        arrow_schema,
        Operation::Delete,
        vec![],
    )
    .await
}

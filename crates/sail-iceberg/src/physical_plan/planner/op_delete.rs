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
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::logical_expr::ExprWithSource;
use url::Url;

use super::commit::assemble_iceberg_commit_plan;
use super::context::PlannerContext;
use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::physical_plan::{
    IcebergCommitExec, IcebergDiscoveryExec, IcebergManifestScanExec, IcebergScanByDataFilesExec,
};
use crate::spec::Operation;

/// Build a no-op DELETE/TRUNCATE plan that reports 0 affected rows.
///
/// Used when the target table is empty (has no current snapshot) so TRUNCATE
/// succeeds without error. The commit exec short-circuits when its input yields
/// no commit-meta and no data files, returning a single `count = 0` batch
/// without touching table state.
fn noop_delete_plan(
    table_url: Url,
    lakehouse_table: Option<LakehouseExecutionContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let empty: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(
        datafusion::arrow::datatypes::Schema::empty(),
    )));
    Ok(Arc::new(IcebergCommitExec::new(
        empty,
        table_url,
        lakehouse_table,
    )))
}

pub async fn plan_delete(
    ctx: &PlannerContext<'_>,
    condition: Option<ExprWithSource>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.table();
    let table_url = ctx.table_url().clone();

    // A DELETE or TRUNCATE against a created-but-never-written table (metadata only,
    // no current snapshot) has nothing to delete → report 0 affected rows instead of
    // failing, matching Iceberg/Spark semantics. The commit exec short-circuits on the
    // empty input (no commit-meta and no data files), so table state is untouched.
    if table.metadata().current_snapshot().is_none() {
        return noop_delete_plan(table_url, ctx.lakehouse_table().cloned());
    }

    // TRUNCATE (no WHERE clause): commit an empty snapshot that drops all rows.
    if condition.is_none() {
        let iceberg_schema = table
            .metadata()
            .current_schema()
            .ok_or_else(|| DataFusionError::Plan("Table has no current schema".to_string()))?;
        let arrow_schema = Arc::new(iceberg_schema_to_arrow(iceberg_schema)?);
        let empty_scan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(arrow_schema.clone()));
        return assemble_iceberg_commit_plan(
            ctx,
            empty_scan,
            None,
            arrow_schema,
            Operation::Delete,
            vec![],
            None,
        )
        .await;
    }

    // A conditional DELETE needs the current snapshot to scan the surviving files. It
    // is guaranteed to exist here because the empty-table guard above already returned.
    let snapshot = table
        .metadata()
        .current_snapshot()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "plan_delete: snapshot missing after empty-table guard".to_string(),
            )
        })?;

    let iceberg_schema = table
        .metadata()
        .current_schema()
        .ok_or_else(|| DataFusionError::Plan("Table has no current schema".to_string()))?;
    let arrow_schema = Arc::new(iceberg_schema_to_arrow(iceberg_schema)?);
    let Some(condition) = condition else {
        // Unreachable: TRUNCATE (condition None) is handled above.
        return datafusion_common::internal_err!(
            "plan_delete: missing condition outside TRUNCATE path"
        );
    };

    let df_schema = arrow_schema.clone().to_dfschema()?;
    let physical_condition = ctx
        .session()
        .create_physical_expr(condition.expr.clone(), &df_schema)?;

    // Writer branch: scan → keep survivors.
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
        None,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::UInt64Array;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::{DataFusionError, Result};
    use datafusion::logical_expr::lit;
    use datafusion::physical_plan::common;
    use datafusion::prelude::SessionContext;
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::options::ResolveOptions;
    use crate::options::r#gen::IcebergWriteOptions;

    #[tokio::test]
    async fn noop_delete_plan_reports_zero_count() -> Result<()> {
        let table_url = Url::parse("file:///tmp/noop-delete-test").expect("parse url");
        let plan = noop_delete_plan(table_url, None)?;

        // Matches the output shape of a real DELETE: a single `count` UInt64 column.
        assert_eq!(
            plan.schema().fields(),
            &datafusion::arrow::datatypes::Fields::from(vec![Field::new(
                "count",
                DataType::UInt64,
                true
            )])
        );

        let ctx = datafusion::execution::context::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx())?;
        let batches = common::collect(stream).await?;
        let batch = batches
            .first()
            .ok_or_else(|| DataFusionError::Internal("expected one count batch".into()))?;
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Internal("count column is UInt64".into()))?;
        assert_eq!(count.value(0), 0u64);
        Ok(())
    }

    #[tokio::test]
    async fn conditional_delete_on_snapshotless_table_is_noop() -> Result<()> {
        let io_err = |e: std::io::Error| DataFusionError::External(Box::new(e));

        // A created-but-never-written table (metadata only, no current snapshot) is
        // loaded from a metadata file that lists zero snapshots.
        let temp_dir = tempfile::TempDir::new().map_err(io_err)?;
        let table_url = Url::from_directory_path(temp_dir.path())
            .map_err(|_| DataFusionError::Execution("invalid temp dir URL".to_string()))?;

        std::fs::create_dir_all(temp_dir.path().join("metadata")).map_err(io_err)?;
        let metadata = serde_json::json!({
            "format-version": 2,
            "table-uuid": uuid::Uuid::new_v4(),
            "location": table_url.to_string(),
            "last-updated-ms": 0,
            "last-column-id": 1,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    { "id": 1, "name": "id", "required": false, "type": "long" }
                ]
            }],
            "current-schema-id": 0,
            "current-snapshot-id": null
        });
        let metadata_bytes =
            serde_json::to_vec(&metadata).map_err(|e| DataFusionError::External(Box::new(e)))?;
        std::fs::write(
            temp_dir.path().join("metadata/v1.metadata.json"),
            metadata_bytes,
        )
        .map_err(io_err)?;

        let session_ctx = SessionContext::new();
        session_ctx.register_object_store(&table_url, Arc::new(LocalFileSystem::new()));
        let session = session_ctx.state();
        let options = IcebergWriteOptions::resolve(&session, vec![])?;
        let ctx = PlannerContext::new(&session, options, table_url, None, None, false).await?;

        // A conditional DELETE against the empty table must plan as a 0-row no-op
        // rather than fail with a plan error.
        let plan = plan_delete(&ctx, Some(ExprWithSource::new(lit(true), None))).await?;
        assert_eq!(
            plan.schema().fields(),
            &datafusion::arrow::datatypes::Fields::from(vec![Field::new(
                "count",
                DataType::UInt64,
                true
            )])
        );

        let stream = plan.execute(0, session_ctx.task_ctx())?;
        let batches = common::collect(stream).await?;
        let batch = batches
            .first()
            .ok_or_else(|| DataFusionError::Internal("expected one count batch".into()))?;
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Internal("count column is UInt64".into()))?;
        assert_eq!(count.value(0), 0u64);
        Ok(())
    }
}

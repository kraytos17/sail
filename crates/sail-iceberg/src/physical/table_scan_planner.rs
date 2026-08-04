use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use sail_common_datafusion::datasource::MergeCapableSource;
use sail_logical_plan::merge::RowLevelWriteNode;

use crate::logical::IcebergTableSource;
use crate::physical::row_level_planner::plan_iceberg_row_level_write;
use crate::physical_plan::{
    IcebergDiscoveryExec, IcebergManifestScanExec, IcebergScanByDataFilesExec,
};
use crate::table_format::{plan_iceberg_write, IcebergWriteNode};

pub struct IcebergPhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for IcebergPhysicalPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<IcebergWriteNode>() {
            let [logical_input] = logical_inputs else {
                return datafusion_common::internal_err!(
                    "IcebergWriteNode requires exactly one logical input"
                );
            };
            let [physical_input] = physical_inputs else {
                return datafusion_common::internal_err!(
                    "IcebergWriteNode requires exactly one physical input"
                );
            };
            return plan_iceberg_write(session_state, logical_input, physical_input.clone(), node)
                .await
                .map(Some);
        }

        if let Some(rl_node) = node.as_any().downcast_ref::<RowLevelWriteNode>() {
            if rl_node.target_format().eq_ignore_ascii_case("iceberg") {
                return plan_iceberg_row_level_write(session_state, planner, rl_node)
                    .await
                    .map(Some);
            }
        }

        Ok(None)
    }

    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() else {
            return Ok(None);
        };

        // Row-level operations (e.g. UPDATE) request a per-row file path column.
        // Route to the manifest -> scan-by-data-files chain, which materializes
        // the file path as a synthetic partition column, instead of the
        // DataSourceExec-based provider scan.
        if let Some(file_column) = source.file_column_name() {
            let provider = source.provider();
            let Some(snapshot) = provider.current_snapshot() else {
                return Ok(Some(Arc::new(EmptyExec::new(provider.schema()))));
            };
            let table_url = provider.table_uri().to_string();
            let manifest_scan = Arc::new(IcebergManifestScanExec::new(
                table_url.clone(),
                snapshot.clone(),
            ));
            let discovery = Arc::new(IcebergDiscoveryExec::new(
                manifest_scan,
                table_url.clone(),
                snapshot.snapshot_id(),
                false,
            )?);

            let scan_exec: Arc<dyn ExecutionPlan> =
                Arc::new(IcebergScanByDataFilesExec::new_with_file_path_column(
                    discovery,
                    table_url,
                    provider.schema(),
                    Some(file_column.to_string()),
                ));

            // Apply the scan projection above the chain (the chain reads all
            // columns; filters stay above the scan because the source reports
            // them as unsupported when a file column is present).
            let planned = if let Some(projection) = &scan.projection {
                let scan_schema = scan_exec.schema();
                let projections: Vec<_> = projection
                    .iter()
                    .map(|&idx| {
                        let field = scan_schema.field(idx);
                        (
                            Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>,
                            field.name().to_string(),
                        )
                    })
                    .collect();
                Arc::new(ProjectionExec::try_new(projections, scan_exec)?) as Arc<dyn ExecutionPlan>
            } else {
                scan_exec
            };
            return Ok(Some(planned));
        }

        let filters = unnormalize_cols(scan.filters.clone());
        let plan = source
            .provider()
            .scan(
                session_state,
                scan.projection.as_ref(),
                &filters,
                scan.fetch,
            )
            .await?;
        Ok(Some(plan))
    }
}

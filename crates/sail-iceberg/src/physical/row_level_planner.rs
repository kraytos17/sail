use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::RowLevelCommand;
use sail_logical_plan::merge::RowLevelWriteNode;

use crate::catalog_support::commit_helper::extract_table_properties;
use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::physical_plan::delete_exec::IcebergDeleteExec;
use crate::physical_plan::update_exec::IcebergUpdateExec;
use crate::table::Table;

pub(crate) async fn plan_iceberg_row_level_write(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table_url = url::Url::parse(node.target_location()).map_err(|e| {
        datafusion_common::DataFusionError::Plan(format!("Invalid Iceberg table URL: {e}"))
    })?;

    let table = Table::load(session_state, table_url.clone()).await?;
    if table.metadata().current_snapshot().is_none() {
        return datafusion_common::plan_err!("Cannot modify a table with no data");
    }

    let table_schema = table
        .metadata()
        .current_schema()
        .and_then(|s| iceberg_schema_to_arrow(s).ok())
        .map(Arc::new);

    let lakehouse_table: Option<LakehouseExecutionContext> = node.target_lakehouse_table().cloned();
    let table_properties = extract_table_properties(node.target_options());

    match node.command() {
        RowLevelCommand::Delete => {
            let condition = node.condition().cloned();
            Ok(Arc::new(IcebergDeleteExec::new(
                table_url.to_string(),
                condition,
                session_state.clone(),
                lakehouse_table,
                table_properties,
                table_schema,
            )))
        }
        RowLevelCommand::Update => {
            let condition = node.condition().cloned();
            let assignments = node.assignments().map(|a| a.to_vec());
            Ok(Arc::new(IcebergUpdateExec::new(
                table_url.to_string(),
                assignments.unwrap_or_default(),
                condition,
                session_state.clone(),
                lakehouse_table,
                table_properties,
                table_schema,
            )))
        }
        _ => datafusion_common::not_impl_err!(
            "Unsupported row-level operation: {:?}",
            node.command()
        ),
    }
}

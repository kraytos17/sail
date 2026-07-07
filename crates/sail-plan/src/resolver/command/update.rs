use std::sync::Arc;

use datafusion_common::{DFSchemaRef, ToDFSchema};
use datafusion_expr::LogicalPlan;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{
    LakehouseExecutionContext, LakehouseOperation, TableKind, TableStatus,
};
use sail_common_datafusion::datasource::{
    OptionLayer, SourceInfo, TableFormatRegistry, UpdateInfo,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::expression::expression_before_rename;
use sail_common_datafusion::rename::schema::rename_schema;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the UPDATE command.
    pub(super) async fn resolve_command_update(
        &self,
        table: spec::ObjectName,
        _table_alias: Option<spec::Identifier>,
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        condition: Option<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let table_name: Vec<String> = table.clone().into();
        // Look up the table in the catalog to get its metadata
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let table_status = catalog_manager
            .get_table_or_view(table.parts())
            .await
            .map_err(PlanError::from)?;
        let info = self
            .get_table_info_for_update(&table_status, &table_name)
            .await?;

        let field_ids = state.register_fields(info.schema.fields());

        let original_arrow_schema = Arc::new(info.schema.as_arrow().clone());
        let schema_for_resolution = rename_schema(&original_arrow_schema, &field_ids)?;
        let df_schema_for_resolution = schema_for_resolution.to_dfschema_ref()?;

        // Resolve assignments: (column_name, new_value_expr)
        let mut resolved_assignments = Vec::new();
        for (col_name, value_expr) in assignments {
            let col_name_str: String = {
                let parts: Vec<String> = col_name.clone().into();
                parts.join(".")
            };
            let resolved_value = self
                .resolve_expression(value_expr, &df_schema_for_resolution, state)
                .await?;
            let rewritten_value = expression_before_rename(
                &resolved_value,
                &field_ids,
                &original_arrow_schema,
                true,
            )?;
            resolved_assignments.push((col_name_str, ExprWithSource::new(rewritten_value, None)));
        }

        // Convert the condition expression if present
        let resolved_condition = if let Some(cond) = condition {
            let resolved = self
                .resolve_expression(cond, &df_schema_for_resolution, state)
                .await?;
            let rewritten =
                expression_before_rename(&resolved, &field_ids, &original_arrow_schema, true)?;
            Some(ExprWithSource::new(rewritten, None))
        } else {
            None
        };

        let update_info = UpdateInfo {
            table_name,
            path: info.location,
            condition: resolved_condition,
            assignments: resolved_assignments,
            lakehouse_table: info.lakehouse_table,
            options: vec![OptionLayer::TablePropertyList {
                items: info.properties,
            }],
        };

        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        registry
            .get(&info.format)?
            .create_updater(&self.ctx.state(), update_info)
            .await
            .map_err(PlanError::from)
    }

    async fn get_table_info_for_update(
        &self,
        table_status: &TableStatus,
        table_name: &[String],
    ) -> PlanResult<TableInfo> {
        let (location, format, columns, properties) = match &table_status.kind {
            TableKind::Table {
                location,
                format,
                columns,
                properties,
                ..
            } => (
                location.clone(),
                format.clone(),
                columns.clone(),
                properties.clone(),
            ),
            _ => {
                return Err(PlanError::unsupported(
                    "UPDATE is only supported on tables, not views",
                ));
            }
        };

        let location =
            location.ok_or_else(|| PlanError::unsupported("UPDATE on tables without location"))?;
        let lakehouse_table = self
            .resolve_lakehouse_table_context(
                table_name,
                LakehouseOperation::Read,
                Some(&format),
                vec![],
            )
            .await?;

        let schema = if columns.is_empty() {
            let source_info = SourceInfo {
                paths: vec![location.clone()],
                lakehouse_table: Some(lakehouse_table.clone()),
                schema: None,
                constraints: Default::default(),
                partition_by: vec![],
                bucket_by: None,
                sort_order: vec![],
                options: vec![],
                read_case_sensitive: self.config.case_sensitive,
            };
            let registry = self.ctx.extension::<TableFormatRegistry>()?;
            let table_format = registry.get(&format)?;
            let source = table_format
                .create_source(&self.ctx.state(), source_info)
                .await?;
            source.schema().to_dfschema_ref()?
        } else {
            let schema = datafusion::arrow::datatypes::Schema::new(
                columns.iter().map(|c| c.field()).collect::<Vec<_>>(),
            );
            schema.to_dfschema_ref()?
        };

        Ok(TableInfo {
            location,
            format,
            schema,
            properties,
            lakehouse_table: Some(lakehouse_table.for_operation(LakehouseOperation::Write)),
        })
    }
}

struct TableInfo {
    location: String,
    format: String,
    schema: DFSchemaRef,
    properties: Vec<(String, String)>,
    lakehouse_table: Option<LakehouseExecutionContext>,
}

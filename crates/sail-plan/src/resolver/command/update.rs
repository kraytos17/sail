use std::sync::Arc;

use datafusion_common::{DFSchemaRef, ToDFSchema};
use datafusion_expr::LogicalPlan;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{
    LakehouseExecutionContext, LakehouseOperation, TableKind, TableStatus,
};
use sail_common_datafusion::datasource::{
    OptionLayer, SourceInfo, TableFormatRegistry, UpdateAssignment, UpdateInfo,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::expression::expression_before_rename;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::rename::schema::rename_schema;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the UPDATE command.
    pub(super) async fn resolve_command_update(
        &self,
        table: spec::ObjectName,
        table_alias: Option<spec::Identifier>,
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        condition: Option<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let _ = table_alias;
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

        // Resolve the target table scan so the format layer can build a logical
        // update plan (mirroring MERGE, which passes the resolved target plan).
        let target_plan = self.resolve_update_table_plan(table, state).await?;

        // Convert the condition expression if present
        let condition = if let Some(condition) = condition {
            let resolved_condition = self
                .resolve_expression(condition, &df_schema_for_resolution, state)
                .await?;

            let rewritten_condition = expression_before_rename(
                &resolved_condition,
                &field_ids,
                &original_arrow_schema,
                true,
            )?;

            Some(ExprWithSource::new(rewritten_condition, None))
        } else {
            None
        };

        // Resolve each assignment expression against the table schema
        let mut resolved_assignments = Vec::with_capacity(assignments.len());
        for (target, value) in assignments {
            let column_path: Vec<String> = target.into();
            let resolved_value = self
                .resolve_expression(value, &df_schema_for_resolution, state)
                .await?;
            let rewritten_value = expression_before_rename(
                &resolved_value,
                &field_ids,
                &original_arrow_schema,
                true,
            )?;
            // Cast the assignment value to the target column's data type so the
            // write schema matches the table (e.g. `SET score = 100.0` where
            // `score` is DOUBLE must produce a Float64 column, not Decimal).
            let rewritten_value = if let Some(field) = column_path
                .first()
                .and_then(|name| original_arrow_schema.field_with_name(name).ok())
            {
                let target_type = field.data_type().clone();
                datafusion_expr::cast(rewritten_value, target_type)
            } else {
                rewritten_value
            };
            resolved_assignments.push(UpdateAssignment {
                column_path,
                expression: rewritten_value,
            });
        }

        let update_info = UpdateInfo {
            table_name,
            path: info.location,
            target: Arc::new(target_plan),
            condition,
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

    /// Resolve the target table scan plan for UPDATE.
    async fn resolve_update_table_plan(
        &self,
        name: spec::ObjectName,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let read = spec::ReadNamedTable {
            name,
            temporal: None,
            sample: None,
            options: vec![],
        };
        let plan = spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(read)),
            is_streaming: false,
        });
        let plan = self.resolve_query_plan(plan, state).await?;
        // The resolved scan aliases every column to an opaque field ID (read.rs
        // wraps it with rename_logical_plan). Undo that so the format-layer
        // expansion resolves assignments and the condition by real column name,
        // mirroring resolve_write_input.
        let real_names = Self::get_field_names(plan.schema(), state)?;
        Ok(rename_logical_plan(plan, &real_names)?)
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

        let schema = if columns.is_empty() && format.eq_ignore_ascii_case("DELTA") {
            // Schema is not in catalog, try to infer from data source
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

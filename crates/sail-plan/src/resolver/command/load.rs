use std::sync::Arc;

use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{LakehouseOperation, TableKind};
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::load_data::LoadDataNode;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Resolves `LOAD DATA INPATH '<path>' [OVERWRITE] INTO TABLE ns.tbl`.
    ///
    /// v1 supports remote object-store paths only (`LOCAL` rejected) and no explicit
    /// `PARTITION` clause. Only Iceberg tables are supported; other formats error.
    pub(super) async fn resolve_command_load_data(
        &self,
        local: bool,
        location: String,
        table: spec::ObjectName,
        overwrite: bool,
        partition: Vec<(spec::Identifier, Option<spec::Expr>)>,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if local {
            return Err(PlanError::unsupported("LOAD DATA LOCAL is not supported"));
        }
        if !partition.is_empty() {
            return Err(PlanError::unsupported(
                "LOAD DATA ... PARTITION is not supported",
            ));
        }

        let table_name: Vec<String> = table.clone().into();
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let table_status = catalog_manager
            .get_table_or_view(table.parts())
            .await
            .map_err(PlanError::from)?;

        let (format, table_location, properties) = match &table_status.kind {
            TableKind::Table {
                location,
                format,
                properties,
                ..
            } => (format.clone(), location.clone(), properties.clone()),
            _ => {
                return Err(PlanError::unsupported(
                    "LOAD DATA is only supported on tables, not views",
                ));
            }
        };

        if !format.eq_ignore_ascii_case("iceberg") {
            return Err(PlanError::unsupported(format!(
                "LOAD DATA is only supported for Iceberg tables, got '{format}'"
            )));
        }

        let table_location = table_location
            .ok_or_else(|| PlanError::unsupported("LOAD DATA on tables without location"))?;

        let lakehouse_table = self
            .resolve_lakehouse_table_context(
                &table_name,
                LakehouseOperation::Write,
                Some(&format),
                vec![],
            )
            .await?;

        let options = vec![OptionLayer::TablePropertyList { items: properties }];

        let node = LoadDataNode::new(
            location,
            local,
            overwrite,
            format,
            table_location,
            table_name,
            options,
            Some(lakehouse_table),
        );

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }
}

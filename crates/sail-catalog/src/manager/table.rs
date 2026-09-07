use async_trait::async_trait;
use sail_common_datafusion::catalog::{
    LakehouseCommitClient, LakehouseCommitClientError, LakehouseCommitClientOutcome,
    LakehouseCommitClientRequest, TableStatus,
};

use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::lakehouse::{
    BeginTableAccessRequest, DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse,
    LakehouseCommitOutcome, LakehouseCommitRequest, LakehouseCreatePlan, LakehouseCreateRequest,
    LakehouseResolvedTable, LakehouseScanPlanningRequest, LakehouseScanPlanningResponse,
    ResolveLakehouseTableRequest, TableAccessSession, resolve_lakehouse_table_status,
};
use crate::manager::CatalogManager;
use crate::provider::{
    AlterTableOptions, CreateTableMetadataRequirement, CreateTableOptions, DropTableOptions,
};
use crate::utils::match_pattern;

impl CatalogManager {
    pub async fn create_table<T: AsRef<str>>(
        &self,
        table: &[T],
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.create_table(&database, &table, options).await
    }

    pub fn create_table_metadata_requirement<T: AsRef<str>>(
        &self,
        table: &[T],
        options: &CreateTableOptions,
    ) -> CatalogResult<CreateTableMetadataRequirement> {
        let (provider, _, _) = self.resolve_object(table)?;
        provider.create_table_metadata_requirement(options)
    }

    pub async fn get_table<T: AsRef<str>>(&self, table: &[T]) -> CatalogResult<TableStatus> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.get_table(&database, &table).await
    }

    pub async fn resolve_lakehouse_table<T: AsRef<str>>(
        &self,
        table: &[T],
        request: ResolveLakehouseTableRequest,
    ) -> CatalogResult<LakehouseResolvedTable> {
        let (provider, database, name) = self.resolve_object(table)?;
        provider
            .resolve_lakehouse_table(&database, &name, request)
            .await
    }

    pub async fn resolve_lakehouse_table_status<T: AsRef<str>>(
        &self,
        table: &[T],
        status: &TableStatus,
        operation: sail_common_datafusion::catalog::LakehouseOperation,
    ) -> CatalogResult<LakehouseResolvedTable> {
        let (provider, _, _) = self.resolve_object(table)?;
        let catalog_table = table
            .iter()
            .map(|part| part.as_ref().to_string())
            .collect::<Vec<_>>();
        Ok(resolve_lakehouse_table_status(
            provider.get_name(),
            catalog_table,
            status,
            operation,
            &provider.lakehouse_capabilities(),
        ))
    }

    pub async fn plan_lakehouse_create<T: AsRef<str>>(
        &self,
        table: &[T],
        request: LakehouseCreateRequest,
    ) -> CatalogResult<LakehouseCreatePlan> {
        let (provider, database, name) = self.resolve_object(table)?;
        provider
            .plan_lakehouse_create(&database, &name, request)
            .await
    }

    pub async fn begin_table_access<T: AsRef<str>>(
        &self,
        table: &[T],
        request: BeginTableAccessRequest,
    ) -> CatalogResult<TableAccessSession> {
        let (provider, database, name) = self.resolve_object(table)?;
        provider.begin_table_access(&database, &name, request).await
    }

    pub async fn plan_lakehouse_scan<T: AsRef<str>>(
        &self,
        table: &[T],
        request: LakehouseScanPlanningRequest,
    ) -> CatalogResult<LakehouseScanPlanningResponse> {
        let (provider, database, name) = self.resolve_object(table)?;
        provider
            .plan_lakehouse_scan(&database, &name, request)
            .await
    }

    pub async fn list_tables<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let (provider, database) = if database.is_empty() {
            self.resolve_default_database()?
        } else {
            self.resolve_database(database)?
        };
        Ok(provider
            .list_tables(&database)
            .await?
            .into_iter()
            .filter(|x| match_pattern(&x.name, pattern))
            .collect())
    }

    pub async fn list_tables_and_views<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        // Spark *global* temporary views should be put in the "global temporary" database, and they will be
        // included in the output if the database name matches.
        let mut output = if self.state()?.is_global_temporary_view_database(database) {
            self.list_global_temporary_views(pattern).await?
        } else {
            // Persistent views are stored separately from tables, but Spark's
            // SHOW TABLE EXTENDED includes both tables and views.
            let (tables_res, views_res) = tokio::join!(
                self.list_tables(database, pattern),
                self.list_views(database, pattern),
            );
            let mut tables = tables_res?;
            // Catalogs like OneLake and open-source Unity return NotSupported from
            // list_views; treat that as "no views" so SHOW TABLES still works there.
            let views = match views_res {
                Ok(v) => v,
                Err(CatalogError::NotSupported(_)) | Err(CatalogError::NotFound(_, _)) => vec![],
                Err(e) => return Err(e),
            };
            tables.extend(views);
            tables
        };
        // Spark (local) temporary views are session-scoped and are not associated with a catalog.
        // We should include the temporary views in the output.
        output.extend(self.list_temporary_views(pattern).await?);
        Ok(output)
    }

    pub async fn drop_table<T: AsRef<str>>(
        &self,
        table: &[T],
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.drop_table(&database, &table, options).await
    }

    pub async fn alter_table<T: AsRef<str>>(
        &self,
        table: &[T],
        options: AlterTableOptions,
    ) -> CatalogResult<()> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.alter_table(&database, &table, options).await
    }

    pub async fn commit_lakehouse_table<T: AsRef<str>>(
        &self,
        table: &[T],
        request: LakehouseCommitRequest,
    ) -> CatalogResult<LakehouseCommitOutcome> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider
            .commit_lakehouse_table(&database, &table, request)
            .await
    }

    pub async fn get_delta_ratified_commits<T: AsRef<str>>(
        &self,
        table: &[T],
        request: DeltaRatifiedCommitRequest,
    ) -> CatalogResult<DeltaRatifiedCommitResponse> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider
            .get_delta_ratified_commits(&database, &table, request)
            .await
    }

    pub async fn get_table_or_view<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<TableStatus> {
        if let [name] = reference {
            match self.get_temporary_view(name.as_ref()).await {
                Ok(x) => return Ok(x),
                Err(CatalogError::NotFound(_, _)) => {}
                Err(e) => return Err(e),
            }
        }
        if let [x @ .., name] = reference
            && self.state()?.is_global_temporary_view_database(x)
        {
            return self.get_global_temporary_view(name.as_ref()).await;
        }
        match self.get_table(reference).await {
            Ok(x) => return Ok(x),
            Err(CatalogError::NotFound(_, _)) => {}
            Err(e) => return Err(e),
        }
        match self.get_view(reference).await {
            Ok(x) => Ok(x),
            Err(CatalogError::NotFound(_, name)) => Err(CatalogError::NotFound(
                CatalogObject::Table,
                format!("[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: {name}"),
            )),
            Err(CatalogError::NotSupported(_)) => Err(CatalogError::NotFound(
                CatalogObject::Table,
                format!(
                    "[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: {}",
                    reference.last().map(AsRef::as_ref).unwrap_or("<unknown>")
                ),
            )),
            Err(e) => Err(e),
        }
    }
}

/// Adapts [`CatalogManager`] to the format-crate commit interface so table
/// formats can issue catalog-coordinated commits without depending on this
/// crate (which would be a dependency cycle).
#[async_trait]
impl LakehouseCommitClient for CatalogManager {
    async fn commit_lakehouse_table(
        &self,
        table: &[String],
        request: LakehouseCommitClientRequest,
    ) -> Result<LakehouseCommitClientOutcome, LakehouseCommitClientError> {
        let outcome = self
            .commit_lakehouse_table(
                table,
                LakehouseCommitRequest {
                    context: request.context,
                    format: request.format,
                    requirements: request.requirements,
                    updates: request.updates,
                    payload: request.payload,
                },
            )
            .await
            .map_err(|e| match e {
                CatalogError::NotSupported(message)
                | CatalogError::UnsupportedCapability(message) => {
                    LakehouseCommitClientError::NotSupported(message)
                }
                CatalogError::Conflict(message) => LakehouseCommitClientError::Conflict(message),
                CatalogError::CommitStateUnknown(message) => {
                    LakehouseCommitClientError::StateUnknown(message)
                }
                e => LakehouseCommitClientError::Failed(e.to_string()),
            })?;
        Ok(match outcome {
            LakehouseCommitOutcome::Committed { context, payload } => {
                LakehouseCommitClientOutcome::Committed { context, payload }
            }
            LakehouseCommitOutcome::Noop { context } => {
                LakehouseCommitClientOutcome::Noop { context }
            }
            LakehouseCommitOutcome::RetryableConflict { message } => {
                LakehouseCommitClientOutcome::RetryableConflict { message }
            }
            LakehouseCommitOutcome::StateUnknown { message } => {
                LakehouseCommitClientOutcome::StateUnknown { message }
            }
            LakehouseCommitOutcome::Rejected { message } => {
                LakehouseCommitClientOutcome::Rejected { message }
            }
        })
    }
}

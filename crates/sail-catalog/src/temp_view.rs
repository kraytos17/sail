use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use datafusion_expr::LogicalPlan;
use lazy_static::lazy_static;
use sail_common_datafusion::catalog::{TableColumnStatus, TemporaryViewSource};

use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::provider::{CreateTemporaryViewColumnOptions, CreateTemporaryViewOptions};
use crate::utils::match_pattern;

lazy_static! {
    pub(crate) static ref GLOBAL_TEMPORARY_VIEW_MANAGER: TemporaryViewManager =
        TemporaryViewManager::default();
}

#[derive(Debug, Clone)]
pub struct TemporaryView {
    plan: Arc<LogicalPlan>,
    columns: Vec<TableColumnStatus>,
    comment: Option<String>,
    properties: Vec<(String, String)>,
    source: Option<TemporaryViewSource>,
}

#[derive(Debug, Clone)]
pub struct TemporaryViewColumn {
    pub comment: Option<String>,
}

impl TemporaryView {
    #[must_use]
    pub const fn plan(&self) -> &Arc<LogicalPlan> {
        &self.plan
    }

    #[must_use]
    pub fn columns(&self) -> &[TableColumnStatus] {
        &self.columns
    }

    #[must_use]
    pub const fn comment(&self) -> Option<&String> {
        self.comment.as_ref()
    }

    #[must_use]
    pub fn properties(&self) -> &[(String, String)] {
        &self.properties
    }

    #[must_use]
    pub const fn source(&self) -> Option<&TemporaryViewSource> {
        self.source.as_ref()
    }
}

#[derive(Debug)]
pub struct TemporaryViewManager {
    views: RwLock<HashMap<String, Arc<TemporaryView>>>,
}

impl Default for TemporaryViewManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TemporaryViewManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
        }
    }

    fn read(&self) -> CatalogResult<RwLockReadGuard<'_, HashMap<String, Arc<TemporaryView>>>> {
        self.views
            .read()
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }

    fn write(&self) -> CatalogResult<RwLockWriteGuard<'_, HashMap<String, Arc<TemporaryView>>>> {
        self.views
            .write()
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }

    /// # Errors
    /// Returns an error if the temporary view already exists or if column count mismatches.
    pub fn create_view(
        &self,
        name: String,
        options: CreateTemporaryViewOptions,
    ) -> CatalogResult<()> {
        let CreateTemporaryViewOptions {
            input,
            columns,
            if_not_exists,
            replace,
            comment,
            properties,
            source,
        } = options;
        let mut views = self.write()?;
        if views.contains_key(&name) {
            if if_not_exists {
                return Ok(());
            } else if !replace {
                return Err(CatalogError::AlreadyExists(
                    CatalogObject::TemporaryView,
                    name,
                ));
            }
        }
        let comments = if columns.is_empty() {
            vec![None; input.schema().fields().len()]
        } else if input.schema().fields().len() != columns.len() {
            return Err(CatalogError::InvalidArgument(
                "temporary view column count do not match input schema".to_string(),
            ));
        } else {
            columns
                .into_iter()
                .map(|column| {
                    let CreateTemporaryViewColumnOptions { comment } = column;
                    comment
                })
                .collect()
        };
        let columns = input
            .schema()
            .fields()
            .iter()
            .zip(comments)
            .map(|(field, comment)| {
                Ok(TableColumnStatus {
                    name: field.name().clone(),
                    data_type: field.data_type().clone(),
                    nullable: field.is_nullable(),
                    comment,
                    default: None,
                    generated_always_as: None,
                    identity: None,
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                })
            })
            .collect::<CatalogResult<Vec<_>>>()?;
        let view = TemporaryView {
            plan: input,
            columns,
            comment,
            properties,
            source,
        };
        views.insert(name, Arc::new(view));
        drop(views);
        Ok(())
    }

    /// # Errors
    /// Returns an error if the temporary view does not exist and `if_exists` is false.
    pub fn drop_view(&self, name: &str, if_exists: bool) -> CatalogResult<()> {
        let mut views = self.write()?;
        if !views.contains_key(name) && !if_exists {
            return Err(CatalogError::NotFound(
                CatalogObject::TemporaryView,
                name.to_string(),
            ));
        }
        views.remove(name);
        drop(views);
        Ok(())
    }

    /// # Errors
    /// Returns an error if the temporary view does not exist.
    pub fn get_view(&self, name: &str) -> CatalogResult<Arc<TemporaryView>> {
        let views = self.read()?;
        let view = views.get(name).ok_or_else(|| {
            CatalogError::NotFound(CatalogObject::TemporaryView, name.to_string())
        })?;
        Ok(Arc::clone(view))
    }

    pub fn list_views(
        &self,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<(String, Arc<TemporaryView>)>> {
        let views = self.read()?;
        Ok(views
            .iter()
            .filter(|(name, _)| match_pattern(name.as_str(), pattern))
            .map(|(name, plan)| (name.clone(), Arc::clone(plan)))
            .collect())
    }
}

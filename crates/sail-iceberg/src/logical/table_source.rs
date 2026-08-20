use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource};
use sail_common_datafusion::datasource::MergeCapableSource;

use crate::datasource::provider::IcebergTableProvider;

#[derive(Clone)]
pub struct IcebergTableSource {
    provider: Arc<IcebergTableProvider>,
    /// When set, each row is tagged with its source file path in a column of
    /// this name (used by row-level operations for targeted rewrite).
    file_column: Option<String>,
}

impl std::fmt::Debug for IcebergTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergTableSource")
            .field("table_uri", &self.provider.table_uri())
            .field(
                "schema_fields",
                &self
                    .provider
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl IcebergTableSource {
    pub fn new(provider: Arc<IcebergTableProvider>) -> Self {
        Self {
            provider,
            file_column: None,
        }
    }

    pub fn provider(&self) -> &Arc<IcebergTableProvider> {
        &self.provider
    }
}

impl TableSource for IcebergTableSource {
    fn schema(&self) -> SchemaRef {
        let base = self.provider.schema();
        let Some(file_column) = &self.file_column else {
            return base;
        };
        if base.field_with_name(file_column).is_ok() {
            return base;
        }
        let mut fields = base.fields().to_vec();
        fields.push(Arc::new(Field::new(
            file_column.clone(),
            DataType::Utf8,
            true,
        )));
        Arc::new(Schema::new(fields))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        if self.file_column.is_some() {
            // The row-level scan path (manifest -> scan-by-data-files) reads all
            // files and does not apply filters itself, so keep predicates above
            // the scan where the planner applies them (mirrors the
            // metadata-as-data read path in the provider).
            return Ok(vec![TableProviderFilterPushDown::Unsupported; filter.len()]);
        }
        self.provider.supports_filters_pushdown(filter)
    }
}

impl MergeCapableSource for IcebergTableSource {
    fn file_column_name(&self) -> Option<&str> {
        self.file_column.as_deref()
    }

    fn with_file_column(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        let mut source = self.clone();
        source.file_column = Some(name.to_string());
        Ok(Arc::new(source))
    }

    fn row_index_column_name(&self) -> Option<&str> {
        None
    }

    fn with_row_index_column(&self, _name: &str) -> Result<Arc<dyn TableSource>> {
        // Iceberg UPDATE does not use deletion vectors (targeted rewrite only needs
        // the file path column); row-index support can be added with DELETE-by-DV.
        Ok(Arc::new(self.clone()))
    }
}

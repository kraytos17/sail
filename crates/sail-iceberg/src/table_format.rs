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

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, Extension, UserDefinedLogicalNodeCore};
use educe::Educe;
use log::warn;
use object_store::ObjectStoreExt;
use sail_common_datafusion::catalog::iceberg::is_iceberg_table_marker;
use sail_common_datafusion::catalog::managed::metadata_location_value;
use sail_common_datafusion::catalog::{
    CatalogPartitionField, CommitAuthority, LakehouseExecutionContext, PartitionTransform,
    ScanAuthority,
};
use sail_common_datafusion::datasource::{
    create_sort_order, find_path_in_options, BucketBy, DeleteInfo, OptionLayer, PhysicalSinkMode,
    SinkInfo, SinkMode, SourceInfo, TableFormat, TableFormatAlterTableOperation,
    TableFormatCreateTableColumn, TableFormatCreateTableInfo, TableFormatCreateTableResult,
    TableFormatRegistry, UpdateInfo,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::with_variant_extension_if_marked_storage;
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::merge::RowLevelWriteNode;
use url::Url;

use crate::datasource::provider::IcebergTableProvider;
use crate::datasource::type_converter::{
    arrow_schema_to_iceberg, arrow_type_to_iceberg, ICEBERG_ARROW_FIELD_DOC_KEY,
};
use crate::io::StoreContext;
use crate::logical::IcebergTableSource;
use crate::operations::bootstrap::{
    bootstrap_empty_table_metadata, replace_empty_table_metadata, NewTableMetadataStyle,
};
use crate::options::gen::{IcebergReadOptions, IcebergWriteOptions};
use crate::physical_plan::plan_builder::{IcebergPlanBuilder, IcebergTableConfig};
use crate::physical_plan::IcebergWriterExecOptions;
use crate::schema_evolution::SchemaEvolver;
use crate::spec::{MetadataLog, PartitionSpec, Schema, Snapshot, TableMetadata};
use crate::table::metadata_loader::{
    encode_metadata_file, load_metadata_file_bytes, metadata_file_extension_from_properties,
    metadata_file_version_from_path, metadata_location_to_object_path_string,
};
use crate::table::{find_latest_metadata_file, Table};
use crate::utils::metadata::metadata_files_for_version;
use crate::utils::partition_transform::{
    catalog_partition_field_from_iceberg, format_partition_expr, format_partition_exprs,
    iceberg_transform_from_partition_field, partition_field_name,
};
use crate::utils::timestamp::monotonic_timestamp_ms;
use crate::utils::{parse_absolute_url, url_to_object_path};

const MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES: usize = 5;

/// Iceberg implementation of [`TableFormat`].
#[derive(Debug, Default)]
pub struct IcebergTableFormat;

impl IcebergTableFormat {
    pub fn register(registry: &TableFormatRegistry) -> Result<()> {
        registry.register(Arc::new(Self))
    }

    /// Run table compaction on the Iceberg table at the given path.
    /// Small files below 75% of `target_file_size` (default 128 MB) are merged.
    pub async fn compact_table(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        target_file_size: Option<u64>,
        lakehouse_table: Option<LakehouseExecutionContext>,
        table_properties: Vec<(String, String)>,
    ) -> Result<()> {
        use datafusion::execution::session_state::SessionStateBuilder;

        use crate::physical_plan::compact_exec::run_compaction;

        let table_url = Self::parse_table_url(vec![path.to_string()]).await?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let session_state = SessionStateBuilder::new()
            .with_runtime_env(runtime_env.clone())
            .with_default_features()
            .build();

        run_compaction(
            &table_url,
            object_store,
            target_file_size.unwrap_or(134_217_728),
            &session_state,
            None,
            lakehouse_table.as_ref(),
            &table_properties,
        )
        .await
    }
}

#[async_trait]
impl TableFormat for IcebergTableFormat {
    fn name(&self) -> &str {
        "iceberg"
    }

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        let provider = build_iceberg_provider(ctx, info).await?;
        Ok(Arc::new(IcebergTableSource::new(provider)))
    }

    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<datafusion::arrow::datatypes::SchemaRef> {
        Ok(self.create_source(ctx, info).await?.schema())
    }

    async fn infer_metadata(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<sail_common_datafusion::datasource::TableFormatMetadata> {
        Ok(sail_common_datafusion::datasource::TableFormatMetadata {
            schema: self.infer_schema(ctx, info).await?,
            properties: vec![],
        })
    }

    async fn create_writer(&self, _ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        let Some(path) = find_path_in_options(&info.options) else {
            return plan_err!("missing path in Iceberg table options");
        };
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            lakehouse_table,
        } = info;

        let partition_by = match bucket_by {
            Some(bucket_by) => {
                let mut fields = partition_by;
                fields.extend(partition_fields_from_bucket_by(bucket_by));
                fields
            }
            None => partition_by,
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(IcebergWriteNode::new(
                Arc::new(input),
                IcebergWriteNodeOptions {
                    path,
                    mode,
                    partition_by,
                    sort_order,
                    options,
                    lakehouse_table,
                },
            )),
        }))
    }

    async fn create_table_metadata(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        info: TableFormatCreateTableInfo,
    ) -> Result<TableFormatCreateTableResult> {
        let TableFormatCreateTableInfo {
            path,
            columns,
            comment: _,
            partition_by,
            bucket_by,
            properties,
            replace,
            lakehouse_table,
        } = info;

        let partition_by = match bucket_by {
            Some(bucket_by) => {
                let mut fields = partition_by;
                fields.extend(partition_fields_from_bucket_by(bucket_by));
                fields
            }
            None => partition_by,
        };
        let catalog_table = lakehouse_table
            .as_ref()
            .map(|context| context.catalog_table().to_vec());

        let table_url = Self::parse_table_url(vec![path]).await?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let existing_metadata = match find_latest_metadata_file(&object_store, &table_url).await {
            Ok(metadata_file) if columns.is_empty() && !replace => {
                let metadata_location = table_metadata_location(&table_url, &metadata_file)?;
                return Ok(TableFormatCreateTableResult {
                    properties: vec![(
                        sail_common_datafusion::catalog::managed::METADATA_LOCATION_UNDERSCORE_KEY
                            .to_string(),
                        metadata_location,
                    )],
                });
            }
            Ok(metadata_file) => {
                let metadata_data = load_metadata_file_bytes(&object_store, &metadata_file).await?;
                let metadata = TableMetadata::from_json(&metadata_data)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                if !replace {
                    return plan_err!("Iceberg table metadata already exists at path: {table_url}");
                }
                Some((metadata_file, metadata))
            }
            Err(err)
                if err.to_string().contains("No metadata files found")
                    && columns.is_empty()
                    && !replace =>
            {
                return plan_err!("Iceberg CREATE TABLE requires at least one column");
            }
            Err(err) if err.to_string().contains("No metadata files found") => None,
            Err(err) => return Err(err),
        };

        let arrow_schema = create_table_arrow_schema(columns)?;
        let mut iceberg_schema = arrow_schema_to_iceberg(&arrow_schema)?;
        iceberg_schema = if let Some((_, metadata)) = existing_metadata.as_ref() {
            let next_field_id = metadata.last_column_id + 1;
            let schema =
                SchemaEvolver::assign_schema_field_ids_starting_at(&iceberg_schema, next_field_id)?;
            iceberg_schema_with_id(&schema, next_schema_id(metadata))?
        } else {
            SchemaEvolver::assign_schema_field_ids(&iceberg_schema)?
        };
        if iceberg_schema.fields().iter().any(|field| field.id == 0) {
            return plan_err!("Invalid Iceberg schema: field id 0 detected after assignment");
        }

        let mut partition_spec = create_table_partition_spec(&iceberg_schema, &partition_by)?;
        if let Some((_, metadata)) = existing_metadata.as_ref() {
            partition_spec = partition_spec.with_spec_id(next_partition_spec_id(metadata));
        }
        let table_properties = iceberg_table_properties_from_catalog_create(properties)?;
        let store_ctx = StoreContext::new(object_store, &table_url)?;
        let metadata_style = if catalog_table.is_some() {
            NewTableMetadataStyle::Uuid
        } else {
            NewTableMetadataStyle::Hadoop
        };
        let bootstrap = if let Some((metadata_file, previous_metadata)) = existing_metadata.as_ref()
        {
            replace_empty_table_metadata(
                &table_url,
                &store_ctx,
                iceberg_schema,
                partition_spec,
                &table_properties,
                previous_metadata,
                metadata_file,
                metadata_style,
            )
            .await?
        } else {
            bootstrap_empty_table_metadata(
                &table_url,
                &store_ctx,
                iceberg_schema,
                partition_spec,
                &table_properties,
                metadata_style,
            )
            .await?
        };
        let metadata_location = table_url
            .join(&bootstrap.metadata_file)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .to_string();

        Ok(TableFormatCreateTableResult {
            properties: vec![(
                sail_common_datafusion::catalog::managed::METADATA_LOCATION_UNDERSCORE_KEY
                    .to_string(),
                metadata_location,
            )],
        })
    }

    async fn create_deleter(&self, _ctx: &dyn Session, info: DeleteInfo) -> Result<LogicalPlan> {
        let DeleteInfo {
            table_name,
            path,
            condition,
            lakehouse_table,
            options,
        } = info;
        let write_node = RowLevelWriteNode::new_delete(
            Arc::new(LogicalPlan::EmptyRelation(
                datafusion_expr::logical_plan::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(datafusion_common::DFSchema::empty()),
                },
            )),
            Arc::new(datafusion_common::DFSchema::empty()),
            condition,
            self.name().to_string(),
            path,
            table_name,
            options,
            lakehouse_table,
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(write_node),
        }))
    }

    async fn create_updater(&self, _ctx: &dyn Session, info: UpdateInfo) -> Result<LogicalPlan> {
        let UpdateInfo {
            table_name,
            path,
            condition,
            assignments,
            lakehouse_table,
            options,
        } = info;
        let write_node = RowLevelWriteNode::new_update(
            Arc::new(LogicalPlan::EmptyRelation(
                datafusion_expr::logical_plan::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(datafusion_common::DFSchema::empty()),
                },
            )),
            Arc::new(datafusion_common::DFSchema::empty()),
            assignments,
            condition,
            self.name().to_string(),
            path,
            table_name,
            options,
            lakehouse_table,
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(write_node),
        }))
    }

    async fn alter_table(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        operation: TableFormatAlterTableOperation,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Result<()> {
        reject_catalog_managed_iceberg_alter(lakehouse_table.as_ref())?;
        match operation {
            TableFormatAlterTableOperation::SetTableProperties { changes, if_exists } => {
                self.alter_table_properties(runtime_env, path, changes, if_exists)
                    .await
            }
            TableFormatAlterTableOperation::AddColumns { columns } => {
                self.alter_table_add_columns(runtime_env, path, columns)
                    .await
            }
            TableFormatAlterTableOperation::DropColumns { names, if_exists } => {
                self.alter_table_drop_columns(runtime_env, path, names, if_exists)
                    .await
            }
            op => not_impl_err!("unsupported Iceberg ALTER TABLE operation: {op:?}"),
        }
    }
}

fn reject_catalog_managed_iceberg_alter(
    lakehouse_table: Option<&LakehouseExecutionContext>,
) -> Result<()> {
    let Some(context) = lakehouse_table else {
        return Ok(());
    };
    if context.commit != CommitAuthority::Filesystem {
        return not_impl_err!(
            "ALTER TABLE is not yet supported for catalog-managed Iceberg tables: {}",
            context.catalog_table().join(".")
        );
    }
    Ok(())
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash, PartialOrd)]
pub struct IcebergWriteNodeOptions {
    pub path: String,
    pub mode: SinkMode,
    pub partition_by: Vec<CatalogPartitionField>,
    pub sort_order: Vec<Sort>,
    pub options: Vec<OptionLayer>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash, PartialOrd)]
pub struct IcebergWriteNode {
    input: Arc<LogicalPlan>,
    options: IcebergWriteNodeOptions,
    #[educe(PartialOrd(ignore))]
    schema: datafusion_common::DFSchemaRef,
}

impl IcebergWriteNode {
    pub fn new(input: Arc<LogicalPlan>, options: IcebergWriteNodeOptions) -> Self {
        Self {
            input,
            options,
            schema: Arc::new(datafusion_common::DFSchema::empty()),
        }
    }

    pub fn options(&self) -> &IcebergWriteNodeOptions {
        &self.options
    }
}

impl UserDefinedLogicalNodeCore for IcebergWriteNode {
    fn name(&self) -> &str {
        "IcebergWrite"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "IcebergWrite: options={:?}", self.options)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        Ok(Self {
            input: Arc::new(inputs.one()?),
            options: self.options.clone(),
            schema: self.schema.clone(),
        })
    }
}

pub(crate) async fn plan_iceberg_write(
    ctx: &SessionState,
    logical_input: &LogicalPlan,
    physical_input: Arc<dyn ExecutionPlan>,
    node: &IcebergWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    use datafusion::physical_plan::empty::EmptyExec;

    let IcebergWriteNodeOptions {
        path,
        mode,
        partition_by,
        sort_order,
        options,
        lakehouse_table,
    } = node.options().clone();

    let mode = match mode {
        SinkMode::ErrorIfExists => PhysicalSinkMode::ErrorIfExists,
        SinkMode::IgnoreIfExists => PhysicalSinkMode::IgnoreIfExists,
        SinkMode::Append => PhysicalSinkMode::Append,
        SinkMode::Overwrite => PhysicalSinkMode::Overwrite,
        SinkMode::OverwriteIf { condition } => {
            let source = condition.source.clone();
            PhysicalSinkMode::OverwriteIf {
                condition: Some(condition),
                source,
            }
        }
        SinkMode::OverwritePartitions => PhysicalSinkMode::OverwritePartitions,
    };
    validate_iceberg_lakehouse_storage_access(lakehouse_table.as_ref())?;
    let metadata_location = metadata_location_from_options(&options);
    let catalog_managed_table = catalog_managed_iceberg_from_options(&options);
    let (clean_options, table_properties) =
        split_iceberg_write_options_and_table_properties(options)?;
    let variant_shredding_option_presence =
        IcebergWriterExecOptions::variant_shredding_option_presence(&clean_options);
    let iceberg_options = IcebergWriteOptions::resolve(ctx, clean_options)?;

    let sort_order = create_sort_order(ctx, sort_order, logical_input.schema())?;
    let physical_sort = sort_order.map(|req| {
        req.into_iter()
            .map(|r| datafusion::physical_expr::PhysicalSortExpr {
                expr: r.expr,
                options: r.options.unwrap_or_default(),
            })
            .collect::<Vec<_>>()
    });

    let table_url = IcebergTableFormat::parse_table_url(vec![path]).await?;

    let store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let exists_res = match metadata_location.as_deref() {
        Some(location) if catalog_managed_table => {
            metadata_location_to_object_path_string(location)
        }
        _ => find_latest_metadata_file(&store, &table_url).await,
    };
    let table_exists = exists_res.is_ok();

    match mode {
        PhysicalSinkMode::ErrorIfExists if table_exists => {
            return plan_err!("Iceberg table already exists at path: {table_url}");
        }
        PhysicalSinkMode::IgnoreIfExists if table_exists => {
            return Ok(Arc::new(EmptyExec::new(physical_input.schema())));
        }
        PhysicalSinkMode::OverwriteIf { .. } => {}
        PhysicalSinkMode::OverwritePartitions => {}
        _ => {}
    }

    let existing_partition_columns = if table_exists {
        let metadata_location = catalog_managed_table.then_some(metadata_location).flatten();
        let table =
            Table::load_with_metadata_location(ctx, table_url.clone(), metadata_location).await?;
        Some(IcebergTableFormat::partition_columns_from_metadata(&table)?)
    } else {
        None
    };

    // Validate overwrite predicate only references partition columns (v1 constraint)
    if let PhysicalSinkMode::OverwriteIf {
        condition: Some(condition),
        ..
    } = &mode
    {
        let partition_columns = existing_partition_columns.as_deref().ok_or_else(|| {
            DataFusionError::Plan("Predicate overwrite requires an existing table".to_string())
        })?;
        let predicate_columns: Vec<&str> = condition
            .expr
            .column_refs()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        for col_name in &predicate_columns {
            if !partition_columns.iter().any(|p| p.column == *col_name) {
                return not_impl_err!(
                    "Predicate overwrite on non-partition column '{}' is not yet supported",
                    col_name
                );
            }
        }
    }

    if let Some(existing_partitions) = &existing_partition_columns {
        if !partition_by.is_empty() && partition_by != *existing_partitions {
            match mode {
                PhysicalSinkMode::Append => {
                    return plan_err!(
                        "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                        Cannot change partitioning on append.",
                        format_partition_exprs(existing_partitions),
                        format_partition_exprs(&partition_by)
                    );
                }
                PhysicalSinkMode::Overwrite if !iceberg_options.overwrite_schema => {
                    return plan_err!(
                        "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                        Set overwriteSchema=true to change partitioning.",
                        format_partition_exprs(existing_partitions),
                        format_partition_exprs(&partition_by)
                    );
                }
                _ => {}
            }
        }
    }

    let resolved_partition_columns = if !partition_by.is_empty() {
        partition_by
    } else {
        existing_partition_columns.unwrap_or_default()
    };

    let mut options = IcebergWriterExecOptions::from(iceberg_options);
    options.apply_variant_shredding_option_presence(variant_shredding_option_presence);
    options.table_properties = table_properties;
    options.lakehouse_table = lakehouse_table;
    // Extract overwrite predicate from sink mode as JSON partition key-value pairs
    if let PhysicalSinkMode::OverwriteIf {
        condition: Some(condition),
        ..
    } = &mode
    {
        let partition_predicate = extract_partition_predicate_from_expr(&condition.expr);
        options.overwrite_predicate = partition_predicate
            .map(|map| serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string()));
    }
    let table_config = IcebergTableConfig {
        table_url,
        partition_columns: resolved_partition_columns,
        table_exists,
        options,
    };

    let logical_input_schema = Arc::new(logical_input.schema().as_arrow().clone());
    let builder = IcebergPlanBuilder::new(
        physical_input,
        table_config,
        mode,
        physical_sort,
        Some(logical_input_schema),
        ctx,
    );
    builder.build().await
}

impl IcebergTableFormat {
    async fn alter_table_properties(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        changes: Vec<(String, Option<String>)>,
        if_exists: bool,
    ) -> Result<()> {
        let table_url = Self::parse_table_url(vec![path.to_string()]).await?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;

        let initial_latest_meta = find_latest_metadata_file(&object_store, &table_url).await?;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let latest_meta = if attempt == 1 {
                initial_latest_meta.clone()
            } else {
                find_latest_metadata_file(&object_store, &table_url).await?
            };

            let bytes = load_metadata_file_bytes(&object_store, &latest_meta).await?;
            let mut table_meta = TableMetadata::from_json(&bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            crate::properties::apply_table_property_changes(&mut table_meta, &changes, if_exists)?;

            let current_version = metadata_file_version_from_path(&latest_meta).unwrap_or(0);
            let next_version = current_version + 1;
            let existing_for_next = metadata_files_for_version(&store_ctx, next_version).await?;
            if !existing_for_next.is_empty() {
                log::warn!(
                    "Detected existing Iceberg metadata files for version {}: {:?}. Retrying attempt {}",
                    next_version,
                    existing_for_next,
                    attempt
                );
                if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                    return Err(alter_table_properties_conflict_error());
                }
                continue;
            }

            let timestamp_ms = monotonic_timestamp_ms();
            table_meta.last_updated_ms = timestamp_ms;
            table_meta.metadata_log.push(MetadataLog {
                timestamp_ms,
                metadata_file: latest_meta.clone(),
            });

            let new_meta_bytes = table_meta
                .to_json()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
            let new_meta_rel = format!("metadata/v{next_version}{file_extension}");
            let new_meta_bytes = encode_metadata_file(&new_meta_rel, &new_meta_bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let new_meta_path = object_store::path::Path::from(new_meta_rel.as_str());
            let put_opts = object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            };
            let payload = object_store::PutPayload::from(Bytes::from(new_meta_bytes));
            match store_ctx
                .prefixed
                .put_opts(&new_meta_path, payload, put_opts)
                .await
            {
                Ok(_) => {}
                Err(object_store::Error::AlreadyExists { .. }) => {
                    log::warn!(
                        "Iceberg metadata file {} already exists for version {}. Retrying attempt {}",
                        new_meta_rel,
                        next_version,
                        attempt
                    );
                    if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                        return Err(alter_table_properties_conflict_error());
                    }
                    continue;
                }
                Err(e) => return Err(DataFusionError::External(Box::new(e))),
            }

            let version_files = metadata_files_for_version(&store_ctx, next_version).await?;
            let conflict_after_write = version_files.iter().any(|path| path != &new_meta_rel);
            if conflict_after_write {
                log::warn!(
                    "Concurrent Iceberg metadata writes detected for version {}: {:?}. Retrying attempt {}",
                    next_version,
                    version_files,
                    attempt
                );
                if let Err(err) = store_ctx.prefixed.delete(&new_meta_path).await {
                    log::warn!(
                        "Failed to delete conflicted Iceberg metadata file {}: {:?}",
                        new_meta_rel,
                        err
                    );
                }
                if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                    return Err(alter_table_properties_conflict_error());
                }
                continue;
            }

            let hint_path = object_store::path::Path::from("metadata/version-hint.text");
            store_ctx
                .prefixed
                .put(
                    &hint_path,
                    object_store::PutPayload::from(Bytes::from(next_version.to_string())),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            return Ok(());
        }
    }

    async fn alter_table_add_columns(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        columns: Vec<TableFormatCreateTableColumn>,
    ) -> Result<()> {
        let table_url = Self::parse_table_url(vec![path.to_string()]).await?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;

        let initial_latest_meta = find_latest_metadata_file(&object_store, &table_url).await?;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let latest_meta = if attempt == 1 {
                initial_latest_meta.clone()
            } else {
                find_latest_metadata_file(&object_store, &table_url).await?
            };

            let bytes = load_metadata_file_bytes(&object_store, &latest_meta).await?;
            let mut table_meta = TableMetadata::from_json(&bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let current_schema = table_meta.current_schema().cloned().ok_or_else(|| {
                DataFusionError::Plan("No current schema in table metadata".to_string())
            })?;

            let mut new_fields: Vec<std::sync::Arc<crate::spec::types::NestedField>> =
                current_schema.fields().iter().cloned().collect();
            let mut next_id = table_meta.last_column_id + 1;

            for col in &columns {
                let iceberg_type = arrow_type_to_iceberg(&col.data_type)?;
                let field =
                    crate::spec::types::NestedField::optional(next_id, &col.name, iceberg_type);
                new_fields.push(std::sync::Arc::new(field));
                next_id += 1;
            }
            let new_schema_id = next_schema_id(&table_meta);

            let new_schema = crate::spec::Schema::builder()
                .with_schema_id(new_schema_id)
                .with_fields(new_fields)
                .build()
                .map_err(|e| DataFusionError::Plan(format!("Schema build error: {e}")))?;

            table_meta.last_column_id = next_id - 1;
            table_meta.schemas.push(new_schema.clone());
            table_meta.current_schema_id = new_schema_id;

            let current_version = metadata_file_version_from_path(&latest_meta).unwrap_or(0);
            let next_version = current_version + 1;
            let existing_for_next = metadata_files_for_version(&store_ctx, next_version).await?;
            if !existing_for_next.is_empty() {
                log::warn!(
                    "Detected existing Iceberg metadata files for version {}: {:?}. Retrying attempt {}",
                    next_version,
                    existing_for_next,
                    attempt
                );
                if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                    return Err(alter_table_properties_conflict_error());
                }
                continue;
            }

            let timestamp_ms = monotonic_timestamp_ms();
            table_meta.last_updated_ms = timestamp_ms;
            table_meta.metadata_log.push(MetadataLog {
                timestamp_ms,
                metadata_file: latest_meta.clone(),
            });

            let new_meta_bytes = table_meta
                .to_json()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
            let new_meta_rel = format!("metadata/v{next_version}{file_extension}");
            let new_meta_bytes = encode_metadata_file(&new_meta_rel, &new_meta_bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let new_meta_path = object_store::path::Path::from(new_meta_rel.as_str());
            let put_opts = object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            };
            let payload = object_store::PutPayload::from(Bytes::from(new_meta_bytes));
            match store_ctx
                .prefixed
                .put_opts(&new_meta_path, payload, put_opts)
                .await
            {
                Ok(_) => {}
                Err(object_store::Error::AlreadyExists { .. }) => {
                    log::warn!(
                        "Iceberg metadata file {} already exists for version {}. Retrying attempt {}",
                        new_meta_rel,
                        next_version,
                        attempt
                    );
                    if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                        return Err(alter_table_properties_conflict_error());
                    }
                    continue;
                }
                Err(e) => return Err(DataFusionError::External(Box::new(e))),
            }

            let version_files = metadata_files_for_version(&store_ctx, next_version).await?;
            let conflict_after_write = version_files.iter().any(|path| path != &new_meta_rel);
            if conflict_after_write {
                log::warn!(
                    "Concurrent Iceberg metadata writes detected for version {}: {:?}. Retrying attempt {}",
                    next_version,
                    version_files,
                    attempt
                );
                if let Err(err) = store_ctx.prefixed.delete(&new_meta_path).await {
                    log::warn!(
                        "Failed to delete conflicted Iceberg metadata file {}: {:?}",
                        new_meta_rel,
                        err
                    );
                }
                if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                    return Err(alter_table_properties_conflict_error());
                }
                continue;
            }

            let hint_path = object_store::path::Path::from("metadata/version-hint.text");
            store_ctx
                .prefixed
                .put(
                    &hint_path,
                    object_store::PutPayload::from(Bytes::from(next_version.to_string())),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            return Ok(());
        }
    }

    async fn alter_table_drop_columns(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        names: Vec<String>,
        if_exists: bool,
    ) -> Result<()> {
        let table_url = Self::parse_table_url(vec![path.to_string()]).await?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;

        let initial_latest_meta = find_latest_metadata_file(&object_store, &table_url).await?;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let latest_meta = if attempt == 1 {
                initial_latest_meta.clone()
            } else {
                find_latest_metadata_file(&object_store, &table_url).await?
            };

            let bytes = load_metadata_file_bytes(&object_store, &latest_meta).await?;
            let mut table_meta = TableMetadata::from_json(&bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let current_schema = table_meta.current_schema().cloned().ok_or_else(|| {
                DataFusionError::Plan("No current schema in table metadata".to_string())
            })?;

            let mut new_fields: Vec<std::sync::Arc<crate::spec::types::NestedField>> =
                current_schema.fields().iter().cloned().collect();
            for name in &names {
                let pos = new_fields.iter().position(|f| f.name == *name);
                match pos {
                    Some(idx) => {
                        new_fields.remove(idx);
                    }
                    None => {
                        if !if_exists {
                            return Err(DataFusionError::Plan(format!(
                                "Column '{}' not found in Iceberg table schema",
                                name
                            )));
                        }
                    }
                }
            }

            let new_schema_id = next_schema_id(&table_meta);

            let new_schema = crate::spec::Schema::builder()
                .with_schema_id(new_schema_id)
                .with_fields(new_fields)
                .build()
                .map_err(|e| DataFusionError::Plan(format!("Schema build error: {e}")))?;

            table_meta.schemas.push(new_schema.clone());
            table_meta.current_schema_id = new_schema_id;

            let current_version = metadata_file_version_from_path(&latest_meta).unwrap_or(0);
            let next_version = current_version + 1;

            // Same retry + write pattern as alter_table_add_columns
            let existing_for_next = metadata_files_for_version(&store_ctx, next_version).await?;
            if !existing_for_next.is_empty() {
                if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                    return Err(alter_table_properties_conflict_error());
                }
                continue;
            }

            let timestamp_ms = monotonic_timestamp_ms();
            table_meta.last_updated_ms = timestamp_ms;
            table_meta.metadata_log.push(MetadataLog {
                timestamp_ms,
                metadata_file: latest_meta.clone(),
            });

            let new_meta_bytes = table_meta
                .to_json()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
            let new_meta_rel = format!("metadata/v{next_version}{file_extension}");
            let new_meta_bytes = encode_metadata_file(&new_meta_rel, &new_meta_bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let new_meta_path = object_store::path::Path::from(new_meta_rel.as_str());
            let put_opts = object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            };
            let payload = object_store::PutPayload::from(Bytes::from(new_meta_bytes));
            match store_ctx
                .prefixed
                .put_opts(&new_meta_path, payload, put_opts)
                .await
            {
                Ok(_) => {}
                Err(object_store::Error::AlreadyExists { .. }) => {
                    if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                        return Err(alter_table_properties_conflict_error());
                    }
                    continue;
                }
                Err(e) => return Err(DataFusionError::External(Box::new(e))),
            }

            let hint_path = object_store::path::Path::from("metadata/version-hint.text");
            store_ctx
                .prefixed
                .put(
                    &hint_path,
                    object_store::PutPayload::from(Bytes::from(next_version.to_string())),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            return Ok(());
        }
    }

    // TODO: Implement row-level DELETE/UPDATE/MERGE for this format. Expanded
    // inputs should consume Sail row intent tags to decide which rows rewrite
    // data files and which rows produce low-level delete artifacts, then strip
    // all internal metadata before writing user data.
}

/// Create an Iceberg table provider for reading.
pub async fn create_iceberg_provider(
    ctx: &dyn Session,
    table_url: Url,
    options: IcebergReadOptions,
) -> Result<Arc<dyn TableProvider>> {
    Ok(create_iceberg_provider_concrete(ctx, table_url, options, None, false).await?)
}

pub async fn create_iceberg_provider_concrete(
    ctx: &dyn Session,
    table_url: Url,
    options: IcebergReadOptions,
    metadata_location: Option<String>,
    catalog_managed_table: bool,
) -> Result<Arc<IcebergTableProvider>> {
    let metadata_location = catalog_managed_table.then_some(metadata_location).flatten();
    let table = Table::load_with_metadata_location(ctx, table_url, metadata_location).await?;
    let provider = table.to_provider(&options)?;
    Ok(Arc::new(provider))
}

async fn build_iceberg_provider(
    ctx: &dyn Session,
    info: SourceInfo,
) -> Result<Arc<IcebergTableProvider>> {
    let SourceInfo {
        paths,
        lakehouse_table,
        schema: _,
        constraints: _,
        partition_by: _,
        bucket_by: _,
        sort_order: _,
        options,
        read_case_sensitive: _,
    } = info;

    validate_iceberg_read_lakehouse_context(lakehouse_table.as_ref())?;
    let table_url = IcebergTableFormat::parse_table_url(paths).await?;
    let metadata_location = metadata_location_from_options(&options);
    let catalog_managed_table = catalog_managed_iceberg_from_options(&options);
    let iceberg_options = IcebergReadOptions::resolve(ctx, options)?;
    create_iceberg_provider_concrete(
        ctx,
        table_url,
        iceberg_options,
        metadata_location,
        catalog_managed_table,
    )
    .await
}

fn validate_iceberg_read_lakehouse_context(
    lakehouse_table: Option<&LakehouseExecutionContext>,
) -> Result<()> {
    let Some(context) = lakehouse_table else {
        return Ok(());
    };
    validate_iceberg_lakehouse_storage_access(Some(context))?;
    if context.scan == ScanAuthority::IcebergRestServerSide {
        // TODO: Implement Iceberg REST server-side scan planning sessions before
        // allowing server-mode tables to fall through to client-side storage access.
        return not_impl_err!(
            "Iceberg REST catalog table {} requires server-side scan planning, which is not implemented yet",
            context.catalog_table().join(".")
        );
    }
    Ok(())
}

fn validate_iceberg_lakehouse_storage_access(
    lakehouse_table: Option<&LakehouseExecutionContext>,
) -> Result<()> {
    let Some(context) = lakehouse_table else {
        return Ok(());
    };
    if context
        .rest_session
        .as_ref()
        .is_some_and(|session| session.remote_signing_enabled)
    {
        // TODO: Wire REST remote signing into Iceberg FileIO/object-store access.
        warn!(
            "Iceberg REST catalog table {} advertises remote signing, which is not implemented yet",
            context.catalog_table().join(".")
        );
    }
    if context
        .rest_session
        .as_ref()
        .is_some_and(|session| session.storage_credential_count > 0)
    {
        // TODO: Apply REST vended credentials to operation-scoped storage access.
        warn!(
            "Iceberg REST catalog table {} advertises vended storage credentials, which is not implemented yet",
            context.catalog_table().join(".")
        );
    }
    Ok(())
}

/// Load metadata and pick snapshot per options (precedence: snapshot_id > ref > timestamp > current).
#[expect(dead_code)]
pub(crate) async fn load_table_metadata_with_options(
    ctx: &dyn Session,
    table_url: &Url,
    options: IcebergReadOptions,
) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
    log::trace!(
        "Loading table metadata (with options) from: {}, options: {:?}",
        table_url,
        options
    );
    let table = Table::load(ctx, table_url.clone()).await?;
    table.scan_state(&options)
}

impl IcebergTableFormat {
    pub async fn parse_table_url(paths: Vec<String>) -> Result<Url> {
        if paths.len() != 1 {
            return plan_err!(
                "Iceberg table requires exactly one path, got {}",
                paths.len()
            );
        }

        let path = &paths[0];
        let mut table_url = match parse_absolute_url(path) {
            Some(url) => url,
            _ => file_url_from_absolute_path(path).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Iceberg table location must be an absolute path or URL: {path}"
                ))
            })?,
        };

        if !table_url.path().ends_with('/') {
            table_url.set_path(&format!("{}/", table_url.path()));
        }
        Ok(table_url)
    }

    fn partition_columns_from_metadata(table: &Table) -> Result<Vec<CatalogPartitionField>> {
        partition_columns_from_table_metadata(table.metadata())
    }
}

fn partition_columns_from_table_metadata(
    metadata: &TableMetadata,
) -> Result<Vec<CatalogPartitionField>> {
    let spec = match metadata.default_partition_spec() {
        Some(spec) => spec,
        None => return Ok(vec![]),
    };
    if spec.is_unpartitioned() {
        return Ok(vec![]);
    }

    let schema = metadata.current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
    })?;

    let mut columns = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let col_name = schema
            .field_by_id(field.source_id)
            .map(|f| f.name.clone())
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Partition field references unknown source column id {}",
                    field.source_id
                ))
            })?;
        columns.push(
            catalog_partition_field_from_iceberg(col_name, field.transform)
                .map_err(DataFusionError::Plan)?,
        );
    }

    Ok(columns)
}

fn create_table_arrow_schema(columns: Vec<TableFormatCreateTableColumn>) -> Result<ArrowSchema> {
    let fields = columns
        .into_iter()
        .map(
            |TableFormatCreateTableColumn {
                 name,
                 data_type,
                 nullable,
                 comment,
                 default,
                 generated_always_as,
                 identity,
             }| {
                if default.is_some() {
                    return not_impl_err!("column DEFAULT in Iceberg CREATE TABLE");
                }
                if generated_always_as.is_some() {
                    return not_impl_err!("generated columns in Iceberg CREATE TABLE");
                }
                if identity.is_some() {
                    return not_impl_err!("identity columns in Iceberg CREATE TABLE");
                }
                let mut field = ArrowField::new(name, data_type, nullable);
                if let Some(comment) = comment {
                    field = field.with_metadata(std::collections::HashMap::from([(
                        ICEBERG_ARROW_FIELD_DOC_KEY.to_string(),
                        comment,
                    )]));
                }
                field = with_variant_extension_if_marked_storage(field);
                Ok(field)
            },
        )
        .collect::<Result<Vec<_>>>()?;
    Ok(ArrowSchema::new(fields))
}

fn create_table_partition_spec(
    iceberg_schema: &Schema,
    partition_by: &[CatalogPartitionField],
) -> Result<PartitionSpec> {
    let mut partition_spec_builder = PartitionSpec::builder();
    for field in partition_by {
        let source_id = iceberg_schema
            .field_id_by_name(&field.column)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Partition column mismatch: column '{}' not found in schema",
                    format_partition_expr(field)
                ))
            })?;
        partition_spec_builder = partition_spec_builder.add_field(
            source_id,
            partition_field_name(field),
            iceberg_transform_from_partition_field(field),
        );
    }
    Ok(partition_spec_builder.build())
}

fn iceberg_schema_with_id(schema: &Schema, schema_id: i32) -> Result<Schema> {
    Schema::builder()
        .with_schema_id(schema_id)
        .with_fields(schema.fields().iter().cloned())
        .build()
        .map_err(|e| DataFusionError::Plan(format!("Failed to assign Iceberg schema id: {e}")))
}

fn next_schema_id(metadata: &TableMetadata) -> i32 {
    metadata
        .schemas
        .iter()
        .map(|schema| schema.schema_id())
        .max()
        .unwrap_or(0)
        + 1
}

fn next_partition_spec_id(metadata: &TableMetadata) -> i32 {
    metadata
        .partition_specs
        .iter()
        .map(|spec| spec.spec_id())
        .max()
        .unwrap_or(0)
        + 1
}

fn file_url_from_absolute_path(path: &str) -> Option<Url> {
    if Path::new(path).is_absolute() {
        return Url::from_file_path(path).ok();
    }
    windows_drive_path_to_file_url(path)
}

fn windows_drive_path_to_file_url(path: &str) -> Option<Url> {
    let bytes = path.as_bytes();
    if bytes.len() < 3
        || !bytes[0].is_ascii_alphabetic()
        || bytes[1] != b':'
        || !matches!(bytes[2], b'/' | b'\\')
    {
        return None;
    }

    let path = path.replace('\\', "/");
    Url::parse(&format!("file:///{path}")).ok()
}

pub(crate) fn table_metadata_location(table_url: &Url, metadata_file: &str) -> Result<String> {
    if parse_absolute_url(metadata_file).is_some() {
        return Ok(metadata_file.to_string());
    }

    let relative_metadata_file = relative_metadata_file(table_url, metadata_file)?;
    Ok(table_url
        .join(&relative_metadata_file)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .to_string())
}

fn relative_metadata_file(table_url: &Url, metadata_file: &str) -> Result<String> {
    let base_path = url_to_object_path(table_url)?.to_string();
    let metadata_file = metadata_file.trim_start_matches('/');

    if let Some(relative) = strip_path_prefix(metadata_file, &base_path) {
        return Ok(relative.to_string());
    }
    if table_url.scheme() == "file" {
        if let Some(base_without_drive) = strip_windows_drive_prefix(&base_path) {
            if let Some(relative) = strip_path_prefix(metadata_file, base_without_drive) {
                return Ok(relative.to_string());
            }
        }
    }
    Ok(metadata_file.to_string())
}

fn strip_path_prefix<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        return None;
    }
    path.strip_prefix(prefix)?.strip_prefix('/')
}

fn strip_windows_drive_prefix(path: &str) -> Option<&str> {
    let bytes = path.as_bytes();
    if bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' && bytes[2] == b'/' {
        Some(&path[3..])
    } else {
        None
    }
}

fn iceberg_table_properties_from_catalog_create(
    properties: Vec<(String, String)>,
) -> Result<Vec<(String, String)>> {
    let catalog_table_option = sail_common_datafusion::datasource::CATALOG_TABLE_OPTION;
    if properties
        .iter()
        .any(|(key, _)| key.eq_ignore_ascii_case(catalog_table_option))
    {
        return plan_err!(
            "Iceberg table property `{catalog_table_option}` is reserved for internal use"
        );
    }
    Ok(properties
        .into_iter()
        .filter(|(key, _)| !key.starts_with("option."))
        .collect())
}

pub(crate) fn metadata_location_from_properties(properties: &[(String, String)]) -> Option<String> {
    metadata_location_value(
        properties
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .map(ToString::to_string)
}

pub fn metadata_location_from_options(options: &[OptionLayer]) -> Option<String> {
    options.iter().rev().find_map(|layer| match layer {
        OptionLayer::TablePropertyList { items } | OptionLayer::OptionList { items } => {
            metadata_location_from_properties(items)
        }
        _ => None,
    })
}

pub(crate) fn catalog_managed_iceberg_from_properties(properties: &[(String, String)]) -> bool {
    properties.iter().any(|(key, value)| {
        let key = key.trim();
        is_iceberg_table_marker(key, value.trim()) || key.starts_with("metadata.")
    })
}

pub fn catalog_managed_iceberg_from_options(options: &[OptionLayer]) -> bool {
    options.iter().any(|layer| match layer {
        OptionLayer::TablePropertyList { items } | OptionLayer::OptionList { items } => {
            catalog_managed_iceberg_from_properties(items)
        }
        _ => false,
    })
}

#[expect(clippy::type_complexity)]
pub fn split_iceberg_write_options_and_table_properties(
    options: Vec<OptionLayer>,
) -> Result<(Vec<OptionLayer>, Vec<(String, String)>)> {
    let catalog_table_option = sail_common_datafusion::datasource::CATALOG_TABLE_OPTION;
    let mut table_properties = Vec::new();
    let clean_options = options
        .into_iter()
        .map(|layer| match layer {
            OptionLayer::OptionList { items } => {
                if items
                    .iter()
                    .any(|(key, _)| key.eq_ignore_ascii_case(catalog_table_option))
                {
                    return plan_err!(
                        "Iceberg write option `{catalog_table_option}` is reserved for internal use"
                    );
                }
                Ok(Some(OptionLayer::OptionList { items }))
            }
            OptionLayer::TablePropertyList { items } => {
                if items
                    .iter()
                    .any(|(key, _)| key.eq_ignore_ascii_case(catalog_table_option))
                {
                    return plan_err!(
                        "Iceberg table property `{catalog_table_option}` is reserved for internal use"
                    );
                }
                // Catalog-encoded OPTIONS are stored as `option.*` table properties.
                // Keep them for option resolution, but do not commit them to Iceberg metadata.
                table_properties.extend(
                    items
                        .iter()
                        .filter(|(key, _)| !key.starts_with("option."))
                        .cloned(),
                );
                Ok(Some(OptionLayer::TablePropertyList { items }))
            }
            other => Ok(Some(other)),
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok((clean_options, table_properties))
}

fn alter_table_properties_conflict_error() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg ALTER TABLE SET/UNSET TBLPROPERTIES failed after {MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}

/// Extract partition column equality conditions from an expression.
/// Only supports equality predicates on partition columns (e.g., `col = 'value'`).
/// Returns a JSON-serializable vector of (column, value) pairs.
pub(crate) fn extract_partition_predicate_from_expr(expr: &Expr) -> Option<Vec<(String, String)>> {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            use datafusion_expr::Operator;
            match binary_expr.op {
                Operator::Eq => {
                    let left = binary_expr.left.as_ref();
                    let right = binary_expr.right.as_ref();
                    match (left, right) {
                        (Expr::Column(col), Expr::Literal(scalar, _)) => {
                            Some(vec![(col.name.clone(), scalar.to_string())])
                        }
                        (Expr::Literal(scalar, _), Expr::Column(col)) => {
                            Some(vec![(col.name.clone(), scalar.to_string())])
                        }
                        _ => None,
                    }
                }
                Operator::And => {
                    let left = extract_partition_predicate_from_expr(binary_expr.left.as_ref());
                    let right = extract_partition_predicate_from_expr(binary_expr.right.as_ref());
                    match (left, right) {
                        (Some(mut l), Some(r)) => {
                            l.extend(r);
                            Some(l)
                        }
                        (Some(l), None) => Some(l),
                        (None, Some(r)) => Some(r),
                        (None, None) => None,
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Convert a [`BucketBy`] into its partition fields representation.
/// Each column becomes a `CatalogPartitionField` with a `Bucket(n)` transform.
pub(crate) fn partition_fields_from_bucket_by(bucket_by: BucketBy) -> Vec<CatalogPartitionField> {
    let num_buckets = bucket_by.num_buckets as u32;
    bucket_by
        .columns
        .into_iter()
        .map(|col| CatalogPartitionField {
            column: col,
            transform: Some(PartitionTransform::Bucket(num_buckets)),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use sail_common_datafusion::catalog::{
        CatalogProviderId, CatalogTableIdentity, CommitAuthority, IcebergRestTableSessionRef,
        LakehouseAuthority, LakehouseFormat, LakehouseOperation, MetadataPointerAuthority,
        PartitionTransform, TableLifecycle,
    };
    use sail_common_datafusion::datasource::BucketBy;

    use super::*;

    #[test]
    fn split_iceberg_write_options_keeps_catalog_options_out_of_table_properties() -> Result<()> {
        let options = vec![
            OptionLayer::TablePropertyList {
                items: vec![
                    ("option.metadataAsDataRead".to_string(), "true".to_string()),
                    ("write.data.path".to_string(), "custom_data".to_string()),
                    (
                        "write.folder-storage.path".to_string(),
                        "legacy_data".to_string(),
                    ),
                    ("custom.key".to_string(), "custom-value".to_string()),
                ],
            },
            OptionLayer::OptionList {
                items: vec![
                    ("mergeSchema".to_string(), "true".to_string()),
                    ("path".to_string(), "/tmp/table".to_string()),
                ],
            },
        ];

        let (clean_options, table_properties) =
            split_iceberg_write_options_and_table_properties(options)?;

        assert_eq!(
            table_properties,
            vec![
                ("write.data.path".to_string(), "custom_data".to_string()),
                (
                    "write.folder-storage.path".to_string(),
                    "legacy_data".to_string(),
                ),
                ("custom.key".to_string(), "custom-value".to_string()),
            ]
        );
        let ctx = datafusion::execution::context::SessionContext::default();
        let state = ctx.state();
        #[expect(clippy::unwrap_used)]
        let iceberg_options = IcebergWriteOptions::resolve(&state, clean_options).unwrap();
        assert!(iceberg_options.merge_schema);
        assert_eq!(
            iceberg_options.write_data_path.as_deref(),
            Some("custom_data")
        );
        assert_eq!(
            iceberg_options.write_folder_storage_path.as_deref(),
            Some("legacy_data")
        );
        Ok(())
    }

    #[test]
    fn catalog_table_option_is_reserved_for_iceberg_options() {
        let options = vec![OptionLayer::OptionList {
            items: vec![(
                sail_common_datafusion::datasource::CATALOG_TABLE_OPTION.to_string(),
                r#"["catalog","schema","table"]"#.to_string(),
            )],
        }];

        let result = split_iceberg_write_options_and_table_properties(options);
        assert!(matches!(
            &result,
            Err(err) if format!("{err}").contains("reserved for internal use")
        ));
    }

    #[test]
    fn catalog_table_option_is_reserved_for_iceberg_table_properties() {
        let options = vec![OptionLayer::TablePropertyList {
            items: vec![(
                sail_common_datafusion::datasource::CATALOG_TABLE_OPTION.to_string(),
                r#"["catalog","schema","table"]"#.to_string(),
            )],
        }];

        let result = split_iceberg_write_options_and_table_properties(options);
        assert!(matches!(
            &result,
            Err(err) if format!("{err}").contains("reserved for internal use")
        ));
    }

    #[test]
    fn catalog_managed_iceberg_detection_requires_marker_or_metadata_summary() {
        assert!(!catalog_managed_iceberg_from_properties(&[(
            "metadata-location".to_string(),
            "file:///tmp/table/metadata/v1.metadata.json".to_string(),
        )]));
        assert!(catalog_managed_iceberg_from_properties(&[(
            "table_type".to_string(),
            "ICEBERG".to_string(),
        )]));
        assert!(catalog_managed_iceberg_from_properties(&[(
            "metadata.table-uuid".to_string(),
            "9f7c2fc5-2e7d-4a6a-b3f9-0f6a47a3522c".to_string(),
        )]));
    }

    #[test]
    fn parse_table_url_accepts_windows_drive_paths() -> Result<()> {
        let url = futures::executor::block_on(IcebergTableFormat::parse_table_url(vec![
            r"C:\Users\runneradmin\AppData\Local\Temp\iceberg_table".to_string(),
        ]))?;
        assert_eq!(
            url.as_str(),
            "file:///C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/"
        );
        Ok(())
    }

    #[test]
    fn parse_table_url_preserves_windows_file_uri_drive() -> Result<()> {
        let url = futures::executor::block_on(IcebergTableFormat::parse_table_url(vec![
            "file:///C:/Users/runneradmin/AppData/Local/Temp/iceberg_table".to_string(),
        ]))?;
        assert_eq!(
            url.as_str(),
            "file:///C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/"
        );
        Ok(())
    }

    #[test]
    fn table_metadata_location_preserves_file_uri_drive() -> Result<()> {
        let table_url =
            Url::parse("file:///C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/")
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

        assert_eq!(
            table_metadata_location(&table_url, "metadata/v1.metadata.json")?,
            "file:///C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/metadata/v1.metadata.json"
        );
        assert_eq!(
            table_metadata_location(
                &table_url,
                "C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/metadata/v1.metadata.json",
            )?,
            "file:///C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/metadata/v1.metadata.json"
        );
        assert_eq!(
            table_metadata_location(
                &table_url,
                "Users/runneradmin/AppData/Local/Temp/iceberg_table/metadata/v1.metadata.json",
            )?,
            "file:///C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/metadata/v1.metadata.json"
        );
        Ok(())
    }

    #[test]
    fn read_rejects_required_rest_server_side_scan_planning() {
        let context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("rest".to_string()),
            vec!["rest".to_string(), "db".to_string(), "tbl".to_string()],
            CatalogTableIdentity {
                table_id: Some("12345678-1234-1234-1234-123456789012".to_string()),
                table_uri: Some("s3://bucket/table".to_string()),
            },
            LakehouseOperation::Read,
            LakehouseFormat::Iceberg,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::External,
                pointer: MetadataPointerAuthority::IcebergRest,
                commit: CommitAuthority::IcebergRestCommit,
            },
            ScanAuthority::IcebergRestServerSide,
        );

        let result = validate_iceberg_read_lakehouse_context(Some(&context));
        assert!(matches!(
            &result,
            Err(err) if format!("{err}").contains("requires server-side scan planning")
        ));
    }

    #[test]
    fn storage_access_allows_required_rest_remote_signing() {
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("rest".to_string()),
            vec!["rest".to_string(), "db".to_string(), "tbl".to_string()],
            CatalogTableIdentity {
                table_id: Some("12345678-1234-1234-1234-123456789012".to_string()),
                table_uri: Some("s3://bucket/table".to_string()),
            },
            LakehouseOperation::Read,
            LakehouseFormat::Iceberg,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::External,
                pointer: MetadataPointerAuthority::IcebergRest,
                commit: CommitAuthority::IcebergRestCommit,
            },
            ScanAuthority::ClientTableFormat,
        );
        context.rest_session = Some(IcebergRestTableSessionRef {
            fingerprint: "rest-session".to_string(),
            scan_planning_mode: Some("client".to_string()),
            storage_credential_count: 0,
            remote_signing_enabled: true,
        });

        let result = validate_iceberg_lakehouse_storage_access(Some(&context));
        assert!(result.is_ok());
    }

    #[test]
    fn storage_access_allows_required_rest_vended_credentials() {
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("rest".to_string()),
            vec!["rest".to_string(), "db".to_string(), "tbl".to_string()],
            CatalogTableIdentity {
                table_id: Some("12345678-1234-1234-1234-123456789012".to_string()),
                table_uri: Some("s3://bucket/table".to_string()),
            },
            LakehouseOperation::Read,
            LakehouseFormat::Iceberg,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::External,
                pointer: MetadataPointerAuthority::IcebergRest,
                commit: CommitAuthority::IcebergRestCommit,
            },
            ScanAuthority::ClientTableFormat,
        );
        context.rest_session = Some(IcebergRestTableSessionRef {
            fingerprint: "rest-session".to_string(),
            scan_planning_mode: Some("client".to_string()),
            storage_credential_count: 1,
            remote_signing_enabled: false,
        });

        let result = validate_iceberg_lakehouse_storage_access(Some(&context));
        assert!(result.is_ok());
    }

    #[test]
    fn partition_fields_from_bucket_by_single_column() {
        let bucket_by = BucketBy {
            columns: vec!["user_id".to_string()],
            num_buckets: 16,
        };
        let fields = partition_fields_from_bucket_by(bucket_by);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].column, "user_id");
        assert_eq!(fields[0].transform, Some(PartitionTransform::Bucket(16)));
    }

    #[test]
    fn partition_fields_from_bucket_by_multiple_columns() {
        let bucket_by = BucketBy {
            columns: vec!["user_id".to_string(), "org_id".to_string()],
            num_buckets: 8,
        };
        let fields = partition_fields_from_bucket_by(bucket_by);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].column, "user_id");
        assert_eq!(fields[0].transform, Some(PartitionTransform::Bucket(8)));
        assert_eq!(fields[1].column, "org_id");
        assert_eq!(fields[1].transform, Some(PartitionTransform::Bucket(8)));
    }

    #[test]
    fn partition_fields_from_bucket_by_empty_columns() {
        let bucket_by = BucketBy {
            columns: vec![],
            num_buckets: 4,
        };
        let fields = partition_fields_from_bucket_by(bucket_by);
        assert!(fields.is_empty());
    }

    #[test]
    fn partition_fields_from_bucket_by_merges_with_existing() {
        let bucket_by = BucketBy {
            columns: vec!["user_id".to_string()],
            num_buckets: 16,
        };
        let existing = vec![CatalogPartitionField {
            column: "event_date".to_string(),
            transform: Some(PartitionTransform::Day),
        }];
        let bucket_fields = partition_fields_from_bucket_by(bucket_by);
        let merged: Vec<_> = existing.into_iter().chain(bucket_fields).collect();
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].column, "event_date");
        assert_eq!(merged[0].transform, Some(PartitionTransform::Day));
        assert_eq!(merged[1].column, "user_id");
        assert_eq!(merged[1].transform, Some(PartitionTransform::Bucket(16)));
    }

    #[test]
    fn extract_partition_predicate_simple_eq() {
        use datafusion_expr::{col, lit};
        // col("event_date") = lit("2024-01-15")
        let expr = col("event_date").eq(lit("2024-01-15"));
        let result = extract_partition_predicate_from_expr(&expr);
        assert!(result.is_some());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, "event_date");
    }

    #[test]
    fn extract_partition_predicate_and_combined() {
        use datafusion_expr::{col, lit};
        // col("year") = lit("2024") AND col("month") = lit("01")
        let expr = col("year").eq(lit("2024")).and(col("month").eq(lit("01")));
        let result = extract_partition_predicate_from_expr(&expr);
        assert!(result.is_some());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().any(|(k, _)| k == "year"));
        assert!(pairs.iter().any(|(k, _)| k == "month"));
    }

    #[test]
    fn extract_partition_predicate_no_match() {
        use datafusion_expr::{col, lit};
        // col("value") > lit(100)  — not an equality predicate
        let expr = col("value").gt(lit(100i64));
        let result = extract_partition_predicate_from_expr(&expr);
        assert!(result.is_none());
    }

    #[test]
    fn extract_partition_predicate_literal_on_left() {
        use datafusion_expr::{col, lit};
        // lit("2024-01-15") = col("event_date")  — reversed order
        let expr = lit("2024-01-15").eq(col("event_date"));
        let result = extract_partition_predicate_from_expr(&expr);
        assert!(result.is_some());
        let pairs = result.unwrap();
        assert_eq!(pairs[0].0, "event_date");
    }

    #[test]
    fn extract_partition_predicate_complex_expr() {
        use datafusion_expr::{col, lit};
        // (col("a") = lit("1") AND col("b") = lit("2")) OR col("c") = lit("3")
        // Only the AND branch should be extracted; OR is not supported
        let expr = col("a")
            .eq(lit("1"))
            .and(col("b").eq(lit("2")))
            .or(col("c").eq(lit("3")));
        let result = extract_partition_predicate_from_expr(&expr);
        // OR is not supported, so should return None for the top-level
        assert!(result.is_none());
    }
}

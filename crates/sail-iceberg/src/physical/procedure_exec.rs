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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, not_impl_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use sail_common_datafusion::catalog::{CommitAuthority, LakehouseExecutionContext};
use sail_common_datafusion::lakesource::LakeSourceProcedureOperation;
use sail_logical_plan::procedure::ProcedureOptions;

use crate::catalog_support::commit::{
    CatalogCommitOutcome, IcebergCatalogCommitCoordinator, catalog_requirements,
};
use crate::io::StoreContext;
use crate::lake_source::{IcebergLakeSource, resolve_iceberg_metadata_location};
use crate::operations::expire_snapshots_gc::expire_files_gc;
use crate::operations::procedure::{
    CallProcedureOutput, apply_procedure_updates, compute_procedure_output,
    compute_procedure_updates, procedure_requirements, validate_procedure_requirements,
};
use crate::spec::TableMetadata;
use crate::table::find_latest_metadata_file;
use crate::table::metadata_loader::load_metadata_file_bytes;

/// The maximum number of catalog-commit retries for a `CALL` procedure.
const MAX_PROCEDURE_COMMIT_RETRIES: usize = 5;

/// Loads the table metadata for a procedure, preferring a catalog pointer when
/// one is available and falling back to storage discovery.
async fn load_procedure_metadata(
    object_store: &Arc<dyn object_store::ObjectStore>,
    table_url: &url::Url,
    metadata_location: Option<String>,
) -> Result<(TableMetadata, String)> {
    let latest = match metadata_location {
        Some(location) => {
            let url =
                url::Url::parse(&location).map_err(|e| DataFusionError::External(Box::new(e)))?;
            crate::utils::url_to_object_path(&url)?.to_string()
        }
        None => find_latest_metadata_file(object_store, table_url).await?,
    };
    let bytes = load_metadata_file_bytes(object_store, &latest).await?;
    let metadata =
        TableMetadata::from_json(&bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok((metadata, latest))
}

/// Executes an Iceberg `CALL <catalog>.system.<procedure>(...)`.
///
/// This node has a single output partition and no children. It commits
/// procedure metadata updates through the catalog coordinator (for
/// catalog-managed tables) or the filesystem metadata path, using the
/// [`TaskContext`] for both the object store and the catalog manager.
#[derive(Debug, Clone)]
pub struct IcebergProcedureExec {
    options: ProcedureOptions,
    commit_authority: CommitAuthority,
    catalog_table: Vec<String>,
    lakehouse_table: Option<LakehouseExecutionContext>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl IcebergProcedureExec {
    pub fn new(
        options: ProcedureOptions,
        commit_authority: CommitAuthority,
        catalog_table: Vec<String>,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Self {
        let schema = CallProcedureOutput::schema_for(&options.operation);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            options,
            commit_authority,
            catalog_table,
            lakehouse_table,
            schema,
            properties,
        }
    }

    pub fn options(&self) -> &ProcedureOptions {
        &self.options
    }

    pub fn commit_authority(&self) -> CommitAuthority {
        self.commit_authority
    }

    pub fn catalog_table(&self) -> &[String] {
        &self.catalog_table
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.lakehouse_table.as_ref()
    }
}

impl DisplayAs for IcebergProcedureExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "IcebergProcedureExec: {} — {}",
            self.options.procedure_name.join("."),
            self.options.operation.label()
        )
    }
}

impl ExecutionPlan for IcebergProcedureExec {
    fn name(&self) -> &'static str {
        "IcebergProcedureExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            plan_err!("IcebergProcedureExec does not accept children")
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            plan_err!("IcebergProcedureExec does not accept children")
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("IcebergProcedureExec only supports partition 0");
        }
        let exec = Arc::new(self.clone());
        let schema = self.schema.clone();
        let stream = futures::stream::once(async move { exec.run(&context).await });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl IcebergProcedureExec {
    async fn run(
        &self,
        context: &Arc<TaskContext>,
    ) -> Result<datafusion::arrow::array::RecordBatch> {
        let runtime_env = context.runtime_env();
        let path = self.options.target_path.as_deref().ok_or_else(|| {
            DataFusionError::Plan(
                "CALL procedure requires a resolved target table path".to_string(),
            )
        })?;
        let table_url = IcebergLakeSource::parse_table_url(vec![path.to_string()]).await?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;

        // Resolve the metadata location the same way the write path does, so a
        // catalog pointer (metadata-location option) takes precedence over
        // storage discovery.
        let metadata_location =
            crate::lake_source::metadata_location_from_options(&self.options.target_options);
        let catalog_managed =
            crate::lake_source::catalog_managed_iceberg_from_options(&self.options.target_options);
        let metadata_location = resolve_iceberg_metadata_location(
            self.options.target_lakehouse_table.as_ref(),
            metadata_location,
            catalog_managed,
        )?;

        let (pre_commit, latest_meta) =
            load_procedure_metadata(&object_store, &table_url, metadata_location).await?;

        let updates = compute_procedure_updates(&self.options.operation, &pre_commit)?;
        let output = compute_procedure_output(&self.options.operation, &pre_commit)?;

        // Nothing to commit (e.g. expire_snapshots with an empty expire set):
        // return the output without writing a metadata version.
        if updates.is_empty() {
            return self
                .finish(&store_ctx, &object_store, &table_url, &pre_commit, output)
                .await;
        }

        if self.commit_authority == CommitAuthority::Filesystem {
            let requirements = procedure_requirements(&pre_commit);
            crate::lake_source::retry_metadata_commit(
                &store_ctx,
                &object_store,
                &table_url,
                latest_meta,
                true,
                move |table_meta| {
                    validate_procedure_requirements(table_meta, &requirements)?;
                    apply_procedure_updates(table_meta, &updates)?;
                    Ok(())
                },
            )
            .await?;
            self.finish(&store_ctx, &object_store, &table_url, &pre_commit, output)
                .await
        } else {
            self.commit_via_catalog(context, &table_url, &pre_commit)
                .await
        }
    }

    /// Commits a catalog-managed procedure, recomputing updates against fresh
    /// metadata and retrying on concurrent-commit conflicts.
    async fn commit_via_catalog(
        &self,
        context: &Arc<TaskContext>,
        table_url: &url::Url,
        pre_commit: &TableMetadata,
    ) -> Result<datafusion::arrow::array::RecordBatch> {
        let Some(lakehouse_table) = self.lakehouse_table.as_ref() else {
            return not_impl_err!(
                "missing lakehouse context for catalog-managed CALL procedure: {}",
                self.catalog_table.join(".")
            );
        };
        // The coordinator reaches `CatalogManager` through `TaskContext`, which
        // is a `SessionExtensionAccessor`; a `&dyn Session` is not required.
        let coordinator =
            IcebergCatalogCommitCoordinator::new(context.as_ref(), &self.catalog_table);

        let mut attempt = 0;
        loop {
            attempt += 1;
            let updates = compute_procedure_updates(&self.options.operation, pre_commit)?;
            let output = compute_procedure_output(&self.options.operation, pre_commit)?;
            let requirements =
                catalog_requirements(pre_commit, &procedure_requirements(pre_commit), &[]);
            let outcome = coordinator
                .commit(lakehouse_table, requirements, updates)
                .await?;
            match outcome {
                CatalogCommitOutcome::Committed(_) => {
                    let runtime_env = context.runtime_env();
                    let object_store = runtime_env
                        .object_store_registry
                        .get_store(table_url)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let store_ctx = StoreContext::new(object_store.clone(), table_url)?;
                    return self
                        .finish(&store_ctx, &object_store, table_url, pre_commit, output)
                        .await;
                }
                CatalogCommitOutcome::Conflict => {
                    log::warn!(
                        "CALL procedure commit conflict for {} on attempt {attempt}; retrying",
                        self.catalog_table.join(".")
                    );
                    if attempt >= MAX_PROCEDURE_COMMIT_RETRIES {
                        return Err(DataFusionError::Execution(format!(
                            "CALL procedure commit failed after {MAX_PROCEDURE_COMMIT_RETRIES} retries due to concurrent metadata updates: {}",
                            self.catalog_table.join(".")
                        )));
                    }
                }
                CatalogCommitOutcome::NotSupported => {
                    return not_impl_err!(
                        "CALL procedures are not supported for catalog-managed Iceberg table: {}",
                        self.catalog_table.join(".")
                    );
                }
            }
        }
    }

    async fn finish(
        &self,
        store_ctx: &StoreContext,
        object_store: &Arc<dyn object_store::ObjectStore>,
        table_url: &url::Url,
        pre_commit: &TableMetadata,
        output: CallProcedureOutput,
    ) -> Result<datafusion::arrow::array::RecordBatch> {
        match &self.options.operation {
            LakeSourceProcedureOperation::ExpireSnapshots { .. } => {
                let post_meta = find_latest_metadata_file(object_store, table_url).await?;
                let bytes = load_metadata_file_bytes(object_store, &post_meta).await?;
                let post_commit = TableMetadata::from_json(&bytes)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let counts = expire_files_gc(store_ctx, pre_commit, &post_commit).await?;
                CallProcedureOutput::ExpireSnapshots {
                    deleted_data_files_count: counts.data_files as i64,
                    deleted_position_delete_files_count: counts.position_delete_files as i64,
                    deleted_equality_delete_files_count: counts.equality_delete_files as i64,
                    deleted_manifest_files_count: counts.manifest_files as i64,
                    deleted_manifest_lists_count: counts.manifest_lists as i64,
                    deleted_statistics_files_count: counts.statistics_files as i64,
                }
                .to_record_batch()
            }
            _ => output.to_record_batch(),
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::execution::context::TaskContext;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use url::Url;

    use super::*;
    use crate::io::StoreContext;
    use crate::operations::bootstrap::{NewTableMetadataStyle, bootstrap_empty_table_metadata};
    use crate::spec::{NestedField, PartitionSpec, PrimitiveType, Schema as IcebergSchema, Type};

    fn snapshot_ref_options(operation: LakeSourceProcedureOperation) -> ProcedureOptions {
        ProcedureOptions {
            format: "iceberg".to_string(),
            procedure_name: vec!["system".to_string(), operation.label().to_string()],
            operation,
            target_table: Some(vec!["db".to_string(), "t".to_string()]),
            target_path: Some("file:///table/".to_string()),
            target_options: vec![],
            target_lakehouse_table: None,
        }
    }

    #[test]
    fn procedure_exec_outputs_single_partition_and_no_children() {
        let exec = IcebergProcedureExec::new(
            snapshot_ref_options(LakeSourceProcedureOperation::RollbackToSnapshot {
                snapshot_id: 1,
            }),
            CommitAuthority::Filesystem,
            vec![],
            None,
        );
        assert_eq!(exec.children().len(), 0);
        assert_eq!(exec.properties().output_partitioning().partition_count(), 1);
        assert_eq!(exec.schema().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn procedure_output_schema_marks_nullability_like_iceberg() {
        // rollback: both non-null.
        let rollback =
            CallProcedureOutput::schema_for(&LakeSourceProcedureOperation::RollbackToSnapshot {
                snapshot_id: 1,
            });
        assert!(!rollback.field(0).is_nullable());
        assert!(!rollback.field(1).is_nullable());
        // set_current: previous nullable, current non-null.
        let set_current =
            CallProcedureOutput::schema_for(&LakeSourceProcedureOperation::SetCurrentSnapshot {
                snapshot_id: None,
                r#ref: Some("tag".to_string()),
            });
        assert!(set_current.field(0).is_nullable());
        assert!(!set_current.field(1).is_nullable());
        // all fields are Int64 and expire has six columns.
        let expire =
            CallProcedureOutput::schema_for(&LakeSourceProcedureOperation::ExpireSnapshots {
                older_than_ms: None,
                retain_last: None,
                snapshot_ids: vec![],
            });
        assert_eq!(expire.fields().len(), 6);
        assert!(
            expire
                .fields()
                .iter()
                .all(|f| f.data_type() == &DataType::Int64)
        );
    }

    /// Builds an in-memory Iceberg table (empty, one metadata version) and a
    /// `TaskContext` whose object store is registered for `file:///`.
    async fn empty_table_fixture() -> (Arc<dyn ObjectStore>, Arc<TaskContext>) {
        let table_url = Url::parse("file:///table/").expect("table url");
        let memory: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store_ctx = StoreContext::new(Arc::clone(&memory), &table_url).expect("store context");
        let schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields([Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        bootstrap_empty_table_metadata(
            &table_url,
            &store_ctx,
            schema,
            PartitionSpec::builder().with_spec_id(0).build(),
            &[("format-version".to_string(), "2".to_string())],
            NewTableMetadataStyle::Hadoop,
        )
        .await
        .expect("bootstrap");

        let context = SessionContext::new();
        context.runtime_env().register_object_store(
            &Url::parse("file:///").expect("file store url"),
            Arc::clone(&memory),
        );
        (memory, context.task_ctx())
    }

    #[tokio::test]
    async fn set_current_snapshot_on_empty_table_reports_null_previous() {
        let (memory, task_ctx) = empty_table_fixture().await;
        let exec = IcebergProcedureExec::new(
            snapshot_ref_options(LakeSourceProcedureOperation::SetCurrentSnapshot {
                snapshot_id: Some(1),
                r#ref: None,
            }),
            CommitAuthority::Filesystem,
            vec![],
            None,
        );
        // No snapshot with id 1 exists, so this fails before a commit; the point
        // is that the exec wires resolution and rejects cleanly (no metadata
        // write), not that it succeeds on an empty table.
        let error = exec.run(&task_ctx).await.expect_err("missing snapshot");
        assert!(
            error.to_string().contains("does not exist"),
            "unexpected error: {error}"
        );
        // No new metadata version was published by the failed attempt.
        let table_url = Url::parse("file:///table/").expect("table url");
        let latest = find_latest_metadata_file(&memory, &table_url)
            .await
            .expect("metadata");
        assert!(latest.contains("metadata/"));
    }

    #[tokio::test]
    async fn expire_snapshots_with_empty_expire_set_short_circuits() {
        // On a table with no snapshots the expire-set is empty, so the exec
        // returns the (zeroed) counts without writing a metadata version.
        let (memory, task_ctx) = empty_table_fixture().await;
        let exec = IcebergProcedureExec::new(
            snapshot_ref_options(LakeSourceProcedureOperation::ExpireSnapshots {
                older_than_ms: None,
                retain_last: None,
                snapshot_ids: vec![],
            }),
            CommitAuthority::Filesystem,
            vec![],
            None,
        );
        let output = exec.run(&task_ctx).await.expect("expire output");
        assert_eq!(output.num_rows(), 1);
        assert_eq!(output.num_columns(), 6);

        // The short-circuit must not create a new metadata version: exactly one
        // bootstrap metadata file exists under `metadata/`.
        let prefix = object_store::path::Path::from("table/metadata/");
        let objects = memory
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .expect("list metadata");
        let json_files = objects
            .iter()
            .filter(|meta| meta.location.as_ref().ends_with(".json"))
            .count();
        assert_eq!(json_files, 1, "bootstrap metadata must be the only version");
    }
}

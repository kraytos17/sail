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

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_datafusion_err, internal_err, DataFusionError, Result};
use futures::stream::once;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_logical_plan::call_procedure::CallProcedure;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::catalog_support::commit::{
    CatalogCommitOutcome, CatalogTableInfo, IcebergCatalogCommitCoordinator,
    IcebergCatalogCommitMode,
};
use crate::io::StoreContext;
use crate::spec::snapshots::MAIN_BRANCH;
use crate::spec::{SnapshotReference, SnapshotRetention, TableRequirement, TableUpdate};
use crate::table::find_latest_metadata_file;
use crate::table_format::IcebergTableFormat;
use crate::utils::get_object_store_from_context;

/// Driver-side execution plan for `CALL <catalog>.system.<procedure>(...)`.
///
/// The target table metadata is loaded at plan time (`plan_call_procedure`); this exec
/// only performs the commit at execution time through the same dual-path machinery as
/// `IcebergCommitExec`:
/// - catalog-managed tables (e.g. Polaris) via `IcebergCatalogCommitCoordinator::commit`,
/// - filesystem tables via `IcebergTableFormat::retry_metadata_commit`.
///
/// The result is a procedure-specific, spec-shaped single-row batch (see
/// [`CallProcedureOutput`]).
#[derive(Debug, Clone)]
pub struct CallProcedureExec {
    procedure: CallProcedure,
    table_url: Url,
    lakehouse_table: Option<LakehouseExecutionContext>,
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
    output: CallProcedureOutput,
    cache: Arc<PlanProperties>,
}

impl CallProcedureExec {
    pub fn new(
        procedure: CallProcedure,
        table_url: Url,
        lakehouse_table: Option<LakehouseExecutionContext>,
        updates: Vec<TableUpdate>,
        requirements: Vec<TableRequirement>,
        output: CallProcedureOutput,
    ) -> Self {
        let schema = output.schema();
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            procedure,
            table_url,
            lakehouse_table,
            updates,
            requirements,
            output,
            cache,
        }
    }

    pub fn procedure(&self) -> &CallProcedure {
        &self.procedure
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.lakehouse_table.as_ref()
    }

    pub fn updates(&self) -> &[TableUpdate] {
        &self.updates
    }

    pub fn requirements(&self) -> &[TableRequirement] {
        &self.requirements
    }

    pub fn output(&self) -> &CallProcedureOutput {
        &self.output
    }

    /// Applies the procedure's updates at execution time, returning the output batch.
    async fn execute_call(&self, context: Arc<TaskContext>) -> Result<RecordBatch> {
        // Resolve the commit authority: catalog-managed (Polaris) vs filesystem.
        let catalog_table = self
            .lakehouse_table
            .as_ref()
            .map(|c| c.catalog_table().to_vec());
        let catalog_table_info = match &catalog_table {
            Some(table) => {
                IcebergCatalogCommitCoordinator::load_table_info(context.as_ref(), table).await?
            }
            None => CatalogTableInfo::default(),
        };
        let commit_mode = IcebergCatalogCommitMode::resolve(
            self.lakehouse_table.as_ref(),
            &catalog_table_info,
            &[],
        );

        let mut updates = self.updates.clone();
        let mut requirements = self.requirements.clone();

        match commit_mode {
            IcebergCatalogCommitMode::Filesystem => {
                let object_store = get_object_store_from_context(&context, &self.table_url)?;
                let store_ctx = StoreContext::new(object_store.clone(), &self.table_url)?;
                let initial_latest_meta =
                    find_latest_metadata_file(&object_store, &self.table_url).await?;
                let updates_for_commit = std::mem::take(&mut updates);
                IcebergTableFormat::retry_metadata_commit(
                    object_store,
                    &store_ctx,
                    &self.table_url,
                    initial_latest_meta,
                    true,
                    move |table_meta| {
                        // Validate the optimistic-concurrency guard against the freshly
                        // re-read metadata before applying updates (same semantics as the
                        // catalog commit path, mirroring `IcebergCommitExec`).
                        validate_procedure_requirements(table_meta, &requirements)?;
                        apply_procedure_updates(table_meta, &updates_for_commit)?;
                        Ok(())
                    },
                )
                .await?;
            }
            IcebergCatalogCommitMode::CatalogCommit
            | IcebergCatalogCommitMode::CompatibilityCatalogCommit => {
                let lakehouse_table = self.lakehouse_table.as_ref().ok_or_else(|| {
                    DataFusionError::Internal(
                        "missing lakehouse context for Iceberg catalog commit".to_string(),
                    )
                })?;
                let catalog_table = catalog_table.ok_or_else(|| {
                    DataFusionError::Internal(
                        "missing catalog table for Iceberg catalog commit".to_string(),
                    )
                })?;
                let coordinator =
                    IcebergCatalogCommitCoordinator::new(context.as_ref(), &catalog_table);
                match coordinator
                    .commit(lakehouse_table, std::mem::take(&mut requirements), updates)
                    .await?
                {
                    CatalogCommitOutcome::Committed(_) => {}
                    CatalogCommitOutcome::NotSupported => {
                        return internal_err!(
                            "Iceberg catalog commit is not supported by the resolved catalog authority"
                        );
                    }
                    CatalogCommitOutcome::Conflict => {
                        return internal_err!(
                            "Iceberg catalog commit conflict while executing CALL"
                        );
                    }
                }
            }
            IcebergCatalogCommitMode::MetadataLocationCas => {
                return internal_err!(
                    "CALL procedures do not support metadata-location CAS commit yet"
                );
            }
        }

        self.output.to_record_batch()
    }
}

impl DisplayAs for CallProcedureExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CallProcedureExec: procedure={:?}, table={}",
                    self.procedure, self.table_url
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "procedure={:?}", self.procedure)?;
                write!(f, "table={}", self.table_url)
            }
        }
    }
}

impl ExecutionPlan for CallProcedureExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("{} should not have children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "{} expects only partition 0 but got {}",
                self.name(),
                partition
            );
        }
        let schema = self.schema();
        let this = Arc::new(self.clone());
        let stream = once(async move { this.execute_call(context).await });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Computes the `TableUpdate`s for a procedure against the current table metadata.
pub fn compute_procedure_updates(
    procedure: &CallProcedure,
    metadata: &crate::spec::TableMetadata,
) -> Result<Vec<TableUpdate>> {
    match procedure {
        CallProcedure::RollbackToSnapshot { snapshot_id, .. } => {
            if metadata.snapshot(*snapshot_id).is_none() {
                return internal_err!("snapshot {snapshot_id} does not exist");
            }
            // Rollback requires the target to be an ancestor of the current state
            // (distinct from `set_current_snapshot`, which has no ancestry requirement).
            if !is_current_ancestor(metadata, *snapshot_id) {
                return internal_err!(
                    "Cannot roll back to snapshot, not an ancestor of the current state: {snapshot_id}"
                );
            }
            Ok(vec![set_main_snapshot_ref(metadata, *snapshot_id)])
        }
        CallProcedure::SetCurrentSnapshot { .. } => {
            let snapshot_id =
                resolve_target_snapshot_id(procedure, metadata)?.ok_or_else(|| {
                    internal_datafusion_err!("missing snapshot_id or ref for set_current_snapshot")
                })?;
            if metadata.snapshot(snapshot_id).is_none() {
                return internal_err!("snapshot {snapshot_id} does not exist");
            }
            Ok(vec![set_main_snapshot_ref(metadata, snapshot_id)])
        }
        CallProcedure::ExpireSnapshots {
            older_than_ms,
            retain_last,
            ..
        } => {
            let now = crate::utils::timestamp::monotonic_timestamp_ms();
            let default_max_age_ms = metadata
                .properties
                .get("history.expire.max-snapshot-age-ms")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(DEFAULT_MAX_SNAPSHOT_AGE_MS);
            let default_min_keep = metadata
                .properties
                .get("history.expire.min-snapshots-to-keep")
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(DEFAULT_MIN_SNAPSHOTS_TO_KEEP);
            let older_than = older_than_ms.unwrap_or(now - default_max_age_ms);
            let retain_last = retain_last.unwrap_or(default_min_keep);

            // Retained refs: `main` always; non-`main` refs only if their snapshot still
            // exists and has not aged past `max-ref-age-ms` (default: never).
            let default_max_ref_age_ms = metadata
                .properties
                .get("history.expire.max-ref-age-ms")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(i64::MAX);
            let mut retained_refs: std::collections::HashMap<&String, &SnapshotReference> =
                std::collections::HashMap::new();
            for (name, reference) in &metadata.refs {
                if name == MAIN_BRANCH {
                    retained_refs.insert(name, reference);
                    continue;
                }
                let Some(snap) = metadata.snapshot(reference.snapshot_id) else {
                    // Dangling ref: its snapshot no longer exists.
                    continue;
                };
                let max_ref_age_ms = reference.max_ref_age_ms().unwrap_or(default_max_ref_age_ms);
                if now - snap.timestamp_ms() <= max_ref_age_ms {
                    retained_refs.insert(name, reference);
                }
            }

            // Retained snapshot ids: every retained ref's target, plus per-branch ancestry
            // (head-inclusive) up to `min_snapshots_to_keep` or `older_than`, plus
            // unreferenced-but-recent snapshots.
            let mut retained: std::collections::HashSet<i64> =
                retained_refs.values().map(|r| r.snapshot_id).collect();
            let mut referenced: std::collections::HashSet<i64> = std::collections::HashSet::new();
            for (_, reference) in &retained_refs {
                if !reference.is_branch() {
                    referenced.insert(reference.snapshot_id);
                    continue;
                }
                let min_keep = reference
                    .min_snapshots_to_keep()
                    .unwrap_or(retain_last)
                    .max(1) as usize;
                let cutoff_ms = reference
                    .max_snapshot_age_ms()
                    .map(|age| now - age)
                    .unwrap_or(older_than);
                let mut kept = 0usize;
                let mut current = metadata.snapshot(reference.snapshot_id);
                while let Some(snap) = current {
                    referenced.insert(snap.snapshot_id());
                    if kept < min_keep || snap.timestamp_ms() >= cutoff_ms {
                        retained.insert(snap.snapshot_id());
                        kept += 1;
                        current = snap
                            .parent_snapshot_id()
                            .and_then(|id| metadata.snapshot(id));
                    } else {
                        break;
                    }
                }
            }
            for snap in &metadata.snapshots {
                if !referenced.contains(&snap.snapshot_id()) && snap.timestamp_ms() >= older_than {
                    retained.insert(snap.snapshot_id());
                }
            }

            let expired_ids: Vec<i64> = metadata
                .snapshots
                .iter()
                .filter(|s| !retained.contains(&s.snapshot_id()))
                .map(|s| s.snapshot_id())
                .collect();
            if expired_ids.is_empty() {
                return Ok(vec![]);
            }
            let mut updates = vec![TableUpdate::RemoveSnapshots {
                snapshot_ids: expired_ids,
            }];
            for (name, reference) in &metadata.refs {
                // Drop non-`main` refs whose target snapshot is being expired.
                if name != MAIN_BRANCH && !retained.contains(&reference.snapshot_id) {
                    updates.push(TableUpdate::RemoveSnapshotRef {
                        ref_name: name.clone(),
                    });
                }
            }
            Ok(updates)
        }
    }
}

/// Default `history.expire.max-snapshot-age-ms` (5 days) per the Iceberg spec.
const DEFAULT_MAX_SNAPSHOT_AGE_MS: i64 = 432_000_000;
/// Default `history.expire.min-snapshots-to-keep` per the Iceberg spec.
const DEFAULT_MIN_SNAPSHOTS_TO_KEEP: i32 = 1;

/// Builds the `SetSnapshotRef` update pointing `main` at `snapshot_id`, preserving the
/// current `main` retention policy (or a default branch retention when absent).
fn set_main_snapshot_ref(metadata: &crate::spec::TableMetadata, snapshot_id: i64) -> TableUpdate {
    let retention = metadata
        .refs
        .get(MAIN_BRANCH)
        .map(|r| r.retention.clone())
        .unwrap_or(SnapshotRetention::Branch {
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        });
    TableUpdate::SetSnapshotRef {
        ref_name: MAIN_BRANCH.to_string(),
        reference: crate::spec::SnapshotReference {
            snapshot_id,
            retention,
        },
    }
}

/// Resolves the target snapshot id for `rollback_to_snapshot` / `set_current_snapshot`.
///
/// For `set_current_snapshot` with a `ref` (branch/tag) argument, resolves the ref name to
/// its referenced snapshot id from the table metadata's `refs` map, mirroring the Iceberg
/// procedure ("Cannot find matching snapshot ID for ref %s"). Returns `None` when neither a
/// `snapshot_id` nor a `ref` was provided.
pub(crate) fn resolve_target_snapshot_id(
    procedure: &CallProcedure,
    metadata: &crate::spec::TableMetadata,
) -> Result<Option<i64>> {
    match procedure {
        CallProcedure::RollbackToSnapshot { snapshot_id, .. } => Ok(Some(*snapshot_id)),
        CallProcedure::SetCurrentSnapshot {
            snapshot_id, r#ref, ..
        } => match (snapshot_id, r#ref) {
            (Some(id), None) => Ok(Some(*id)),
            (None, Some(ref_name)) => {
                let reference = metadata.refs.get(ref_name).ok_or_else(|| {
                    internal_datafusion_err!("Cannot find matching snapshot ID for ref {ref_name}")
                })?;
                Ok(Some(reference.snapshot_id))
            }
            (Some(_), Some(_)) => {
                internal_err!("Either snapshot_id or ref must be provided, not both")
            }
            (None, None) => Ok(None),
        },
        CallProcedure::ExpireSnapshots { .. } => Ok(None),
    }
}

/// Whether `snapshot_id` is the current snapshot or one of its ancestors (walking
/// `parent_snapshot_id`), per the Iceberg rollback contract. The current snapshot is its
/// own ancestor; an empty table has no ancestors.
fn is_current_ancestor(metadata: &crate::spec::TableMetadata, snapshot_id: i64) -> bool {
    let mut current = metadata.current_snapshot();
    while let Some(snap) = current {
        if snap.snapshot_id() == snapshot_id {
            return true;
        }
        current = snap
            .parent_snapshot_id()
            .and_then(|id| metadata.snapshot(id));
    }
    false
}

/// The table requirement guarding the commit: the `main` ref must still point at the
/// snapshot that was current when the procedure was resolved.
pub fn procedure_requirements(metadata: &crate::spec::TableMetadata) -> Vec<TableRequirement> {
    vec![TableRequirement::RefSnapshotIdMatch {
        r#ref: MAIN_BRANCH.to_string(),
        snapshot_id: metadata.refs.get(MAIN_BRANCH).map(|r| r.snapshot_id),
    }]
}

/// The spec-shaped single-row output of a `CALL <catalog>.system.<procedure>(...)`.
///
/// Mirrors the Apache Iceberg Spark procedures output tables:
/// - `rollback_to_snapshot` / `set_current_snapshot` →
///   `previous_snapshot_id`, `current_snapshot_id`;
/// - `expire_snapshots` → six `deleted_*_count` columns (all `0` for the
///   metadata-only v1 implementation).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallProcedureOutput {
    /// `rollback_to_snapshot` / `set_current_snapshot`.
    SnapshotRef {
        previous_snapshot_id: i64,
        current_snapshot_id: i64,
    },
    /// `expire_snapshots` — metadata-only, so all counts are zero.
    ExpireSnapshots,
}

impl CallProcedureOutput {
    /// The fixed Arrow schema for this output.
    pub fn schema(&self) -> SchemaRef {
        let fields = match self {
            Self::SnapshotRef { .. } => vec![
                Field::new("previous_snapshot_id", DataType::Int64, true),
                Field::new("current_snapshot_id", DataType::Int64, true),
            ],
            Self::ExpireSnapshots => vec![
                Field::new("deleted_data_files_count", DataType::Int64, true),
                Field::new("deleted_position_delete_files_count", DataType::Int64, true),
                Field::new("deleted_equality_delete_files_count", DataType::Int64, true),
                Field::new("deleted_manifest_files_count", DataType::Int64, true),
                Field::new("deleted_manifest_lists_count", DataType::Int64, true),
                Field::new("deleted_statistics_files_count", DataType::Int64, true),
            ],
        };
        Arc::new(Schema::new(fields))
    }

    /// Builds the single-row result batch for this output.
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let schema = self.schema();
        let columns: Vec<datafusion::arrow::array::ArrayRef> = match self {
            Self::SnapshotRef {
                previous_snapshot_id,
                current_snapshot_id,
            } => vec![
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                    *previous_snapshot_id,
                ])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                    *current_snapshot_id,
                ])),
            ],
            Self::ExpireSnapshots => vec![
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![0i64])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![0i64])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![0i64])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![0i64])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![0i64])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![0i64])),
            ],
        };
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

/// Validates the procedure's `RefSnapshotIdMatch` requirement against freshly-loaded
/// metadata on the filesystem commit path.
///
/// Mirrors `IcebergCommitExec::validate_requirements` for the only requirement a CALL
/// procedure issues (`main` must still point at the plan-time snapshot). `main` is
/// resolved via `current_snapshot_id`; a `None`/negative expected id means "must not
/// exist". Fails with a conflict-style error so the caller can treat it like the
/// catalog commit path's `CatalogCommitOutcome::Conflict`.
fn validate_procedure_requirements(
    table_meta: &crate::spec::TableMetadata,
    requirements: &[TableRequirement],
) -> Result<()> {
    for requirement in requirements {
        let TableRequirement::RefSnapshotIdMatch {
            r#ref: reference,
            snapshot_id: expected,
        } = requirement
        else {
            return internal_err!(
                "unsupported TableRequirement for CALL procedure: {requirement:?}"
            );
        };
        let actual = if reference == MAIN_BRANCH {
            table_meta.current_snapshot_id
        } else {
            table_meta
                .refs
                .get(reference)
                .map(|ref_entry| ref_entry.snapshot_id)
        };
        let actual = actual.filter(|snapshot_id| *snapshot_id >= 0);
        if &actual != expected {
            return internal_err!(
                "Iceberg commit failed: reference '{}' expected snapshot {:?} but found {:?}",
                reference,
                expected,
                actual
            );
        }
    }
    Ok(())
}

/// Applies the computed `TableUpdate`s directly to `TableMetadata` (filesystem commit).
fn apply_procedure_updates(
    table_meta: &mut crate::spec::TableMetadata,
    updates: &[TableUpdate],
) -> Result<()> {
    for update in updates {
        match update {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                table_meta.refs.insert(ref_name.clone(), reference.clone());
                if ref_name == MAIN_BRANCH {
                    table_meta.current_snapshot_id = Some(reference.snapshot_id);
                }
            }
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                let ids: std::collections::HashSet<i64> = snapshot_ids.iter().copied().collect();
                table_meta
                    .snapshots
                    .retain(|s| !ids.contains(&s.snapshot_id()));
            }
            TableUpdate::RemoveSnapshotRef { ref_name } => {
                table_meta.refs.remove(ref_name);
            }
            other => {
                return internal_err!("unsupported TableUpdate for CALL procedure: {other:?}");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::spec::snapshots::{SnapshotReference, SnapshotRetention, Summary};
    use crate::spec::{FormatVersion, Operation, Schema, Snapshot};

    fn sample_metadata() -> crate::spec::TableMetadata {
        let mut meta = crate::spec::TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: None,
            location: "s3://bucket/tbl".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 0,
            last_column_id: 0,
            schemas: vec![Schema::builder().with_schema_id(0).build().unwrap()],
            current_schema_id: 0,
            partition_specs: vec![
                crate::spec::partition::spec::PartitionSpec::unpartitioned_spec(),
            ],
            default_spec_id: 0,
            last_partition_id: 0,
            properties: HashMap::new(),
            current_snapshot_id: Some(2),
            next_row_id: None,
            encryption_keys: vec![],
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![],
            default_sort_order_id: None,
            refs: HashMap::new(),
            statistics: vec![],
            partition_statistics: vec![],
        };
        let snap1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(1_000)
            .with_manifest_list("s3://bucket/m1.avro")
            .with_summary(Summary::new(Operation::Append))
            .build()
            .unwrap();
        let snap2 = Snapshot::builder()
            .with_snapshot_id(2)
            .with_parent_snapshot_id(1)
            .with_sequence_number(2)
            .with_timestamp_ms(2_000)
            .with_manifest_list("s3://bucket/m2.avro")
            .with_summary(Summary::new(Operation::Append))
            .build()
            .unwrap();
        meta.snapshots = vec![snap1, snap2];
        meta.refs = HashMap::from([(
            "main".to_string(),
            SnapshotReference {
                snapshot_id: 2,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        )]);
        meta
    }

    /// Metadata with a four-deep snapshot chain `1(1000ms) → 2(2000ms) → 3(3000ms) →
    /// 4(4000ms)` where `main` points at 4. No extra refs; retention defaults.
    fn chain_metadata() -> crate::spec::TableMetadata {
        let mut meta = sample_metadata();
        meta.snapshots = (1..=4)
            .map(|id| {
                let mut builder = Snapshot::builder()
                    .with_snapshot_id(id)
                    .with_sequence_number(id)
                    .with_timestamp_ms(id * 1_000)
                    .with_manifest_list(format!("s3://bucket/m{id}.avro"))
                    .with_summary(Summary::new(Operation::Append));
                if id > 1 {
                    builder = builder.with_parent_snapshot_id(id - 1);
                }
                builder.build().unwrap()
            })
            .collect();
        meta.current_snapshot_id = Some(4);
        meta.refs = HashMap::from([(
            "main".to_string(),
            SnapshotReference {
                snapshot_id: 4,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        )]);
        meta
    }

    /// A tag `t` pointing at snapshot 1.
    fn tag_ref(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Tag {
                max_ref_age_ms: None,
            },
        }
    }

    #[test]
    fn rollback_computes_set_snapshot_ref_for_main() {
        let meta = sample_metadata();
        let updates = compute_procedure_updates(
            &CallProcedure::RollbackToSnapshot {
                table: "t".to_string(),
                snapshot_id: 1,
            },
            &meta,
        )
        .unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, MAIN_BRANCH);
                assert_eq!(reference.snapshot_id, 1);
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn rollback_rejects_missing_snapshot() {
        let meta = sample_metadata();
        let err = compute_procedure_updates(
            &CallProcedure::RollbackToSnapshot {
                table: "t".to_string(),
                snapshot_id: 99,
            },
            &meta,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn rollback_allows_current_snapshot() {
        let meta = chain_metadata();
        let updates = compute_procedure_updates(
            &CallProcedure::RollbackToSnapshot {
                table: "t".to_string(),
                snapshot_id: 4,
            },
            &meta,
        )
        .unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, MAIN_BRANCH);
                assert_eq!(reference.snapshot_id, 4);
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn rollback_rejects_non_ancestor() {
        let mut meta = chain_metadata();
        // A detached sibling snapshot that is not on the main ancestry (1→2→3→4).
        let snap5 = Snapshot::builder()
            .with_snapshot_id(5)
            .with_sequence_number(5)
            .with_timestamp_ms(5_000)
            .with_manifest_list("s3://bucket/m5.avro")
            .with_summary(Summary::new(Operation::Append))
            .build()
            .unwrap();
        meta.snapshots.push(snap5);
        let err = compute_procedure_updates(
            &CallProcedure::RollbackToSnapshot {
                table: "t".to_string(),
                snapshot_id: 5,
            },
            &meta,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("Cannot roll back to snapshot, not an ancestor of the current state: 5"));
    }

    #[test]
    fn rollback_rejects_when_no_current_snapshot() {
        let mut meta = chain_metadata();
        meta.current_snapshot_id = None;
        meta.refs = HashMap::new();
        // The snapshot exists, but there is no current snapshot, so nothing is an ancestor.
        let err = compute_procedure_updates(
            &CallProcedure::RollbackToSnapshot {
                table: "t".to_string(),
                snapshot_id: 3,
            },
            &meta,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("not an ancestor of the current state"));
    }

    #[test]
    fn set_current_snapshot_does_not_require_ancestry() {
        let mut meta = chain_metadata();
        let snap5 = Snapshot::builder()
            .with_snapshot_id(5)
            .with_sequence_number(5)
            .with_timestamp_ms(5_000)
            .with_manifest_list("s3://bucket/m5.avro")
            .with_summary(Summary::new(Operation::Append))
            .build()
            .unwrap();
        meta.snapshots.push(snap5);
        // set_current_snapshot only requires existence, not ancestry.
        let updates = compute_procedure_updates(
            &CallProcedure::SetCurrentSnapshot {
                table: "t".to_string(),
                snapshot_id: Some(5),
                r#ref: None,
            },
            &meta,
        )
        .unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, MAIN_BRANCH);
                assert_eq!(reference.snapshot_id, 5);
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn expire_computes_remove_snapshots_skipping_current() {
        let meta = sample_metadata();
        // Nothing older than 500ms → nothing expires.
        let updates = compute_procedure_updates(
            &CallProcedure::ExpireSnapshots {
                table: "t".to_string(),
                older_than_ms: Some(500),
                retain_last: None,
            },
            &meta,
        )
        .unwrap();
        assert!(updates.is_empty());

        // Snapshot 1 (1000ms) is expired; snapshot 2 (2000ms) is the current/main
        // snapshot and must be retained.
        let updates = compute_procedure_updates(
            &CallProcedure::ExpireSnapshots {
                table: "t".to_string(),
                older_than_ms: Some(1_500),
                retain_last: None,
            },
            &meta,
        )
        .unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                assert_eq!(snapshot_ids, &vec![1]);
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn apply_set_snapshot_ref_updates_current_and_refs() {
        let mut meta = sample_metadata();
        let updates = compute_procedure_updates(
            &CallProcedure::SetCurrentSnapshot {
                table: "t".to_string(),
                snapshot_id: Some(1),
                r#ref: None,
            },
            &meta,
        )
        .unwrap();
        apply_procedure_updates(&mut meta, &updates).unwrap();
        assert_eq!(meta.current_snapshot_id, Some(1));
        assert_eq!(meta.refs.get("main").unwrap().snapshot_id, 1);
    }

    #[test]
    fn output_snapshot_ref_has_spec_schema_and_row() {
        let output = CallProcedureOutput::SnapshotRef {
            previous_snapshot_id: 2,
            current_snapshot_id: 1,
        };
        let schema = output.schema();
        let fields: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(fields, vec!["previous_snapshot_id", "current_snapshot_id"]);
        assert!(schema
            .fields()
            .iter()
            .all(|f| f.data_type() == &DataType::Int64));

        let batch = output.to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let prev = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        let curr = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(prev.value(0), 2);
        assert_eq!(curr.value(0), 1);
    }

    #[test]
    fn output_expire_has_six_zero_counts() {
        let output = CallProcedureOutput::ExpireSnapshots;
        let schema = output.schema();
        let fields: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            fields,
            vec![
                "deleted_data_files_count",
                "deleted_position_delete_files_count",
                "deleted_equality_delete_files_count",
                "deleted_manifest_files_count",
                "deleted_manifest_lists_count",
                "deleted_statistics_files_count",
            ]
        );

        let batch = output.to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 6);
        for col in batch.columns() {
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 0);
        }
    }

    #[test]
    fn filesystem_requirements_pass_when_main_unchanged() {
        let meta = sample_metadata();
        let requirements = procedure_requirements(&meta);
        assert_eq!(requirements.len(), 1);
        validate_procedure_requirements(&meta, &requirements).unwrap();
    }

    #[test]
    fn filesystem_requirements_reject_moved_main() {
        let mut meta = sample_metadata();
        meta.refs.get_mut("main").unwrap().snapshot_id = 99;
        meta.current_snapshot_id = Some(99);
        let requirements = procedure_requirements(&sample_metadata());
        let err = validate_procedure_requirements(&meta, &requirements).unwrap_err();
        assert!(err
            .to_string()
            .contains("expected snapshot Some(2) but found Some(99)"));
    }

    #[test]
    fn expire_retain_last_one_expires_oldest_only() {
        let meta = chain_metadata();
        // Only snapshot 4 (4000ms) is recent; with retain_last=1 only its ancestry up to
        // the first old ancestor survives, so 1, 2 and 3 all expire.
        let updates = compute_procedure_updates(
            &CallProcedure::ExpireSnapshots {
                table: "t".to_string(),
                older_than_ms: Some(3_500),
                retain_last: Some(1),
            },
            &meta,
        )
        .unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                assert_eq!(snapshot_ids, &vec![1, 2, 3]);
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn expire_retain_last_two_preserves_second_ancestor() {
        let meta = chain_metadata();
        // retain_last=2 additionally preserves snapshot 3 despite being older than the
        // 3500ms cutoff, so only 1 and 2 expire.
        let updates = compute_procedure_updates(
            &CallProcedure::ExpireSnapshots {
                table: "t".to_string(),
                older_than_ms: Some(3_500),
                retain_last: Some(2),
            },
            &meta,
        )
        .unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                assert_eq!(snapshot_ids, &vec![1, 2]);
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn expire_tag_referenced_snapshot_is_retained() {
        let mut meta = chain_metadata();
        meta.refs.insert("t".to_string(), tag_ref(1));
        let updates = compute_procedure_updates(
            &CallProcedure::ExpireSnapshots {
                table: "t".to_string(),
                older_than_ms: Some(3_500),
                retain_last: Some(1),
            },
            &meta,
        )
        .unwrap();
        // Snapshot 1 is the tag target and must survive; snapshots 2 and 3 (old, beyond
        // the count floor) expire. No `RemoveSnapshotRef` is emitted for the tag.
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                assert_eq!(snapshot_ids, &vec![2, 3]);
            }
            other => panic!("unexpected update: {other:?}"),
        }
        assert!(!updates
            .iter()
            .any(|u| matches!(u, TableUpdate::RemoveSnapshotRef { ref_name } if ref_name == "t")));
    }

    #[test]
    fn expire_branch_referenced_head_is_retained() {
        let mut meta = chain_metadata();
        meta.refs.insert(
            "b".to_string(),
            SnapshotReference {
                snapshot_id: 2,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        );
        let updates = compute_procedure_updates(
            &CallProcedure::ExpireSnapshots {
                table: "t".to_string(),
                older_than_ms: Some(3_500),
                retain_last: Some(1),
            },
            &meta,
        )
        .unwrap();
        // Branch `b`'s count floor (1) keeps its head snapshot 2; its ancestor 1 and the
        // main-branch middle snapshots 3 still expire. No `RemoveSnapshotRef` for `b`.
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                assert_eq!(snapshot_ids, &vec![1, 3]);
            }
            other => panic!("unexpected update: {other:?}"),
        }
        assert!(!updates
            .iter()
            .any(|u| matches!(u, TableUpdate::RemoveSnapshotRef { ref_name } if ref_name == "b")));
    }

    #[test]
    fn expire_drops_aged_out_ref_and_its_snapshot() {
        let mut meta = chain_metadata();
        // A tag with a tiny max-ref-age against an old snapshot ages out: the ref is
        // dropped (RemoveSnapshotRef) and the snapshot becomes expirable.
        meta.refs.insert(
            "t".to_string(),
            SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: Some(100),
                },
            },
        );
        let updates = compute_procedure_updates(
            &CallProcedure::ExpireSnapshots {
                table: "t".to_string(),
                older_than_ms: Some(3_500),
                retain_last: Some(1),
            },
            &meta,
        )
        .unwrap();
        assert_eq!(updates.len(), 2);
        let has_remove_ref = updates
            .iter()
            .any(|u| matches!(u, TableUpdate::RemoveSnapshotRef { ref_name } if ref_name == "t"));
        assert!(
            has_remove_ref,
            "expected RemoveSnapshotRef for aged-out tag, got {updates:?}"
        );
        match updates
            .iter()
            .find(|u| matches!(u, TableUpdate::RemoveSnapshots { .. }))
            .unwrap()
        {
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                assert!(snapshot_ids.contains(&1));
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn expire_uses_defaults_when_args_omitted() {
        let mut meta = chain_metadata();
        // No older_than/retain_last: defaults are 5 days ago and 1. All chain snapshots
        // are far older than 5 days, so only the head's count floor (1) is retained.
        meta.properties.insert(
            "history.expire.max-snapshot-age-ms".to_string(),
            "1000".to_string(),
        );
        let updates = compute_procedure_updates(
            &CallProcedure::ExpireSnapshots {
                table: "t".to_string(),
                older_than_ms: None,
                retain_last: None,
            },
            &meta,
        )
        .unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                assert_eq!(snapshot_ids, &vec![1, 2, 3]);
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn set_current_snapshot_resolves_ref_to_snapshot_id() {
        let mut meta = chain_metadata();
        meta.refs.insert("t".to_string(), tag_ref(2));
        let updates = compute_procedure_updates(
            &CallProcedure::SetCurrentSnapshot {
                table: "t".to_string(),
                snapshot_id: None,
                r#ref: Some("t".to_string()),
            },
            &meta,
        )
        .unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, MAIN_BRANCH);
                assert_eq!(reference.snapshot_id, 2);
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn set_current_snapshot_rejects_missing_ref() {
        let meta = chain_metadata();
        let err = compute_procedure_updates(
            &CallProcedure::SetCurrentSnapshot {
                table: "t".to_string(),
                snapshot_id: None,
                r#ref: Some("missing".to_string()),
            },
            &meta,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("Cannot find matching snapshot ID for ref missing"));
    }

    #[test]
    fn set_current_snapshot_rejects_both_snapshot_id_and_ref() {
        let meta = chain_metadata();
        let err = resolve_target_snapshot_id(
            &CallProcedure::SetCurrentSnapshot {
                table: "t".to_string(),
                snapshot_id: Some(1),
                r#ref: Some("t".to_string()),
            },
            &meta,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("Either snapshot_id or ref must be provided, not both"));
    }

    #[test]
    fn snapshot_metadata_lookup_by_id() {
        let meta = chain_metadata();
        assert_eq!(meta.snapshot(3).unwrap().snapshot_id(), 3);
        assert!(meta.snapshot(99).is_none());
    }
}

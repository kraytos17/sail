use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{Result, internal_err};
use sail_common_datafusion::datasource::TableFormatProcedureOperation;

use crate::spec::{
    MAIN_BRANCH, Snapshot, SnapshotReference, SnapshotRetention, TableMetadata, TableRequirement,
    TableUpdate,
};

/// Look up a snapshot by ID in the metadata's snapshot list.
fn find_snapshot<'a>(metadata: &'a TableMetadata, snapshot_id: i64) -> Option<&'a Snapshot> {
    metadata
        .snapshots
        .iter()
        .find(|s| s.snapshot_id() == snapshot_id)
}

/// Computes the metadata `TableUpdate`s for a CALL procedure against the given metadata.
pub(crate) fn compute_procedure_updates(
    procedure: &TableFormatProcedureOperation,
    metadata: &TableMetadata,
) -> Result<Vec<TableUpdate>> {
    match procedure {
        TableFormatProcedureOperation::RollbackToSnapshot { snapshot_id } => {
            if find_snapshot(metadata, *snapshot_id).is_none() {
                return internal_err!("snapshot {snapshot_id} does not exist");
            }
            if !is_current_ancestor(metadata, *snapshot_id) {
                return internal_err!(
                    "Cannot roll back to snapshot, not an ancestor of the current state: {snapshot_id}"
                );
            }
            Ok(vec![set_main_snapshot_ref(metadata, *snapshot_id)])
        }
        TableFormatProcedureOperation::SetCurrentSnapshot { .. } => {
            let Some(snapshot_id) = resolve_target_snapshot_id(procedure, metadata)? else {
                return internal_err!("missing snapshot_id or ref for set_current_snapshot");
            };
            if find_snapshot(metadata, snapshot_id).is_none() {
                return internal_err!("snapshot {snapshot_id} does not exist");
            }
            Ok(vec![set_main_snapshot_ref(metadata, snapshot_id)])
        }
        TableFormatProcedureOperation::ExpireSnapshots {
            older_than_ms,
            retain_last,
        } => expire_snapshot_updates(metadata, *older_than_ms, *retain_last),
    }
}

/// Default `history.expire.max-snapshot-age-ms` (5 days) per the Iceberg spec.
const DEFAULT_MAX_SNAPSHOT_AGE_MS: i64 = 432_000_000;
/// Default `history.expire.min-snapshots-to-keep` per the Iceberg spec.
const DEFAULT_MIN_SNAPSHOTS_TO_KEEP: i32 = 1;

/// Computes `RemoveSnapshots` / `RemoveSnapshotRef` for `expire_snapshots` using the spec
/// retain-set algorithm. Returns `Ok(vec![])` when nothing expires.
fn expire_snapshot_updates(
    metadata: &TableMetadata,
    older_than_ms: Option<i64>,
    retain_last: Option<i32>,
) -> Result<Vec<TableUpdate>> {
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

    let (retained, _) = retained_snapshot_ids(metadata, older_than, retain_last);

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
        if name != MAIN_BRANCH && !retained.contains(&reference.snapshot_id) {
            updates.push(TableUpdate::RemoveSnapshotRef {
                ref_name: name.clone(),
            });
        }
    }
    Ok(updates)
}

/// Computes the spec retain-set for `expire_snapshots` against the current metadata.
fn retained_snapshot_ids(
    metadata: &TableMetadata,
    older_than: i64,
    retain_last: i32,
) -> (
    std::collections::HashSet<i64>,
    std::collections::HashSet<i64>,
) {
    let now = crate::utils::timestamp::monotonic_timestamp_ms();
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
        let Some(snap) = find_snapshot(metadata, reference.snapshot_id) else {
            continue;
        };
        let max_ref_age_ms = match &reference.retention {
            SnapshotRetention::Branch { max_ref_age_ms, .. }
            | SnapshotRetention::Tag { max_ref_age_ms } => {
                max_ref_age_ms.unwrap_or(default_max_ref_age_ms)
            }
        };
        if now - snap.timestamp_ms() <= max_ref_age_ms {
            retained_refs.insert(name, reference);
        }
    }

    let mut retained: std::collections::HashSet<i64> =
        retained_refs.values().map(|r| r.snapshot_id).collect();
    let mut referenced: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for (_, reference) in &retained_refs {
        if !reference.is_branch() {
            referenced.insert(reference.snapshot_id);
            continue;
        }
        let (min_keep, max_snapshot_age_ms) = match &reference.retention {
            SnapshotRetention::Branch {
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                ..
            } => (
                min_snapshots_to_keep.unwrap_or(retain_last).max(1) as usize,
                *max_snapshot_age_ms,
            ),
            SnapshotRetention::Tag { .. } => unreachable!("is_branch() already checked"),
        };
        let cutoff_ms = max_snapshot_age_ms
            .map(|age| now - age)
            .unwrap_or(older_than);
        let mut kept = 0usize;
        let mut current = find_snapshot(metadata, reference.snapshot_id);
        while let Some(snap) = current {
            referenced.insert(snap.snapshot_id());
            if kept < min_keep || snap.timestamp_ms() >= cutoff_ms {
                retained.insert(snap.snapshot_id());
                kept += 1;
                current = snap
                    .parent_snapshot_id()
                    .and_then(|id| find_snapshot(metadata, id));
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

    (retained, referenced)
}

/// Builds the `SetSnapshotRef` update pointing `main` at `snapshot_id`, preserving the
/// current `main` retention policy (or a default branch retention when absent).
fn set_main_snapshot_ref(metadata: &TableMetadata, snapshot_id: i64) -> TableUpdate {
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
        reference: SnapshotReference {
            snapshot_id,
            retention,
        },
    }
}

/// Resolves the target snapshot id for `rollback_to_snapshot` / `set_current_snapshot`.
pub(crate) fn resolve_target_snapshot_id(
    procedure: &TableFormatProcedureOperation,
    metadata: &TableMetadata,
) -> Result<Option<i64>> {
    match procedure {
        TableFormatProcedureOperation::RollbackToSnapshot { snapshot_id } => Ok(Some(*snapshot_id)),
        TableFormatProcedureOperation::SetCurrentSnapshot {
            snapshot_id, r#ref, ..
        } => match (snapshot_id, r#ref) {
            (Some(id), None) => Ok(Some(*id)),
            (None, Some(ref_name)) => {
                let Some(reference) = metadata.refs.get(ref_name) else {
                    return internal_err!("Cannot find matching snapshot ID for ref {ref_name}");
                };
                Ok(Some(reference.snapshot_id))
            }
            (Some(_), Some(_)) => {
                internal_err!("Either snapshot_id or ref must be provided, not both")
            }
            (None, None) => Ok(None),
        },
        TableFormatProcedureOperation::ExpireSnapshots { .. } => Ok(None),
    }
}

/// Whether `snapshot_id` is the current snapshot or one of its ancestors (walking
/// `parent_snapshot_id`), per the Iceberg rollback contract.
fn is_current_ancestor(metadata: &TableMetadata, snapshot_id: i64) -> bool {
    let mut current = metadata.current_snapshot();
    while let Some(snap) = current {
        if snap.snapshot_id() == snapshot_id {
            return true;
        }
        current = snap
            .parent_snapshot_id()
            .and_then(|id| find_snapshot(metadata, id));
    }
    false
}

/// The table requirement guarding the commit: the `main` ref must still point at the
/// snapshot that was current when the procedure was resolved.
pub(crate) fn procedure_requirements(metadata: &TableMetadata) -> Vec<TableRequirement> {
    vec![TableRequirement::RefSnapshotIdMatch {
        r#ref: MAIN_BRANCH.to_string(),
        snapshot_id: metadata.refs.get(MAIN_BRANCH).map(|r| r.snapshot_id),
    }]
}

/// Computes the spec-shaped output row for a procedure against the current metadata.
pub(crate) fn compute_procedure_output(
    procedure: &TableFormatProcedureOperation,
    metadata: &TableMetadata,
) -> Result<CallProcedureOutput> {
    match procedure {
        TableFormatProcedureOperation::RollbackToSnapshot { .. }
        | TableFormatProcedureOperation::SetCurrentSnapshot { .. } => {
            let previous = metadata
                .refs
                .get(MAIN_BRANCH)
                .map(|r| r.snapshot_id)
                .or(metadata.current_snapshot_id)
                .unwrap_or(0);
            let current = resolve_target_snapshot_id(procedure, metadata)?.unwrap_or(0);
            Ok(CallProcedureOutput::SnapshotRef {
                previous_snapshot_id: previous,
                current_snapshot_id: current,
            })
        }
        TableFormatProcedureOperation::ExpireSnapshots { .. } => {
            Ok(CallProcedureOutput::ExpireSnapshots {
                deleted_data_files_count: 0,
                deleted_position_delete_files_count: 0,
                deleted_equality_delete_files_count: 0,
                deleted_manifest_files_count: 0,
                deleted_manifest_lists_count: 0,
                deleted_statistics_files_count: 0,
            })
        }
    }
}

/// The spec-shaped single-row output of a `CALL <catalog>.system.<procedure>(...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallProcedureOutput {
    SnapshotRef {
        previous_snapshot_id: i64,
        current_snapshot_id: i64,
    },
    ExpireSnapshots {
        deleted_data_files_count: i64,
        deleted_position_delete_files_count: i64,
        deleted_equality_delete_files_count: i64,
        deleted_manifest_files_count: i64,
        deleted_manifest_lists_count: i64,
        deleted_statistics_files_count: i64,
    },
}

impl CallProcedureOutput {
    /// The fixed Arrow schema for this output.
    pub fn schema(&self) -> SchemaRef {
        let fields = match self {
            Self::SnapshotRef { .. } => vec![
                Field::new("previous_snapshot_id", DataType::Int64, true),
                Field::new("current_snapshot_id", DataType::Int64, true),
            ],
            Self::ExpireSnapshots { .. } => vec![
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
        let columns: Vec<ArrayRef> = match self {
            Self::SnapshotRef {
                previous_snapshot_id,
                current_snapshot_id,
            } => vec![
                Arc::new(Int64Array::from(vec![*previous_snapshot_id])),
                Arc::new(Int64Array::from(vec![*current_snapshot_id])),
            ],
            Self::ExpireSnapshots {
                deleted_data_files_count,
                deleted_position_delete_files_count,
                deleted_equality_delete_files_count,
                deleted_manifest_files_count,
                deleted_manifest_lists_count,
                deleted_statistics_files_count,
            } => vec![
                Arc::new(Int64Array::from(vec![*deleted_data_files_count])),
                Arc::new(Int64Array::from(vec![*deleted_position_delete_files_count])),
                Arc::new(Int64Array::from(vec![*deleted_equality_delete_files_count])),
                Arc::new(Int64Array::from(vec![*deleted_manifest_files_count])),
                Arc::new(Int64Array::from(vec![*deleted_manifest_lists_count])),
                Arc::new(Int64Array::from(vec![*deleted_statistics_files_count])),
            ],
        };
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

/// Validates the procedure's `RefSnapshotIdMatch` requirement against freshly-loaded
/// metadata on the filesystem commit path.
pub(crate) fn validate_procedure_requirements(
    table_meta: &TableMetadata,
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
pub(crate) fn apply_procedure_updates(
    table_meta: &mut TableMetadata,
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
                table_meta
                    .statistics
                    .retain(|s| !ids.contains(&s.snapshot_id));
                table_meta
                    .partition_statistics
                    .retain(|s| !ids.contains(&s.snapshot_id));
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

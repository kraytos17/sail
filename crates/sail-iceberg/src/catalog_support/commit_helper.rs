use std::sync::Arc;

use bytes::Bytes;
use datafusion::execution::context::TaskContext;
use datafusion_common::{DataFusionError, Result};
use object_store::ObjectStoreExt;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::OptionLayer;
use url::Url;

use crate::catalog_support::commit::{
    catalog_requirements, table_metadata_location, CatalogCommitOutcome, CatalogTableInfo,
    IcebergCatalogCommitCoordinator, IcebergCatalogCommitMode,
};
use crate::io::StoreContext;
use crate::spec::{MetadataLog, TableMetadata, TableUpdate};
use crate::table::metadata_loader::{
    encode_metadata_file, metadata_file_extension_from_properties, metadata_file_version_from_path,
};
use crate::table_format::metadata_location_from_properties;
use crate::utils::timestamp::monotonic_timestamp_ms;

/// Result of a single-shot commit attempt via the shared helper.
pub(crate) enum CommitResult {
    Committed,
}

/// Attempt a single-shot Iceberg commit.
///
/// 1. Resolves catalog commit mode from `lakehouse_table` and `table_properties`.
/// 2. If catalog commit mode: calls `IcebergCatalogCommitCoordinator::commit()`.
/// 3. On `NotSupported` or `Conflict`: falls through to object-store write.
/// 4. Writes new metadata JSON + version-hint to the object store.
/// 5. Updates catalog metadata location if required.
///
/// The caller is responsible for the retry loop (reloading metadata, rebuilding
/// the action, and calling this function again on conflict).
///
/// `existing_metadata_location` — the metadata file path that was current at
/// read time (used for metadata_log and as previous location in catalog updates).
/// `initial_catalog_metadata_location` — the catalog's metadata location at the
/// start of the retry loop (for conflict detection on UUID-named metadata files).
pub(crate) async fn commit_iceberg_changes(
    context: Option<&Arc<TaskContext>>,
    store_ctx: &StoreContext,
    table_url: &Url,
    table_meta: &mut TableMetadata,
    action_commit: crate::operations::ActionCommit,
    lakehouse_table: Option<&LakehouseExecutionContext>,
    table_properties: &[(String, String)],
    existing_metadata_location: &str,
    initial_catalog_metadata_location: Option<String>,
) -> Result<CommitResult> {
    let catalog_table: Option<Vec<String>> = lakehouse_table
        .map(|t| t.catalog_table().to_vec())
        .filter(|v| !v.is_empty());

    let (catalog_table_info, mut catalog_commit_mode) = match context {
        Some(ctx) => {
            let info = match catalog_table.as_ref() {
                Some(table) => {
                    IcebergCatalogCommitCoordinator::load_table_info(ctx.as_ref(), table).await?
                }
                None => CatalogTableInfo::default(),
            };
            let mode = IcebergCatalogCommitMode::resolve(lakehouse_table, &info, table_properties);
            log::info!("iceberg commit: resolved commit mode={mode:?}");
            (info, mode)
        }
        None => (
            CatalogTableInfo::default(),
            IcebergCatalogCommitMode::Filesystem,
        ),
    };
    let _catalog_metadata_location = catalog_commit_mode
        .uses_catalog_metadata()
        .then(|| {
            metadata_location_from_properties(table_properties)
                .or(catalog_table_info.metadata_location.clone())
        })
        .flatten();

    let catalog_commit_table = catalog_table
        .as_ref()
        .filter(|_| catalog_commit_mode.uses_catalog_commit());
    let mut catalog_metadata_update_table = catalog_table
        .as_ref()
        .filter(|_| catalog_commit_mode.uses_metadata_location_update());
    let catalog_registered_metadata_table = catalog_table
        .as_ref()
        .filter(|_| matches!(catalog_commit_mode, IcebergCatalogCommitMode::Filesystem));

    let action_requirements = action_commit.requirements().to_vec();
    let action_updates = action_commit.into_updates();

    // Try catalog commit first
    if let (Some(catalog_table), Some(ctx)) = (catalog_commit_table, context) {
        log::info!("iceberg commit: attempting catalog-native commit");
        let requirements = catalog_requirements(table_meta, &[], &action_requirements);
        let updates = action_updates.clone();
        match IcebergCatalogCommitCoordinator::new(ctx.as_ref(), catalog_table)
            .commit(
                lakehouse_table.ok_or_else(|| {
                    DataFusionError::Internal(
                        "missing lakehouse context for Iceberg catalog commit".to_string(),
                    )
                })?,
                requirements,
                updates,
            )
            .await?
        {
            CatalogCommitOutcome::Committed(_) => {
                log::info!("iceberg commit: catalog-native commit succeeded");
                return Ok(CommitResult::Committed);
            }
            CatalogCommitOutcome::NotSupported => {
                log::warn!(
                    "iceberg commit: catalog commit not supported, falling back to object-store write"
                );
                // Recompute metadata update tables: when catalog commit is not supported,
                // fall back to MetadataLocationCas so we still update the catalog's
                // metadata-location pointer to the new metadata file.
                catalog_commit_mode = IcebergCatalogCommitMode::MetadataLocationCas;
                catalog_metadata_update_table = Some(catalog_table)
                    .filter(|_| catalog_commit_mode.uses_metadata_location_update());
            }
            CatalogCommitOutcome::Conflict => {
                return Err(DataFusionError::Execution(
                    "Iceberg commit conflict: concurrent modification".to_string(),
                ));
            }
        }
    }

    // Apply action updates to table_meta
    let mut newest_snapshot_seq: Option<i64> = None;
    let timestamp_ms = monotonic_timestamp_ms();
    for upd in action_updates {
        match upd {
            TableUpdate::AddSnapshot { snapshot } => {
                newest_snapshot_seq = Some(snapshot.sequence_number());
                table_meta.snapshots.push(snapshot.clone());
                table_meta.current_snapshot_id = Some(snapshot.snapshot_id());
                table_meta
                    .snapshot_log
                    .push(crate::spec::metadata::table_metadata::SnapshotLog {
                        timestamp_ms,
                        snapshot_id: snapshot.snapshot_id(),
                    });
            }
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                table_meta.refs.insert(ref_name, reference);
            }
            _ => {}
        }
    }
    if let Some(seq) = newest_snapshot_seq {
        if seq > table_meta.last_sequence_number {
            table_meta.last_sequence_number = seq;
        }
    }
    table_meta.last_updated_ms = timestamp_ms;

    // Add metadata_log entry
    table_meta.metadata_log.push(MetadataLog {
        timestamp_ms,
        metadata_file: existing_metadata_location.to_string(),
    });

    // Write new metadata file to object store
    let current_version = metadata_file_version_from_path(existing_metadata_location).unwrap_or(0);
    let next_version = current_version + 1;
    let new_meta_json = table_meta
        .to_json()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
    let use_uuid_metadata_file = catalog_metadata_update_table.is_some();
    let new_meta_rel = if use_uuid_metadata_file {
        format!(
            "metadata/{:05}-{}{}",
            next_version,
            uuid::Uuid::new_v4(),
            file_extension
        )
    } else {
        format!("metadata/v{next_version}{file_extension}")
    };
    let new_metadata_location = table_metadata_location(table_url, &new_meta_rel)?;

    let new_meta_bytes = encode_metadata_file(&new_meta_rel, &new_meta_json)
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
            return Err(DataFusionError::Execution(
                "Iceberg commit conflict: metadata file already exists".to_string(),
            ));
        }
        Err(e) => return Err(DataFusionError::External(Box::new(e))),
    }

    // Write version-hint.text
    let hint = if use_uuid_metadata_file {
        new_meta_rel
            .rsplit('/')
            .next()
            .unwrap_or(&new_meta_rel)
            .to_string()
    } else {
        next_version.to_string()
    };
    let hint_bytes = Bytes::from(hint.into_bytes());
    let hint_path = object_store::path::Path::from("metadata/version-hint.text");
    store_ctx
        .prefixed
        .put(&hint_path, object_store::PutPayload::from(hint_bytes))
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Update catalog metadata location if needed
    if let (Some(catalog_table), Some(ctx)) = (catalog_metadata_update_table, context) {
        log::info!(
            "iceberg commit: updating catalog metadata-location to {}",
            new_metadata_location
        );
        IcebergCatalogCommitCoordinator::new(ctx.as_ref(), catalog_table)
            .update_metadata_location(
                table_properties,
                initial_catalog_metadata_location.as_deref(),
                &new_metadata_location,
            )
            .await?;
        log::info!("iceberg commit: catalog metadata-location updated successfully");
    } else if let (Some(catalog_table), Some(ctx)) = (catalog_registered_metadata_table, context) {
        log::info!(
            "iceberg commit: updating catalog metadata-location (filesystem mode) to {}",
            new_metadata_location
        );
        IcebergCatalogCommitCoordinator::new(ctx.as_ref(), catalog_table)
            .update_metadata_location(
                table_properties,
                initial_catalog_metadata_location.as_deref(),
                &new_metadata_location,
            )
            .await?;
        log::info!("iceberg commit: catalog metadata-location updated successfully");
    }

    log::info!("iceberg commit: committed successfully");
    Ok(CommitResult::Committed)
}

/// Extract the first `TablePropertyList` from an `OptionLayer` slice.
pub(crate) fn extract_table_properties(options: &[OptionLayer]) -> Vec<(String, String)> {
    options
        .iter()
        .filter_map(|layer| match layer {
            OptionLayer::TablePropertyList { items } => Some(items.clone()),
            _ => None,
        })
        .next()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_properties_returns_first_list() {
        let options = vec![
            OptionLayer::OptionList {
                items: vec![("k1".to_string(), "v1".to_string())],
            },
            OptionLayer::TablePropertyList {
                items: vec![
                    ("prop1".to_string(), "val1".to_string()),
                    ("prop2".to_string(), "val2".to_string()),
                ],
            },
            OptionLayer::TablePropertyList {
                items: vec![("prop3".to_string(), "val3".to_string())],
            },
        ];
        let result = extract_table_properties(&options);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("prop1".to_string(), "val1".to_string()));
        assert_eq!(result[1], ("prop2".to_string(), "val2".to_string()));
    }

    #[test]
    fn test_extract_table_properties_returns_empty_when_no_table_property_list() {
        let options = vec![OptionLayer::OptionList {
            items: vec![("k1".to_string(), "v1".to_string())],
        }];
        let result = extract_table_properties(&options);
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_table_properties_returns_empty_when_empty_input() {
        let options: Vec<OptionLayer> = vec![];
        let result = extract_table_properties(&options);
        assert!(result.is_empty());
    }
}

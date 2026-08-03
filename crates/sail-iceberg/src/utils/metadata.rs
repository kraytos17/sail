use chrono::{DateTime, Utc};
use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;
use object_store::ObjectStoreExt;

use crate::io::StoreContext;
use crate::table::metadata_loader::metadata_file_version_from_path;

/// List all metadata files in the table's `metadata/` prefix that correspond to the
/// given version number. Returns (path, last_modified) pairs.
pub async fn metadata_files_for_version(
    store_ctx: &StoreContext,
    version: i32,
) -> Result<Vec<(String, DateTime<Utc>)>> {
    let prefix = object_store::path::Path::from("metadata/");
    let mut stream = store_ctx.prefixed.list(Some(&prefix));
    let mut matches = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.map_err(|e| DataFusionError::External(Box::new(e)))?;
        if metadata_file_version_from_path(meta.location.as_ref()) == Some(version) {
            matches.push((meta.location.to_string(), meta.last_modified));
        }
    }
    Ok(matches)
}

/// Returns true when a candidate metadata file's timestamp precedes the known
/// current-metadata timestamp. A file with a higher version but an older
/// timestamp than the current metadata cannot be from a concurrent write — it
/// is stale (e.g. left over from a previous table instance after DROP+CREATE).
pub fn is_stale_metadata_file(
    candidate_timestamp: DateTime<Utc>,
    current_metadata_timestamp: DateTime<Utc>,
) -> bool {
    candidate_timestamp < current_metadata_timestamp
}

/// Fetch the last-modified timestamp for a metadata file by its object-store path.
/// The path should be an absolute object path as returned by
/// `find_latest_metadata_file` (e.g. `testcat/…/metadata/00001-xxx.json`).
pub async fn get_metadata_file_timestamp(
    store_ctx: &StoreContext,
    metadata_path: &str,
) -> Result<DateTime<Utc>> {
    let path = object_store::path::Path::from(metadata_path);
    let head = store_ctx
        .base
        .head(&path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(head.last_modified)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_is_stale_metadata_file_when_candidate_is_older() {
        let current = Utc.with_ymd_and_hms(2026, 7, 14, 7, 0, 0).unwrap();
        let candidate = Utc.with_ymd_and_hms(2026, 7, 13, 13, 26, 0).unwrap();
        assert!(is_stale_metadata_file(candidate, current));
    }

    #[test]
    fn test_is_stale_metadata_file_when_candidate_is_newer() {
        let current = Utc.with_ymd_and_hms(2026, 7, 14, 7, 0, 0).unwrap();
        let candidate = Utc.with_ymd_and_hms(2026, 7, 14, 7, 0, 5).unwrap();
        assert!(!is_stale_metadata_file(candidate, current));
    }

    #[test]
    fn test_is_stale_metadata_file_when_candidate_is_equal() {
        let ts = Utc.with_ymd_and_hms(2026, 7, 14, 7, 0, 0).unwrap();
        assert!(!is_stale_metadata_file(ts, ts));
    }

    #[test]
    fn test_is_stale_metadata_file_when_candidate_is_seconds_newer() {
        let current = Utc.with_ymd_and_hms(2026, 7, 14, 7, 0, 1).unwrap();
        let candidate = Utc.with_ymd_and_hms(2026, 7, 14, 7, 0, 2).unwrap();
        assert!(!is_stale_metadata_file(candidate, current));
    }
}

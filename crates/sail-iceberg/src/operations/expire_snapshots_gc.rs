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

use std::collections::HashSet;

use datafusion_common::Result;
use object_store::{Error as ObjectStoreError, ObjectStoreExt};

use crate::io::{StoreContext, load_manifest, load_manifest_list};
use crate::spec::snapshots::Snapshot;
use crate::spec::{DataContentType, ManifestContentType, ManifestStatus, TableMetadata};

/// The kind of a file tracked by expire-snapshot garbage collection.
///
/// Mirrors the type tags used by the Iceberg Spark action's `DeleteSummary` so the
/// anti-join operates on the `(path, kind)` pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileKind {
    Data(DataContentType),
    Manifest,
    ManifestList,
    Statistics,
}

impl FileKind {
    /// A stable string tag identifying the kind, matching the Iceberg type tags.
    pub fn tag(&self) -> &'static str {
        match self {
            FileKind::Data(DataContentType::Data) => "DATA",
            FileKind::Data(DataContentType::PositionDeletes) => "POSITION_DELETES",
            FileKind::Data(DataContentType::EqualityDeletes) => "EQUALITY_DELETES",
            FileKind::Manifest => "Manifest",
            FileKind::ManifestList => "Manifest List",
            FileKind::Statistics => "Statistics Files",
        }
    }
}

/// Per-kind count of files physically deleted by a GC pass. Each count is the number of
/// **successful** deletes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExpireGcCounts {
    pub data_files: u64,
    pub position_delete_files: u64,
    pub equality_delete_files: u64,
    pub manifest_files: u64,
    pub manifest_lists: u64,
    pub statistics_files: u64,
}

/// Collects every file reachable from the given snapshots, as `(path, kind)` keys.
///
/// For each snapshot this includes, per the Iceberg Spark action's `fileDS`:
/// - every content file (data / position deletes / equality deletes) with an
///   `Added`/`Existing` manifest entry;
/// - every manifest file in the snapshot's manifest list;
/// - the snapshot's manifest list itself;
/// - any table-level statistics / partition-statistics files produced by the snapshot.
pub async fn collect_files(
    store_ctx: &StoreContext,
    metadata: &TableMetadata,
    snapshots: &[Snapshot],
) -> Result<Vec<(String, FileKind)>> {
    let mut files: Vec<(String, FileKind)> = Vec::new();
    let snapshot_ids: HashSet<i64> = snapshots.iter().map(|s| s.snapshot_id()).collect();

    for snapshot in snapshots {
        let manifest_list = load_manifest_list(store_ctx, snapshot.manifest_list()).await?;
        for manifest_file in manifest_list.entries() {
            files.push((manifest_file.manifest_path.clone(), FileKind::Manifest));
            match manifest_file.content {
                ManifestContentType::Data | ManifestContentType::Deletes => {
                    let manifest = load_manifest(store_ctx, &manifest_file.manifest_path).await?;
                    for entry in manifest.entries() {
                        if !matches!(
                            entry.status,
                            ManifestStatus::Added | ManifestStatus::Existing
                        ) {
                            continue;
                        }
                        let content = entry.data_file.content_type();
                        let kind = FileKind::Data(content);
                        files.push((entry.data_file.file_path().to_string(), kind));
                    }
                }
            }
        }
        files.push((snapshot.manifest_list().to_string(), FileKind::ManifestList));
    }

    for stats in &metadata.statistics {
        if snapshot_ids.contains(&stats.snapshot_id) {
            files.push((stats.statistics_path.clone(), FileKind::Statistics));
        }
    }
    for stats in &metadata.partition_statistics {
        if snapshot_ids.contains(&stats.snapshot_id) {
            files.push((stats.statistics_path.clone(), FileKind::Statistics));
        }
    }

    Ok(files)
}

/// Computes `candidates ∖ valid` on the `(path, kind)` pair.
///
/// This is the expire anti-join: a file that is still reachable from a retained snapshot
/// (i.e. present in `valid`) is never returned.
pub fn diff_files(
    candidates: Vec<(String, FileKind)>,
    valid: Vec<(String, FileKind)>,
) -> Vec<(String, FileKind)> {
    let valid: HashSet<(String, &'static str)> = valid
        .into_iter()
        .map(|(path, kind)| (path, kind.tag()))
        .collect();
    let mut seen: HashSet<(String, &'static str)> = HashSet::new();
    candidates
        .into_iter()
        .filter(|(path, kind)| {
            let key = (path.clone(), kind.tag());
            !valid.contains(&key) && seen.insert(key)
        })
        .collect()
}

/// Physically deletes the given files best-effort, counting only successful deletes.
///
/// A missing file (`NotFound`) is skipped without error and without being counted. Any other
/// failure is logged and skipped, so a single failure never aborts the GC pass.
pub async fn delete_files(
    store_ctx: &StoreContext,
    files: &[(String, FileKind)],
) -> Result<ExpireGcCounts> {
    let mut counts = ExpireGcCounts::default();
    for (path, kind) in files {
        let (store, object_path) = store_ctx.resolve(path)?;
        match store.delete(&object_path).await {
            Ok(()) => increment_count(&mut counts, kind),
            Err(ObjectStoreError::NotFound { .. }) => {
                log::debug!("expire_snapshots: file already gone, skipping: {path}");
            }
            Err(e) => {
                log::warn!("expire_snapshots: failed to delete {kind:?} file {path}: {e}");
            }
        }
    }
    Ok(counts)
}

fn increment_count(counts: &mut ExpireGcCounts, kind: &FileKind) {
    match kind {
        FileKind::Data(DataContentType::Data) => counts.data_files += 1,
        FileKind::Data(DataContentType::PositionDeletes) => counts.position_delete_files += 1,
        FileKind::Data(DataContentType::EqualityDeletes) => counts.equality_delete_files += 1,
        FileKind::Manifest => counts.manifest_files += 1,
        FileKind::ManifestList => counts.manifest_lists += 1,
        FileKind::Statistics => counts.statistics_files += 1,
    }
}

/// Runs a full expire-snapshot GC pass: deletes every file reachable from a snapshot that
/// was removed between `pre_commit` and `post_commit` metadata but still reachable from no
/// retained snapshot.
///
/// The expired snapshot set is the difference of the snapshot ids in the two metadata
/// versions, mirroring `ExpireSnapshotsSparkAction.findExpiredSnapshotIds`. Files are
/// collected from the pre-commit metadata (where the expired snapshots still exist) and
/// diffed against files collected from the post-commit metadata (the retained state).
pub async fn expire_files_gc(
    store_ctx: &StoreContext,
    pre_commit: &TableMetadata,
    post_commit: &TableMetadata,
) -> Result<ExpireGcCounts> {
    let post_ids: HashSet<i64> = post_commit
        .snapshots
        .iter()
        .map(|s| s.snapshot_id())
        .collect();
    let expired: Vec<Snapshot> = pre_commit
        .snapshots
        .iter()
        .filter(|s| !post_ids.contains(&s.snapshot_id()))
        .cloned()
        .collect();
    if expired.is_empty() {
        return Ok(ExpireGcCounts::default());
    }

    let candidates = collect_files(store_ctx, pre_commit, &expired).await?;
    let valid = collect_files(store_ctx, post_commit, &post_commit.snapshots).await?;
    let to_delete = diff_files(candidates, valid);
    delete_files(store_ctx, &to_delete).await
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use url::Url;

    use super::*;
    use crate::spec::manifest::{Manifest, ManifestMetadata};
    use crate::spec::manifest_list::{ManifestFile, ManifestListWriter};
    use crate::spec::snapshots::{Snapshot, SnapshotReference, SnapshotRetention, Summary};
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestContentType,
        ManifestStatus, Operation, PartitionSpec, Schema, TableMetadata,
    };

    fn schema() -> Schema {
        Schema::builder().with_schema_id(0).build().unwrap()
    }

    fn data_file(path: &str, content: DataContentType) -> DataFile {
        DataFile {
            content,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: vec![],
            record_count: 1,
            file_size_in_bytes: 100,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: 0,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn snapshot(id: i64, manifest_list: &str) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_sequence_number(id)
            .with_timestamp_ms(id * 1_000)
            .with_manifest_list(manifest_list)
            .with_summary(Summary::new(Operation::Append))
            .build()
            .unwrap()
    }

    fn manifest_file(path: &str, added_files: i32) -> ManifestFile {
        ManifestFile::builder()
            .with_manifest_path(path)
            .with_manifest_length(100)
            .with_content(ManifestContentType::Data)
            .with_sequence_number(1)
            .with_min_sequence_number(1)
            .with_added_snapshot_id(1)
            .with_file_counts(added_files, 0, 0)
            .with_row_counts(added_files as i64, 0, 0)
            .build()
            .expect("manifest file builder")
    }

    fn metadata(
        snapshots: Vec<Snapshot>,
        refs: HashMap<String, SnapshotReference>,
        statistics: Vec<crate::spec::StatisticsFile>,
        partition_statistics: Vec<crate::spec::PartitionStatisticsFile>,
    ) -> TableMetadata {
        let current = snapshots.iter().map(|s| s.snapshot_id()).max().unwrap_or(0);
        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: None,
            location: "s3://bucket/tbl".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 0,
            last_column_id: 0,
            schemas: vec![schema()],
            current_schema_id: 0,
            partition_specs: vec![PartitionSpec::unpartitioned_spec()],
            default_spec_id: 0,
            last_partition_id: 0,
            properties: HashMap::new(),
            current_snapshot_id: (current > 0).then_some(current),
            next_row_id: None,
            encryption_keys: vec![],
            snapshots,
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![],
            default_sort_order_id: None,
            refs,
            statistics,
            partition_statistics,
        }
    }

    async fn put(store: &dyn ObjectStore, path: &str, bytes: &[u8]) {
        store
            .put(
                &Path::from(path),
                object_store::PutPayload::from(bytes.to_vec()),
            )
            .await
            .unwrap();
    }

    /// Writes dummy bytes at the store location resolved for `raw` (absolute URL or
    /// relative path), so `delete_files` can find and delete it.
    async fn put_data_file(store: &StoreContext, raw: &str) {
        let (store_ref, path) = store.resolve(raw).unwrap();
        put(store_ref.as_ref(), path.as_ref(), b"x").await;
    }

    /// Whether a file exists at the store location resolved for `raw`.
    async fn exists(store: &StoreContext, raw: &str) -> bool {
        let (store_ref, path) = store.resolve(raw).unwrap();
        store_ref.get(&path).await.is_ok()
    }

    /// Builds a store with the given manifest list + manifest and returns a `StoreContext`.
    async fn store_with_table_url() -> StoreContext {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        StoreContext::new(store, &Url::parse("memory://bucket/tbl").unwrap()).unwrap()
    }

    /// Writes a manifest (with the given data files) and a manifest list referencing it.
    async fn write_snapshot_files(
        store: &StoreContext,
        snapshot_id: i64,
        manifest_path: &str,
        manifest_list_path: &str,
        files: &[(String, DataContentType)],
    ) -> (ManifestFile, String) {
        let entries: Vec<crate::spec::ManifestEntry> = files
            .iter()
            .map(|(path, content)| {
                crate::spec::ManifestEntry::new(
                    ManifestStatus::Added,
                    Some(snapshot_id),
                    Some(snapshot_id),
                    Some(snapshot_id),
                    data_file(path, *content),
                )
            })
            .collect();
        let manifest = Manifest::new(
            ManifestMetadata::new(
                Arc::new(schema()),
                0,
                PartitionSpec::unpartitioned_spec(),
                FormatVersion::V2,
                ManifestContentType::Data,
            ),
            entries,
        );
        let manifest_bytes = manifest.to_avro_bytes_v2().unwrap();
        put(store.prefixed.as_ref(), manifest_path, &manifest_bytes).await;

        let mf = manifest_file(manifest_path, files.len() as i32);
        let mut writer = ManifestListWriter::new();
        writer.append(mf.clone());
        let list_bytes = writer.to_bytes(FormatVersion::V2).unwrap();
        put(store.prefixed.as_ref(), manifest_list_path, &list_bytes).await;

        (mf, manifest_list_path.to_string())
    }

    #[test]
    fn diff_returns_only_files_unique_to_candidates() {
        let candidates = vec![
            (
                "s3://b/data-1.parquet".to_string(),
                FileKind::Data(DataContentType::Data),
            ),
            (
                "s3://b/data-2.parquet".to_string(),
                FileKind::Data(DataContentType::Data),
            ),
            ("s3://b/m1.avro".to_string(), FileKind::Manifest),
        ];
        let valid = vec![
            (
                "s3://b/data-2.parquet".to_string(),
                FileKind::Data(DataContentType::Data),
            ),
            ("s3://b/m1.avro".to_string(), FileKind::Manifest),
        ];
        let to_delete = diff_files(candidates, valid);
        assert_eq!(to_delete.len(), 1);
        assert_eq!(to_delete[0].0, "s3://b/data-1.parquet");
        assert_eq!(to_delete[0].1.tag(), "DATA");
    }

    #[test]
    fn diff_dedupes_and_ignores_same_path_different_kind() {
        let candidates = vec![
            ("s3://b/x".to_string(), FileKind::Manifest),
            ("s3://b/x".to_string(), FileKind::Manifest),
        ];
        let valid = vec![("s3://b/x".to_string(), FileKind::Manifest)];
        let to_delete = diff_files(candidates, valid);
        assert!(to_delete.is_empty());

        // Same path with a different kind is a different file.
        let candidates = vec![("s3://b/x".to_string(), FileKind::Statistics)];
        let valid = vec![("s3://b/x".to_string(), FileKind::Manifest)];
        let to_delete = diff_files(candidates, valid);
        assert_eq!(to_delete.len(), 1);
        assert_eq!(to_delete[0].1.tag(), "Statistics Files");
    }

    #[tokio::test]
    async fn expire_files_gc_deletes_expired_only_files() {
        let store = store_with_table_url().await;

        // Snapshot 1 (to be expired) owns data-1 + manifest m1 + manifest list snap-1.
        let (_, list1) = write_snapshot_files(
            &store,
            1,
            "metadata/m1.avro",
            "metadata/snap-1.avro",
            &[(
                "s3://bucket/data-1.parquet".to_string(),
                DataContentType::Data,
            )],
        )
        .await;
        put_data_file(&store, "s3://bucket/data-1.parquet").await;
        // Snapshot 2 (retained) owns data-2 + manifest m2 + manifest list snap-2.
        let (_, list2) = write_snapshot_files(
            &store,
            2,
            "metadata/m2.avro",
            "metadata/snap-2.avro",
            &[(
                "s3://bucket/data-2.parquet".to_string(),
                DataContentType::Data,
            )],
        )
        .await;
        put_data_file(&store, "s3://bucket/data-2.parquet").await;

        let snap1 = snapshot(1, &list1);
        let snap2 = snapshot(2, &list2);
        let refs = HashMap::from([(
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
        let pre = metadata(
            vec![snap1.clone(), snap2.clone()],
            refs.clone(),
            vec![],
            vec![],
        );
        let post = metadata(vec![snap2], refs, vec![], vec![]);

        let counts = expire_files_gc(&store, &pre, &post).await.unwrap();
        assert_eq!(counts.data_files, 1);
        assert_eq!(counts.manifest_files, 1);
        assert_eq!(counts.manifest_lists, 1);
        assert_eq!(counts.position_delete_files, 0);
        assert_eq!(counts.equality_delete_files, 0);
        assert_eq!(counts.statistics_files, 0);

        // Expired snapshot's files are gone; retained snapshot's files survive.
        assert!(!exists(&store, "metadata/snap-1.avro").await);
        assert!(!exists(&store, "metadata/m1.avro").await);
        assert!(!exists(&store, "s3://bucket/data-1.parquet").await);
        assert!(exists(&store, "metadata/snap-2.avro").await);
        assert!(exists(&store, "metadata/m2.avro").await);
        assert!(exists(&store, "s3://bucket/data-2.parquet").await);
    }

    #[tokio::test]
    async fn expire_files_gc_never_deletes_shared_manifest_content() {
        let store = store_with_table_url().await;

        // Both snapshots reference the SAME manifest m1 which contains data-1 and data-2.
        let (_, list1) = write_snapshot_files(
            &store,
            1,
            "metadata/m1.avro",
            "metadata/snap-1.avro",
            &[
                (
                    "s3://bucket/data-1.parquet".to_string(),
                    DataContentType::Data,
                ),
                (
                    "s3://bucket/data-2.parquet".to_string(),
                    DataContentType::Data,
                ),
            ],
        )
        .await;
        let (_, list2) = write_snapshot_files(
            &store,
            2,
            "metadata/m1.avro",
            "metadata/snap-2.avro",
            &[
                (
                    "s3://bucket/data-1.parquet".to_string(),
                    DataContentType::Data,
                ),
                (
                    "s3://bucket/data-2.parquet".to_string(),
                    DataContentType::Data,
                ),
            ],
        )
        .await;

        let snap1 = snapshot(1, &list1);
        let snap2 = snapshot(2, &list2);
        let refs = HashMap::from([(
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
        let pre = metadata(
            vec![snap1.clone(), snap2.clone()],
            refs.clone(),
            vec![],
            vec![],
        );
        let post = metadata(vec![snap2], refs, vec![], vec![]);

        let counts = expire_files_gc(&store, &pre, &post).await.unwrap();
        // Snapshot 1's manifest list is unique to it, but the shared manifest and its
        // content files are still required by snapshot 2.
        assert_eq!(counts.data_files, 0);
        assert_eq!(counts.manifest_files, 0);
        assert_eq!(counts.manifest_lists, 1);

        assert!(exists(&store, "metadata/m1.avro").await);
        assert!(exists(&store, "metadata/snap-2.avro").await);
        assert!(!exists(&store, "metadata/snap-1.avro").await);
    }

    #[tokio::test]
    async fn expire_files_gc_deletes_statistics_files_of_expired_snapshots() {
        let store = store_with_table_url().await;

        let (_, list1) = write_snapshot_files(
            &store,
            1,
            "metadata/m1.avro",
            "metadata/snap-1.avro",
            &[(
                "s3://bucket/data-1.parquet".to_string(),
                DataContentType::Data,
            )],
        )
        .await;
        let (_, list2) = write_snapshot_files(
            &store,
            2,
            "metadata/m2.avro",
            "metadata/snap-2.avro",
            &[(
                "s3://bucket/data-2.parquet".to_string(),
                DataContentType::Data,
            )],
        )
        .await;

        let snap1 = snapshot(1, &list1);
        let snap2 = snapshot(2, &list2);
        let stats1 = crate::spec::StatisticsFile {
            snapshot_id: 1,
            statistics_path: "metadata/stats-1.bin".to_string(),
            file_size_in_bytes: 10,
            file_footer_size_in_bytes: 5,
            key_metadata: None,
            blob_metadata: vec![],
        };
        let stats2 = crate::spec::StatisticsFile {
            snapshot_id: 2,
            statistics_path: "metadata/stats-2.bin".to_string(),
            file_size_in_bytes: 10,
            file_footer_size_in_bytes: 5,
            key_metadata: None,
            blob_metadata: vec![],
        };
        put(store.prefixed.as_ref(), "metadata/stats-1.bin", b"stats1").await;
        put(store.prefixed.as_ref(), "metadata/stats-2.bin", b"stats2").await;

        let refs = HashMap::from([(
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
        let pre = metadata(
            vec![snap1.clone(), snap2.clone()],
            refs.clone(),
            vec![stats1, stats2.clone()],
            vec![],
        );
        let post = metadata(vec![snap2], refs, vec![stats2], vec![]);

        let counts = expire_files_gc(&store, &pre, &post).await.unwrap();
        assert_eq!(counts.statistics_files, 1);
        assert!(!exists(&store, "metadata/stats-1.bin").await);
        assert!(exists(&store, "metadata/stats-2.bin").await);
    }

    #[tokio::test]
    async fn expire_files_gc_counts_position_and_equality_deletes() {
        let store = store_with_table_url().await;

        let (_, list1) = write_snapshot_files(
            &store,
            1,
            "metadata/m1.avro",
            "metadata/snap-1.avro",
            &[
                (
                    "s3://bucket/pos-deletes-1.bin".to_string(),
                    DataContentType::PositionDeletes,
                ),
                (
                    "s3://bucket/eq-deletes-1.bin".to_string(),
                    DataContentType::EqualityDeletes,
                ),
            ],
        )
        .await;
        put_data_file(&store, "s3://bucket/pos-deletes-1.bin").await;
        put_data_file(&store, "s3://bucket/eq-deletes-1.bin").await;
        let (_, list2) = write_snapshot_files(
            &store,
            2,
            "metadata/m2.avro",
            "metadata/snap-2.avro",
            &[(
                "s3://bucket/data-2.parquet".to_string(),
                DataContentType::Data,
            )],
        )
        .await;
        put_data_file(&store, "s3://bucket/data-2.parquet").await;

        let snap1 = snapshot(1, &list1);
        let snap2 = snapshot(2, &list2);
        let refs = HashMap::from([(
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
        let pre = metadata(
            vec![snap1.clone(), snap2.clone()],
            refs.clone(),
            vec![],
            vec![],
        );
        let post = metadata(vec![snap2], refs, vec![], vec![]);

        let counts = expire_files_gc(&store, &pre, &post).await.unwrap();
        assert_eq!(counts.position_delete_files, 1);
        assert_eq!(counts.equality_delete_files, 1);
        assert_eq!(counts.data_files, 0);
    }

    #[tokio::test]
    async fn expire_files_gc_counts_idempotent_delete_of_missing_data_file() {
        let store = store_with_table_url().await;

        // A candidate data file that does not actually exist in the store. Object stores
        // (e.g. S3, InMemory) deletes are idempotent: deleting a missing key succeeds, so
        // it is still counted (count = submitted − failed, and a missing key is not a
        // failure). The strict `NotFound`-skip path only triggers on stores that error.
        let (_, list1) = write_snapshot_files(
            &store,
            1,
            "metadata/m1.avro",
            "metadata/snap-1.avro",
            &[(
                "s3://bucket/ghost.parquet".to_string(),
                DataContentType::Data,
            )],
        )
        .await;
        let (_, list2) = write_snapshot_files(
            &store,
            2,
            "metadata/m2.avro",
            "metadata/snap-2.avro",
            &[(
                "s3://bucket/data-2.parquet".to_string(),
                DataContentType::Data,
            )],
        )
        .await;
        put_data_file(&store, "s3://bucket/data-2.parquet").await;

        let snap1 = snapshot(1, &list1);
        let snap2 = snapshot(2, &list2);
        let refs = HashMap::from([(
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
        let pre = metadata(
            vec![snap1.clone(), snap2.clone()],
            refs.clone(),
            vec![],
            vec![],
        );
        let post = metadata(vec![snap2], refs, vec![], vec![]);

        let counts = expire_files_gc(&store, &pre, &post).await.unwrap();
        // The ghost data file is idempotently deleted (counted); m1/snap-1 also deleted.
        assert_eq!(counts.data_files, 1);
        assert_eq!(counts.manifest_files, 1);
        assert_eq!(counts.manifest_lists, 1);
    }
}

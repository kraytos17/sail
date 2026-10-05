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

//! Classify `LOAD DATA` source files into the fast (zero-copy registration)
//! and fallback (scan + rewrite) paths.

use std::collections::HashMap;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::ObjectStoreExt;
use sail_data_source::listing::utils::list_all_files;
use sail_data_source::{GlobUrl, attach_default_glob, rewrite_directory_url};

use crate::operations::parquet_utils::{ParquetFooterInfo, read_parquet_footer};
use crate::operations::write::base_writer::data_file_writer::aggregate_from_parquet_metadata_with_field_map;
use crate::spec::{DataContentType, DataFile, DataFileFormat, Schema};
use crate::utils::url_to_object_path;

pub(crate) struct ClassifiedFiles {
    pub fast_files: Vec<DataFile>,
    pub fallback_files: Vec<(String, u64)>,
}

/// Split the source files into files that can be registered as-is (schema
/// compatible parquet) and files that must be scanned and rewritten.
pub(crate) async fn classify_source_files(
    ctx: &dyn Session,
    location: &str,
    table_schema: &Schema,
    table_arrow_schema: &ArrowSchema,
    partition_spec_id: i32,
    allow_fast: bool,
) -> DFResult<ClassifiedFiles> {
    let field_id_map: HashMap<String, i32> = table_schema
        .fields()
        .iter()
        .map(|f| (f.name.clone(), f.id))
        .collect();

    let files = resolve_source_files(ctx, location).await?;
    if files.is_empty() {
        return Ok(ClassifiedFiles {
            fast_files: vec![],
            fallback_files: vec![],
        });
    }

    let mut fast_paths: Vec<(String, String, u64)> = Vec::new();
    let mut fallback_files: Vec<(String, u64)> = Vec::new();
    for (key, url, size) in files {
        // Only unpartitioned tables may register files directly: a partitioned
        // table must rewrite so the partition values are derived from the path.
        if allow_fast && url.ends_with(".parquet") {
            fast_paths.push((key, url, size));
        } else {
            fallback_files.push((url, size));
        }
    }

    let max_concurrency = std::thread::available_parallelism()
        .map(|n| n.get() * 4)
        .unwrap_or(16);
    let tasks = fast_paths.into_iter().map(|(key, url, size)| async move {
        // The URLs above came out of our own listing, so parsing cannot fail
        // in practice; a failure still routes the file to rewriting, never to
        // an error, matching the handling of unreadable footers below.
        let footer = match ObjectStoreUrl::parse(&url) {
            Ok(store_url) => match ctx.runtime_env().object_store(&store_url) {
                Ok(store) => read_parquet_footer(&store, &key, size).await,
                Err(e) => Err(format!("failed to resolve object store for {url}: {e}")),
            },
            Err(e) => Err(format!("invalid source file URL '{url}': {e}")),
        };
        (key, url, size, footer)
    });
    let mut results = stream::iter(tasks).buffer_unordered(max_concurrency);

    let mut fast_files: Vec<DataFile> = Vec::new();

    while let Some((key, url, size, footer)) = results.next().await {
        match footer {
            Ok(footer) => {
                if schema_matches(&footer.arrow_schema, table_arrow_schema) {
                    match build_data_file(&url, &footer, &field_id_map, partition_spec_id) {
                        Ok(df) => fast_files.push(df),
                        Err(e) => {
                            log::warn!("Failed to build data file for {url}: {e}; rewriting");
                            fallback_files.push((url, size));
                        }
                    }
                } else {
                    log::debug!("Schema mismatch for {url}; rewriting");
                    fallback_files.push((url, size));
                }
            }
            Err(e) => {
                log::warn!("Failed to read parquet footer for {key}: {e}; rewriting");
                fallback_files.push((url, size));
            }
        }
    }

    Ok(ClassifiedFiles {
        fast_files,
        fallback_files,
    })
}

/// Resolve the source location to `(key, url, size)` triples, sorted by key.
///
/// A concrete path is probed with a single `head`, erroring when it does not
/// exist. Directories and globs go through the shared [`GlobUrl`] machinery —
/// `?`, `[...]` and `{a,b}` patterns, percent-decoding, and Spark
/// hidden-file filtering — so `LOAD DATA` sees exactly the files any other
/// listing read would see.
async fn resolve_source_files(
    ctx: &dyn Session,
    location: &str,
) -> DFResult<Vec<(String, String, u64)>> {
    let mut out = Vec::new();
    for url in GlobUrl::parse(location)? {
        // A concrete file path is probed with a single `head`, erroring when
        // it does not exist. Anything else (an explicit glob, or a bare path
        // ending in `/`) goes through listing, mirroring the previous split
        // between the head-probe branch and the list branch.
        if url.glob.is_none() && !url.base.path().ends_with(object_store::path::DELIMITER) {
            let store = ctx.runtime_env().object_store(&url)?;
            let key = url_to_object_path(&url.base)?;
            match store.head(&key).await {
                Ok(meta) => {
                    out.push((key.to_string(), url.base.to_string(), meta.size));
                }
                Err(e) => {
                    return Err(DataFusionError::External(Box::new(std::io::Error::other(
                        format!("source path does not exist: {location}: {e}"),
                    ))));
                }
            }
            continue;
        }
        let url = rewrite_directory_url(url, ctx).await?;
        let url = attach_default_glob(url)?;
        let base = url.base.clone();
        let listing_url = ListingTableUrl::try_from(url)?;
        let store = ctx.runtime_env().object_store(&listing_url)?;
        let metas = list_all_files(&listing_url, ctx, store.as_ref(), None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        for meta in metas {
            let key = meta.location.to_string();
            let url = base.join(&format!("/{key}")).map_err(|e| {
                DataFusionError::Plan(format!("failed to resolve source URL for {key}: {e}"))
            })?;
            out.push((key, url.to_string(), meta.size));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// Every table column must exist in the file with the same Arrow type for the
/// file to be registered without rewriting. Extra file columns are allowed.
fn schema_matches(file_schema: &ArrowSchema, table_schema: &ArrowSchema) -> bool {
    for field in table_schema.fields() {
        match file_schema.field_with_name(field.name()) {
            Ok(parquet_field) => {
                if parquet_field.data_type() != field.data_type() {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
    true
}

/// Build a registerable [`DataFile`] from a parquet footer.
///
/// Unlike the write path (which reads field ids from the parquet schema), the
/// source file is foreign to the table, so ids are mapped by column name.
fn build_data_file(
    path: &str,
    footer: &ParquetFooterInfo,
    field_id_map: &HashMap<String, i32>,
    partition_spec_id: i32,
) -> Result<DataFile, String> {
    let (column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds, split_offsets) =
        aggregate_from_parquet_metadata_with_field_map(&footer.parquet_metadata, field_id_map)?;

    Ok(DataFile {
        content: DataContentType::Data,
        file_path: path.to_string(),
        file_format: DataFileFormat::Parquet,
        partition: vec![],
        record_count: footer.row_count,
        file_size_in_bytes: footer.file_size,
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts: Default::default(),
        lower_bounds,
        upper_bounds,
        block_size_in_bytes: None,
        key_metadata: None,
        split_offsets,
        equality_ids: vec![],
        sort_order_id: None,
        first_row_id: None,
        partition_spec_id,
        referenced_data_file: None,
        content_offset: None,
        content_size_in_bytes: None,
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::SessionState;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use url::Url;

    use super::*;

    /// Seeds an in-memory store and returns a session resolving
    /// `memory://bucket/` to it, plus each key's byte size.
    async fn fixture(keys: &[&str]) -> (SessionState, std::collections::HashMap<String, u64>) {
        let ctx = SessionContext::new_with_config(SessionConfig::new());
        let store = Arc::new(InMemory::new());
        ctx.register_object_store(&Url::parse("memory://bucket/").unwrap(), store.clone());
        let mut sizes = std::collections::HashMap::new();
        for (i, key) in keys.iter().enumerate() {
            let bytes = vec![i as u8; 10 + i];
            sizes.insert((*key).to_string(), bytes.len() as u64);
            store
                .put(&Path::from(*key), PutPayload::from(bytes))
                .await
                .unwrap();
        }
        (ctx.state(), sizes)
    }

    fn keys_of(files: &[(String, String, u64)]) -> Vec<&str> {
        files.iter().map(|(key, _, _)| key.as_str()).collect()
    }

    fn triple(
        sizes: &std::collections::HashMap<String, u64>,
        key: &str,
        url: &str,
    ) -> (String, String, u64) {
        (key.to_string(), url.to_string(), sizes[key])
    }

    #[tokio::test]
    async fn test_resolve_star_glob() {
        let (state, sizes) = fixture(&[
            "data/a.parquet",
            "data/b.parquet",
            "data/c.csv",
            "data/_temporary/x.parquet",
        ])
        .await;
        let files = resolve_source_files(&state, "memory://bucket/data/*.parquet")
            .await
            .unwrap();
        // `_temporary/x.parquet` is hidden: the shared listing machinery
        // excludes it where the old suffix filter kept it.
        assert_eq!(
            files,
            vec![
                triple(&sizes, "data/a.parquet", "memory://bucket/data/a.parquet"),
                triple(&sizes, "data/b.parquet", "memory://bucket/data/b.parquet"),
            ]
        );
    }

    #[tokio::test]
    async fn test_resolve_question_mark_glob() {
        let (state, _) = fixture(&["data-1.parquet", "data-12.parquet", "data-1.csv"]).await;
        let files = resolve_source_files(&state, "memory://bucket/data-?.parquet")
            .await
            .unwrap();
        assert_eq!(keys_of(&files), vec!["data-1.parquet"]);
    }

    #[tokio::test]
    async fn test_resolve_character_class_glob() {
        let (state, _) = fixture(&["a.parquet", "a.csv", "ab.parquet", "a.txt"]).await;
        let files = resolve_source_files(&state, "memory://bucket/a.[pc]*")
            .await
            .unwrap();
        assert_eq!(keys_of(&files), vec!["a.csv", "a.parquet"]);
    }

    #[tokio::test]
    async fn test_resolve_alternation_glob() {
        let (state, _) = fixture(&["a.parquet", "b.parquet", "c.parquet"]).await;
        let files = resolve_source_files(&state, "memory://bucket/{a,b}.parquet")
            .await
            .unwrap();
        assert_eq!(keys_of(&files), vec!["a.parquet", "b.parquet"]);
    }

    #[tokio::test]
    async fn test_resolve_bare_directory_excludes_hidden_files() {
        let (state, _) = fixture(&[
            "data/a.parquet",
            "data/_SUCCESS",
            "data/.hidden",
            "data/_temporary/x.parquet",
            "data/sub/b.parquet",
        ])
        .await;
        let files = resolve_source_files(&state, "memory://bucket/data/")
            .await
            .unwrap();
        assert_eq!(
            keys_of(&files),
            vec!["data/a.parquet", "data/sub/b.parquet"]
        );
    }

    #[tokio::test]
    async fn test_resolve_bare_file() {
        let (state, sizes) = fixture(&["a.parquet"]).await;
        let files = resolve_source_files(&state, "memory://bucket/a.parquet")
            .await
            .unwrap();
        assert_eq!(
            files,
            vec![triple(&sizes, "a.parquet", "memory://bucket/a.parquet")]
        );
    }

    #[tokio::test]
    async fn test_resolve_missing_bare_path_errors() {
        let (state, _) = fixture(&["a.parquet"]).await;
        let error = resolve_source_files(&state, "memory://bucket/nope.parquet")
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("source path does not exist"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn test_resolve_percent_encoded_names() {
        let (state, sizes) = fixture(&["my dir/a.parquet", "my dir/b.csv"]).await;
        let files = resolve_source_files(&state, "memory://bucket/my%20dir/*.parquet")
            .await
            .unwrap();
        assert_eq!(keys_of(&files), vec!["my dir/a.parquet"]);
        assert_eq!(files[0].2, sizes["my dir/a.parquet"]);
        assert!(
            files[0].1.contains("my%20dir/a.parquet"),
            "unexpected URL: {}",
            files[0].1
        );
    }
}

use std::sync::Arc;

use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::listing::helpers::expr_applicable_for_cols;
use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::logical_expr::Expr;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, GetExt, Result, internal_datafusion_err, plan_err};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_session::Session;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::listing::source::ListingFileSample;
use crate::url::PathGlobFilter;

pub fn rewrite_utf8view_fields(schema: Arc<Schema>) -> Arc<Schema> {
    // TODO: Spark doesn't support Utf8View
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if matches!(field.data_type(), &DataType::Utf8View) {
                field.as_ref().clone().with_data_type(DataType::Utf8)
            } else {
                field.as_ref().clone()
            }
        })
        .collect();

    Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ))
}

fn ends_with_ignore_ascii_case(s: &str, suffix: &str) -> bool {
    s.len() >= suffix.len()
        && s.as_bytes()[s.len() - suffix.len()..].eq_ignore_ascii_case(suffix.as_bytes())
}

/// Infer file-level compression from file names.
///
/// This function returns a concrete compression (including "uncompressed") when *all* sampled files
/// end with the same compression suffix, or [`None`] if the file sample is empty.
/// This function returns an error when sampled files contain a mix of compressed and uncompressed
/// files or multiple compression types.
pub fn infer_listing_compression(
    files: &[ListingFileSample<'_>],
) -> Result<Option<CompressionTypeVariant>> {
    let mut inferred: Option<CompressionTypeVariant> = None;
    for group in files {
        for object in &group.objects {
            let path = object.location.as_ref();
            let compression = [
                CompressionTypeVariant::GZIP,
                CompressionTypeVariant::BZIP2,
                CompressionTypeVariant::XZ,
                CompressionTypeVariant::ZSTD,
            ]
            .into_iter()
            .find(|variant| {
                let ext = FileCompressionType::from(*variant).get_ext();
                ends_with_ignore_ascii_case(path, &ext)
            })
            .unwrap_or(CompressionTypeVariant::UNCOMPRESSED);

            match inferred {
                None => inferred = Some(compression),
                Some(x) if x == compression => {}
                Some(_) => return plan_err!("found mixed compression types"),
            }
        }
    }

    Ok(inferred)
}

/// List up to 10 files per URL into in-memory groups, suitable for schema inference, compression
/// inference, and partition inference.
///
/// File extensions are intentionally ignored since `ListingTableUrl` carries the filtering glob
/// already, and Spark reads every non-hidden file regardless of extension.
pub async fn sample_listing_files<'a>(
    ctx: &dyn Session,
    urls: &'a [ListingTableUrl],
    path_glob_filter: Option<&'a PathGlobFilter>,
) -> Result<Vec<ListingFileSample<'a>>> {
    // Per-URL listings are independent; overlap them while preserving URL order.
    let samples = futures::future::join_all(urls.iter().map(|url| async move {
        let store = ctx.runtime_env().object_store(url)?;
        // Sampling only needs a handful of files; skip cache population here
        // and let the execution path populate it with the full listing.
        let objects: Vec<_> = list_all_files(url, ctx, store.as_ref(), path_glob_filter, false)
            .await?
            // Empty files can't contribute to schema / partition inference and may error when read.
            .try_filter(|meta| futures::future::ready(meta.size > 0))
            .take(10)
            .try_collect()
            .await?;
        Ok::<_, DataFusionError>(ListingFileSample {
            url,
            store,
            objects,
        })
    }))
    .await;
    samples.into_iter().collect()
}

pub fn validate_partitions(
    files: &[ListingFileSample<'_>],
    table_partition_fields: &[FieldRef],
) -> Result<()> {
    if table_partition_fields.is_empty() {
        return Ok(());
    }
    let inferred = infer_partitions(files)?;
    if inferred.is_empty() {
        return Ok(());
    }

    for group in files {
        if !group.url.is_collection() {
            return plan_err!(
                "Can't create a partitioned table backed by a single file, \
            perhaps the URL is missing a trailing slash?"
            );
        }

        let table_partition_names = table_partition_fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();

        if inferred.len() < table_partition_names.len() {
            return plan_err!(
                "Inferred partitions to be {:?}, but got {:?}",
                inferred,
                table_partition_names
            );
        }

        // Match prefix to allow creating tables with partial partitions.
        for (idx, col) in table_partition_names.iter().enumerate() {
            if inferred.get(idx) != Some(col) {
                return plan_err!(
                    "Inferred partitions to be {:?}, but got {:?}",
                    inferred,
                    table_partition_names
                );
            }
        }
    }
    Ok(())
}

pub fn infer_partitions(files: &[ListingFileSample<'_>]) -> Result<Vec<String>> {
    let mut inferred: Option<Vec<String>> = None;
    for group in files {
        for file in &group.objects {
            let path_parts = group
                .url
                .strip_prefix(&file.location)
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "failed to strip listing prefix from object location: {}",
                        file.location
                    )
                })?
                .collect::<Vec<_>>();

            let keys = path_parts
                .into_iter()
                .rev()
                .skip(1) // get parents only and skip the file itself
                .rev()
                .filter(|s| s.contains('='))
                .map(|s| s.split('=').next().unwrap_or("").to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>();

            match &mut inferred {
                None => inferred = Some(keys),
                Some(x) if x == &keys => {}
                Some(x) => {
                    return plan_err!("found mixed partition values {x:?} and {keys:?}");
                }
            }
        }
    }

    Ok(inferred.unwrap_or_default())
}

pub async fn list_all_files<'a>(
    url: &'a ListingTableUrl,
    ctx: &'a dyn Session,
    store: &'a dyn ObjectStore,
    path_glob_filter: Option<&'a PathGlobFilter>,
    populate_cache: bool,
) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
    let exec_options = &ctx.config_options().execution;
    let ignore_subdirectory = exec_options.listing_table_ignore_subdirectory;
    // Narrow the LIST prefix with the literal run of the glob remainder.
    let list_prefix = listing_prefix(url);
    // If the prefix is a file, use a head request, otherwise use a list request.
    let list = match url.is_collection() {
        true => match ctx.runtime_env().cache_manager.get_list_files_cache() {
            None => store.list(Some(&list_prefix)),
            Some(cache) => {
                // Key by the narrowed prefix: different globs narrow differently,
                // and a subset listing must never satisfy a wider cache key.
                let key = TableScopedPath {
                    table: None,
                    path: list_prefix.clone(),
                };
                if let Some(res) = cache.get(&key) {
                    debug!("Hit list all files cache");
                    stream_cached_list(res.files)
                } else if populate_cache {
                    let list_res = store.list(Some(&list_prefix));
                    let vec = list_res.try_collect::<Vec<ObjectMeta>>().await?;
                    let files = Arc::new(vec);
                    cache.put(
                        &key,
                        CachedFileList {
                            files: Arc::clone(&files),
                        },
                    );
                    stream_cached_list(files)
                } else {
                    // Sampling only needs a handful of files; stream without
                    // paying a full enumeration plus a full allocation for the
                    // cache. The execution path populates the cache instead.
                    store.list(Some(&list_prefix))
                }
            }
        },
        false => futures::stream::once(store.head(url.prefix())).boxed(),
    };
    Ok(list
        .try_filter(move |meta| {
            let path = &meta.location;
            let included = url.contains(path, ignore_subdirectory)
                && !has_hidden_path_component(url, path)
                && matches_path_glob_filter(path_glob_filter, path);
            futures::future::ready(included)
        })
        .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
        .boxed())
}

/// Extend a listing prefix with the leading literal run of the glob remainder.
///
/// Every file matching the glob starts with these bytes, so listing under the
/// extended prefix returns exactly the same matches with fewer keys scanned
/// (e.g. `dt=2024-*` narrows `b` to `b/dt=2024-`, `part-*` narrows `b` to
/// `b/part-`). Both the prefix (`Path`) and the compiled pattern live in
/// percent-decoded space, so concatenation is exact. Stopping at the first
/// `*`, `?`, or `[` without tracking escapes or bracket nesting can only
/// yield a shorter run, which prunes less but is never wrong.
///
/// Note the run never contains `/`: the remainder starts at the first segment
/// holding a wildcard, so any `/` inside the run would close a fully literal
/// segment that `split_glob_path` would already have folded into the prefix.
fn listing_prefix(url: &ListingTableUrl) -> Path {
    let run = url
        .get_glob()
        .as_ref()
        .map(|glob| {
            let pattern = glob.as_str();
            let end = pattern.find(['*', '?', '[']).unwrap_or(pattern.len());
            &pattern[..end]
        })
        .unwrap_or("");
    if run.is_empty() {
        url.prefix().clone()
    } else {
        // `Path` strips trailing delimiters, so re-add the separator here.
        let base = url.prefix().as_ref().trim_end_matches('/');
        if base.is_empty() {
            Path::from(run)
        } else {
            Path::from(format!("{base}/{run}"))
        }
    }
}

/// Stream an `Arc`-held file list as owned items without cloning the backing
/// allocation. Short-circuiting consumers (`take(n)`) stop cloning early.
fn stream_cached_list(
    files: Arc<Vec<ObjectMeta>>,
) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
    futures::stream::iter(0..files.len())
        .map(move |i| Ok(files[i].clone()))
        .boxed()
}

pub fn matches_path_glob_filter(
    path_glob_filter: Option<&PathGlobFilter>,
    location: &Path,
) -> bool {
    path_glob_filter.is_none_or(|filter| {
        location
            .filename()
            .is_some_and(|filename| filter.matches(filename))
    })
}

/// Returns `true` if the path is hidden per Spark's `HadoopFSUtils.shouldFilterOutPathName`.
pub fn has_hidden_path_component(url: &ListingTableUrl, location: &Path) -> bool {
    let is_hidden = |name: &str| {
        let exclude = (name.starts_with('_') && !name.contains('='))
            || name.starts_with('.')
            || name.ends_with("._COPYING_");
        let keep = name.starts_with("_common_metadata") || name.starts_with("_metadata");
        exclude && !keep
    };
    url.strip_prefix(location)
        .is_some_and(|mut segments| segments.any(is_hidden))
        || location.filename().is_some_and(is_hidden)
}

pub fn can_be_evaluated_for_partition_pruning(
    partition_column_names: &[&str],
    expr: &Expr,
) -> bool {
    !partition_column_names.is_empty() && expr_applicable_for_cols(partition_column_names, expr)
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listing_prefix_extends_static_prefix_with_literal_run() {
        use sail_common_datafusion::utils::items::ItemTaker;

        use crate::url::GlobUrl;

        // Build through Sail's own glob pipeline (`GlobUrl`), which is what
        // `resolve_listing_urls` feeds into `ListingTableUrl`.
        let prefix_of = |url: &str| {
            let glob_url = GlobUrl::parse(url).unwrap().one().unwrap();
            let url = ListingTableUrl::try_from(glob_url).unwrap();
            listing_prefix(&url).as_ref().to_string()
        };

        // Literal runs narrow the LIST prefix; matching is unchanged.
        // (`Path` strips trailing delimiters, so `b/` parses to prefix `b`.)
        assert_eq!(prefix_of("file:///b/dt=2024-*/*.parquet"), "b/dt=2024-");
        assert_eq!(prefix_of("file:///b/part-*.parquet"), "b/part-");
        assert_eq!(
            prefix_of("file:///b/year=2024/month=0?/*.parquet"),
            "b/year=2024/month=0"
        );
        // No literal run: prefix untouched.
        assert_eq!(prefix_of("file:///b/*.parquet"), "b");
        assert_eq!(prefix_of("file:///b/[ab]*.parquet"), "b");
        // No glob at all: prefix untouched.
        assert_eq!(prefix_of("file:///b/"), "b");
        assert_eq!(prefix_of("file:///b/f.parquet"), "b/f.parquet");
    }

    #[test]
    fn test_has_hidden_path_component() {
        let dir = ListingTableUrl::parse("file:///data/").unwrap();
        let hidden = |path: &str| has_hidden_path_component(&dir, &Path::from(path));

        // Data files and partition directories are kept.
        assert!(!hidden("data/part-0.parquet"));
        assert!(!hidden("data/year=2020/part-0.parquet"));

        // Hidden markers and hidden directories are excluded.
        assert!(hidden("data/_SUCCESS"));
        assert!(hidden("data/.hidden.json"));
        assert!(hidden("data/_temporary/0/part-0.parquet"));
        assert!(hidden("data/visible/_hidden/bad.json"));

        // Files mid-copy (Hadoop `._COPYING_`) are excluded.
        assert!(hidden("data/part-0.parquet._COPYING_"));

        // Spark keeps the Parquet summary files.
        assert!(!hidden("data/_metadata"));
        assert!(!hidden("data/_common_metadata"));

        // Spark keeps `_`-prefixed partition directories (they contain `=`).
        assert!(!hidden("data/_part=1/part-0.parquet"));

        // A hidden listing root is not itself filtered.
        let hidden_root = ListingTableUrl::parse("file:///_root/").unwrap();
        assert!(!has_hidden_path_component(
            &hidden_root,
            &Path::from("_root/part-0.parquet")
        ));

        // A location outside the prefix falls back to judging its file name.
        assert!(!hidden("outside/part-0.parquet"));
        assert!(hidden("outside/_SUCCESS"));

        // An explicitly targeted hidden file is excluded.
        let file = ListingTableUrl::parse("file:///data/_data.json").unwrap();
        assert!(has_hidden_path_component(
            &file,
            &Path::from("data/_data.json")
        ));
    }
}

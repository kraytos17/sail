use std::any::Any;
use std::collections::HashSet;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{LogicalPlan, TableScan};
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result};
use futures::TryStreamExt;
use url::Url;

use crate::listing::table::ListingTableSource;
use crate::listing::utils::list_all_files;

/// Deduplicated, percent-encoded URIs of the files composing `plan` (Spark's `DataFrame.inputFiles`).
pub async fn input_files(ctx: &SessionContext, plan: LogicalPlan) -> Result<Vec<String>> {
    // Optimize first, like Spark, so eliminated scans (e.g. `WHERE false`) contribute no files.
    let state = ctx.state();
    let plan = state.optimize(&plan)?;

    // Collect the listing table sources referenced anywhere in the plan.
    let mut listing_sources: Vec<ListingTableSource> = vec![];
    plan.apply(|node| {
        if let LogicalPlan::TableScan(TableScan { source, .. }) = node {
            // Upcast to `Any` to downcast to the concrete source.
            let source: &dyn Any = source.as_ref();
            if let Some(listing) = source.downcast_ref::<ListingTableSource>() {
                listing_sources.push(listing.clone());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut files: Vec<String> = vec![];
    // List each (path, filter) once so a repeated scan (e.g. `df.union(df)`) isn't re-listed.
    let mut listed: HashSet<(String, Option<String>)> = HashSet::new();

    // Serial dedup pass first so repeated scans still list exactly once.
    let mut work = vec![];
    for source in &listing_sources {
        let path_glob_filter = source.config().path_glob_filter.as_ref();
        for table_path in &source.config().table_paths {
            if !listed.insert((
                table_path.as_str().to_string(),
                path_glob_filter.map(|filter| filter.as_str().to_string()),
            )) {
                continue;
            }
            work.push((table_path, path_glob_filter));
        }
    }
    // Per-path listings are independent; overlap them while preserving order.
    // `state_ref` is a shared reference so each future can hold it at once.
    let state_ref = &state;
    let listed_metas = futures::future::join_all(work.iter().map(
        |(table_path, path_glob_filter)| async move {
            let store = ctx.runtime_env().object_store(table_path)?;
            let base = Url::parse(table_path.object_store().as_str())
                .map_err(|e| DataFusionError::Internal(format!("invalid object store URL: {e}")))?;
            let metas = list_all_files(
                table_path,
                state_ref,
                store.as_ref(),
                *path_glob_filter,
                true,
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;
            Ok::<_, DataFusionError>((base, metas))
        },
    ))
    .await;
    for result in listed_metas {
        let (base, metas) = result?;
        for meta in metas {
            // Percent-encode the path, as Spark returns encoded URIs.
            let mut uri = base.clone();
            uri.path_segments_mut()
                .map_err(|()| {
                    DataFusionError::Internal("object store URL cannot be a base".to_string())
                })?
                .clear()
                .extend(meta.location.parts().map(|part| part.as_ref().to_string()));
            files.push(uri.to_string());
        }
    }

    // Deduplicate, matching Spark's semantics.
    files.sort();
    files.dedup();

    Ok(files)
}

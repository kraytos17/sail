use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::{DataFusionError, Result as DFResult};
use futures::stream::{self, StreamExt};
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

use crate::operations::parquet_utils::{ParquetFooterInfo, read_parquet_footer};
use crate::operations::write::base_writer::data_file_writer::aggregate_from_parquet_metadata_with_field_map;
use crate::spec::{DataContentType, DataFile, DataFileFormat, Schema};
use crate::utils::url_to_object_path;

pub(crate) struct ClassifiedFiles {
    pub fast_files: Vec<DataFile>,
    pub fallback_files: Vec<(String, u64)>,
}

pub(crate) async fn classify_source_files(
    object_store: Arc<dyn ObjectStore>,
    source_url: &Url,
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

    let files = resolve_source_files(object_store.as_ref(), source_url, location).await?;
    if files.is_empty() {
        return Ok(ClassifiedFiles {
            fast_files: vec![],
            fallback_files: vec![],
        });
    }

    let mut fast_paths: Vec<(String, String, u64)> = Vec::new();
    let mut fallback_files: Vec<(String, u64)> = Vec::new();
    for (key, url, size) in files {
        if allow_fast && url.ends_with(".parquet") {
            fast_paths.push((key, url, size));
        } else {
            fallback_files.push((url, size));
        }
    }

    let max_concurrency = std::thread::available_parallelism()
        .map(|n| n.get() * 4)
        .unwrap_or(16);
    let tasks = fast_paths.into_iter().map(|(key, url, size)| {
        let store = Arc::clone(&object_store);
        async move {
            let footer = read_parquet_footer(&store, &key).await;
            (key, url, size, footer)
        }
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

async fn resolve_source_files(
    store: &dyn ObjectStore,
    source_url: &Url,
    location: &str,
) -> DFResult<Vec<(String, String, u64)>> {
    if !location.ends_with('/') && !location.contains('*') {
        let key = url_to_object_path(source_url)?;
        match store.head(&key).await {
            Ok(meta) => {
                return Ok(vec![(key.to_string(), source_url.to_string(), meta.size)]);
            }
            Err(e) => {
                return Err(DataFusionError::External(Box::new(std::io::Error::other(
                    format!("source path does not exist: {location}: {e}"),
                ))));
            }
        }
    }

    let (prefix, suffix_filter) = split_glob(location);
    let prefix_url = url::Url::parse(&prefix)
        .map_err(|e| DataFusionError::Plan(format!("invalid source location '{prefix}': {e}")))?;
    let prefix_key = url_to_object_path(&prefix_url)?;

    let mut out = Vec::new();
    let mut stream = store.list(Some(&prefix_key));
    while let Some(item) = stream.next().await {
        let meta = item.map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(e.to_string())))
        })?;
        let key = meta.location.to_string();
        if suffix_filter
            .as_deref()
            .map_or(true, |sfx| key.ends_with(sfx))
        {
            let url = prefix_url.join(&format!("/{key}")).map_err(|e| {
                DataFusionError::Plan(format!("failed to resolve source URL for {key}: {e}"))
            })?;
            out.push((key, url.to_string(), meta.size));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn split_glob(location: &str) -> (String, Option<String>) {
    if let Some(pos) = location.find('*') {
        let head = &location[..pos];
        let cut = head.rfind('/').map(|i| i + 1).unwrap_or(0);
        let prefix = location[..cut].to_string();
        let suffix = location[pos..].trim_start_matches('*').to_string();
        let suffix = if suffix.is_empty() {
            None
        } else {
            Some(suffix)
        };
        (prefix, suffix)
    } else {
        let mut prefix = location.to_string();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }
        (prefix, None)
    }
}

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

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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::{DataFusionError, Result as DFResult};
use futures::stream::{self, StreamExt};
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

use crate::operations::parquet_utils::{read_parquet_footer, ParquetFooterInfo};
use crate::operations::write::base_writer::data_file_writer::aggregate_from_parquet_metadata_with_field_map;
use crate::spec::{DataContentType, DataFile, DataFileFormat, Schema};
use crate::utils::url_to_object_path;

/// Result of classifying source files into fast-register (parquet, schema-match) and
/// fallback (rewrite) files.
pub(crate) struct ClassifiedFiles {
    /// Data files built directly from parquet footers (registered without rewrite).
    pub fast_files: Vec<DataFile>,
    /// Source paths that must be read + rewritten (csv/json, or schema mismatch), as
    /// `(full URL, size in bytes)` so the fallback scan can plan on real file sizes.
    pub fallback_files: Vec<(String, u64)>,
    /// Sum of row counts across all source files.
    pub total_rows: u64,
}

/// Classify the source file(s) referenced by `location` into fast-register vs fallback.
///
/// v1:
/// - `.parquet` with a schema that name+type-matches the table's current schema → fast.
/// - everything else (`.csv`, `.json`, mismatched parquet) → fallback.
///
/// The classification is done at plan time; footer reads are parallel (bounded by a
/// semaphore). Unpartitioned tables only in v1 — `partition_spec_id` is used as-is.
pub(crate) async fn classify_source_files(
    object_store: Arc<dyn ObjectStore>,
    source_url: &Url,
    location: &str,
    table_schema: &Schema,
    table_arrow_schema: &ArrowSchema,
    partition_spec_id: i32,
    allow_fast: bool,
) -> DFResult<ClassifiedFiles> {
    // Build column-name → Iceberg field-ID map (external parquet has no field IDs).
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
            total_rows: 0,
        });
    }

    // Split by extension: only parquet is a fast-path candidate. When `allow_fast` is false
    // (partitioned tables), every file goes through the rewrite fallback so partition values
    // are computed by the writer.
    let mut fast_paths: Vec<(String, String, u64)> = Vec::new();
    let mut fallback_files: Vec<(String, u64)> = Vec::new();
    for (key, url, size) in files {
        if allow_fast && url.ends_with(".parquet") {
            fast_paths.push((key, url, size));
        } else {
            fallback_files.push((url, size));
        }
    }

    // Parallel footer reads with bounded concurrency.
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
    let mut total_rows: u64 = 0;

    while let Some((key, url, size, footer)) = results.next().await {
        match footer {
            Ok(footer) => {
                total_rows += footer.row_count;
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
        total_rows,
    })
}

/// Resolve a source location into `(object key, full URL, size in bytes)` triples.
///
/// The object store is already bound to the source URL's bucket, so the returned key is
/// relative to that store (for `head`/`list`/footer reads), while the full URL is what
/// Iceberg records as the data-file path.
///
/// - A single file path is validated to exist via `head`.
/// - A directory (`…/path/`) or glob (`…/path/*.parquet`) is expanded via object-store
///   `list` and filtered to regular files.
async fn resolve_source_files(
    store: &dyn ObjectStore,
    source_url: &Url,
    location: &str,
) -> DFResult<Vec<(String, String, u64)>> {
    if !location.ends_with('/') && !location.contains('*') {
        // Single file: verify it exists.
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

    // Directory or glob: strip the wildcard to get a listing prefix, then filter.
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
            // The listed key is relative to the store root and already includes the
            // listing prefix; join against the URL's origin (absolute-path reference).
            let url = prefix_url.join(&format!("/{key}")).map_err(|e| {
                DataFusionError::Plan(format!("failed to resolve source URL for {key}: {e}"))
            })?;
            out.push((key, url.to_string(), meta.size));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// Split a location into a listing prefix and an optional suffix filter.
///
/// `s3a://bucket/dir/*.parquet` → `(s3a://bucket/dir/, Some(".parquet"))`
/// `s3a://bucket/dir/` → `(s3a://bucket/dir/, None)`
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
        // directory: ensure trailing slash
        let mut prefix = location.to_string();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }
        (prefix, None)
    }
}

/// Check whether the parquet file's arrow schema name+type-matches the table's arrow schema.
///
/// Every table column must be present in the parquet file with the same type; extra
/// parquet columns are allowed (they are dropped at registration). A table column missing
/// from the file forces a fallback.
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

/// Build an Iceberg `DataFile` from a parquet footer.
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
mod tests {
    use bytes::Bytes;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use object_store::memory::InMemory;
    use parquet::arrow::async_writer::AsyncArrowWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::spec::types::{PrimitiveType, Type};
    use crate::spec::NestedField;

    fn test_iceberg_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .with_schema_id(0)
            .build()
            .unwrap()
    }

    fn test_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ])
    }

    async fn write_parquet_to_store(
        store: &Arc<dyn ObjectStore>,
        path: &str,
        batch: &RecordBatch,
    ) -> Result<(), String> {
        let props = WriterProperties::builder().build();
        let mut writer = AsyncArrowWriter::try_new(Vec::new(), batch.schema(), Some(props))
            .map_err(|e| e.to_string())?;
        writer.write(batch).await.map_err(|e| e.to_string())?;
        let _metadata = writer.finish().await.map_err(|e| e.to_string())?;
        let buf = writer.into_inner();
        let bytes = Bytes::from(buf);
        store
            .put(
                &object_store::path::Path::from(path),
                object_store::PutPayload::from(bytes),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn classifies_matching_parquet_as_fast() -> Result<(), String> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let batch = RecordBatch::try_new(
            Arc::new(test_arrow_schema()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .map_err(|e| e.to_string())?;
        write_parquet_to_store(&store, "data.parquet", &batch).await?;

        let classified = classify_source_files(
            store,
            &Url::parse("memory://bucket/data.parquet").unwrap(),
            "memory://bucket/data.parquet",
            &test_iceberg_schema(),
            &test_arrow_schema(),
            0,
            true,
        )
        .await
        .map_err(|e| e.to_string())?;

        assert_eq!(classified.fallback_files.len(), 0);
        assert_eq!(classified.fast_files.len(), 1);
        assert_eq!(classified.total_rows, 3);

        let df = &classified.fast_files[0];
        // DataFile path is the FULL URL (what Iceberg records), not a relative key.
        assert_eq!(df.file_path, "memory://bucket/data.parquet");
        assert_eq!(df.record_count, 3);
        assert_eq!(df.file_format, DataFileFormat::Parquet);
        assert_eq!(df.partition_spec_id, 0);
        // id=1, name=2 → both should have stats.
        assert!(df.column_sizes.contains_key(&1));
        assert!(df.column_sizes.contains_key(&2));
        Ok(())
    }

    #[tokio::test]
    async fn mismatched_schema_goes_to_fallback() -> Result<(), String> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .map_err(|e| e.to_string())?;
        write_parquet_to_store(&store, "data.parquet", &batch).await?;

        let classified = classify_source_files(
            store,
            &Url::parse("memory://bucket/data.parquet").unwrap(),
            "memory://bucket/data.parquet",
            &test_iceberg_schema(),
            &test_arrow_schema(),
            0,
            true,
        )
        .await
        .map_err(|e| e.to_string())?;

        // `name` column missing → fallback.
        assert_eq!(classified.fast_files.len(), 0);
        assert_eq!(classified.fallback_files.len(), 1);
        assert_eq!(
            classified.fallback_files[0].0,
            "memory://bucket/data.parquet"
        );
        assert!(classified.fallback_files[0].1 > 0);
        Ok(())
    }

    #[tokio::test]
    async fn csv_file_goes_to_fallback() -> Result<(), String> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &object_store::path::Path::from("data.csv"),
                object_store::PutPayload::from("id,name\n1,a\n2,b\n"),
            )
            .await
            .map_err(|e| e.to_string())?;

        let classified = classify_source_files(
            store,
            &Url::parse("memory://bucket/data.csv").unwrap(),
            "memory://bucket/data.csv",
            &test_iceberg_schema(),
            &test_arrow_schema(),
            0,
            true,
        )
        .await
        .map_err(|e| e.to_string())?;

        assert_eq!(classified.fast_files.len(), 0);
        assert_eq!(classified.fallback_files.len(), 1);
        assert_eq!(classified.fallback_files[0].0, "memory://bucket/data.csv");
        Ok(())
    }

    #[tokio::test]
    async fn directory_lists_parquet_files_with_full_urls() -> Result<(), String> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let batch = RecordBatch::try_new(
            Arc::new(test_arrow_schema()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .map_err(|e| e.to_string())?;
        write_parquet_to_store(&store, "loads/data1.parquet", &batch).await?;
        write_parquet_to_store(&store, "loads/data2.parquet", &batch).await?;

        let classified = classify_source_files(
            store,
            &Url::parse("memory://bucket/loads/").unwrap(),
            "memory://bucket/loads/",
            &test_iceberg_schema(),
            &test_arrow_schema(),
            0,
            true,
        )
        .await
        .map_err(|e| e.to_string())?;

        assert_eq!(classified.fallback_files.len(), 0);
        assert_eq!(classified.fast_files.len(), 2);
        assert_eq!(classified.total_rows, 4);
        let mut paths = classified
            .fast_files
            .iter()
            .map(|df| df.file_path.clone())
            .collect::<Vec<_>>();
        paths.sort();
        assert_eq!(
            paths,
            vec![
                "memory://bucket/loads/data1.parquet".to_string(),
                "memory://bucket/loads/data2.parquet".to_string()
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn glob_filters_parquet_files() -> Result<(), String> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let batch = RecordBatch::try_new(
            Arc::new(test_arrow_schema()),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .map_err(|e| e.to_string())?;
        write_parquet_to_store(&store, "loads/a.parquet", &batch).await?;
        write_parquet_to_store(&store, "loads/b.parquet", &batch).await?;
        write_parquet_to_store(&store, "loads/c.txt", &batch).await?;

        let classified = classify_source_files(
            store,
            &Url::parse("memory://bucket/loads/").unwrap(),
            "memory://bucket/loads/*.parquet",
            &test_iceberg_schema(),
            &test_arrow_schema(),
            0,
            true,
        )
        .await
        .map_err(|e| e.to_string())?;

        assert_eq!(classified.fast_files.len(), 2);
        assert_eq!(classified.total_rows, 2);
        assert!(classified
            .fast_files
            .iter()
            .all(|df| df.file_path.starts_with("memory://bucket/loads/")));
        Ok(())
    }

    #[test]
    fn glob_splitting() {
        assert_eq!(
            split_glob("s3a://bucket/dir/*.parquet"),
            (
                "s3a://bucket/dir/".to_string(),
                Some(".parquet".to_string())
            )
        );
        assert_eq!(
            split_glob("s3a://bucket/dir/"),
            ("s3a://bucket/dir/".to_string(), None)
        );
        assert_eq!(
            split_glob("s3a://bucket/dir/"),
            ("s3a://bucket/dir/".to_string(), None)
        );
    }

    #[tokio::test]
    async fn partitioned_tables_disable_fast_path() -> Result<(), String> {
        // Partitioned tables force every file through the rewrite fallback so the writer
        // computes partition values (an empty partition tuple would be invalid otherwise).
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let batch = RecordBatch::try_new(
            Arc::new(test_arrow_schema()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .map_err(|e| e.to_string())?;
        write_parquet_to_store(&store, "data.parquet", &batch).await?;

        let classified = classify_source_files(
            store,
            &Url::parse("memory://bucket/data.parquet").unwrap(),
            "memory://bucket/data.parquet",
            &test_iceberg_schema(),
            &test_arrow_schema(),
            1,     // partition_spec_id for a partitioned table
            false, // allow_fast = false
        )
        .await
        .map_err(|e| e.to_string())?;

        assert_eq!(classified.fast_files.len(), 0);
        assert_eq!(classified.fallback_files.len(), 1);
        assert_eq!(
            classified.fallback_files[0].0,
            "memory://bucket/data.parquet"
        );
        assert_eq!(classified.total_rows, 0);
        Ok(())
    }
}

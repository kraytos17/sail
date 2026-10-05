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

//! Physical planning for `LOAD DATA ... INTO TABLE <iceberg table>`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{JsonSource, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning};
use datafusion_common::GetExt;
use datafusion_common::config::{CsvOptions, TableParquetOptions};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use sail_common_datafusion::catalog::CatalogPartitionField;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;
use sail_data_source::formats::csv::CsvSource;
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::load_data::LoadDataNode;

use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::lake_source::{
    IcebergLakeSource, catalog_managed_iceberg_from_options, metadata_location_from_options,
    resolve_iceberg_metadata_location, split_iceberg_write_options_and_table_properties,
};
use crate::operations::SnapshotUpdateKind;
use crate::options::r#gen::IcebergWriteOptions;
use crate::physical::load_classifier::classify_source_files;
use crate::physical_plan::plan_builder::repartition_for_write;
use crate::physical_plan::{
    IcebergCommitExec, IcebergLoadDataFastExec, IcebergWriterExec, IcebergWriterExecOptions,
    prepare_iceberg_write_context,
};
use crate::spec::TableRequirement;
use crate::table::Table;
use crate::utils::url_to_object_path;

/// Upper bound on scan parallelism for the LOAD fallback path.
///
/// The scan would otherwise fan out to one partition per source file, and
/// every partition issues its own concurrent object-store reads. Capping the
/// fan-out bounds the concurrent stream count against the store.
///
/// Kept explicit rather than inheriting `target_partitions` so a future bump
/// to the session default cannot silently widen the LOAD fan-out again.
const LOAD_SCAN_MAX_PARTITIONS: usize = 8;

pub(crate) async fn plan_load_data(
    session: &dyn Session,
    node: &LoadDataNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table_url =
        IcebergLakeSource::parse_table_url(vec![node.target_location().to_string()]).await?;

    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let (clean_options, table_properties) =
        split_iceberg_write_options_and_table_properties(node.target_options().to_vec())?;
    let metadata_location_for_load = resolve_iceberg_metadata_location(
        node.target_lakehouse_table(),
        metadata_location,
        catalog_managed_table,
    )?;
    let table =
        Table::load_with_metadata_location(session, table_url.clone(), metadata_location_for_load)
            .await?;

    // Row lineage needs no action here. For V3 the commit assigns per-file
    // `first_row_id` from the table's next row id (`materialize_inherited_entry`
    // in `operations/snapshot.rs`), skipping any file that already carries one.
    // Below V3 there is no lineage at all. The classifier therefore leaves
    // `first_row_id` as `None` on purpose: filling it in at plan time would make
    // the commit skip the file and leave a gap in the numbering.
    let metadata = table.metadata();
    let requirements = vec![
        TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: metadata.last_column_id,
        },
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id,
        },
    ];

    let table_schema = metadata.current_schema().ok_or_else(|| {
        DataFusionError::Plan("LOAD DATA: table has no current schema".to_string())
    })?;
    let table_arrow_schema = iceberg_schema_to_arrow(table_schema)?;
    let default_spec = metadata.default_partition_spec();
    let spec_id = default_spec.map(|s| s.spec_id()).unwrap_or(0);
    let partitioned = default_spec.is_some_and(|spec| !spec.fields().is_empty());

    let classified = classify_source_files(
        session,
        node.location(),
        table_schema,
        &table_arrow_schema,
        spec_id,
        // A partitioned table must rewrite so partition values come from the path.
        !partitioned,
    )
    .await?;

    let snapshot_update_kind = if node.overwrite() {
        SnapshotUpdateKind::FullOverwrite
    } else {
        SnapshotUpdateKind::FastAppend
    };

    if classified.fallback_files.is_empty() {
        let fast_exec: Arc<dyn ExecutionPlan> = Arc::new(IcebergLoadDataFastExec::new(
            classified.fast_files,
            table_url.clone(),
            requirements,
            table_properties,
            node.target_lakehouse_table().cloned(),
        ));

        return Ok(Arc::new(IcebergCommitExec::new(
            fast_exec,
            table_url,
            node.target_lakehouse_table().cloned(),
            snapshot_update_kind,
        )));
    }

    let partition_columns = IcebergLakeSource::partition_columns_from_metadata(&table)?;

    let mut branches: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

    if !classified.fast_files.is_empty() {
        let fast_exec: Arc<dyn ExecutionPlan> = Arc::new(IcebergLoadDataFastExec::new(
            classified.fast_files,
            table_url.clone(),
            requirements.clone(),
            table_properties.clone(),
            node.target_lakehouse_table().cloned(),
        ));
        branches.push(fast_exec);
    }

    // The writer side mirrors `resolve_row_level_writer_options`: resolve the
    // split-off write options, then stamp the commit-owned table properties
    // and lakehouse context onto the exec options.
    let variant_shredding_option_presence =
        IcebergWriterExecOptions::variant_shredding_option_presence(&clean_options);
    let iceberg_options = IcebergWriteOptions::resolve(session, clean_options)?;
    let mut writer_options = IcebergWriterExecOptions::from(iceberg_options);
    writer_options.apply_variant_shredding_option_presence(variant_shredding_option_presence);
    writer_options.table_properties = table_properties.clone();
    writer_options.lakehouse_table = node.target_lakehouse_table().cloned();

    let write_context = prepare_iceberg_write_context(
        &table_url,
        Some(metadata),
        &writer_options,
        &partition_columns,
        &PhysicalSinkMode::Append,
        &table_arrow_schema,
    )?;

    for ((format, compression), files) in group_by_format(&classified.fallback_files) {
        let scan = build_fallback_scan(
            session,
            &files,
            format.as_str(),
            compression,
            &table_arrow_schema,
        )?;
        let scan = repartition_scan_for_load(scan, &partition_columns)?;

        let writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergWriterExec::new(
            scan,
            table_url.clone(),
            partition_columns.clone(),
            PhysicalSinkMode::Append,
            true,
            writer_options.clone(),
            write_context.clone(),
        )?);
        branches.push(writer);
    }

    let union = UnionExec::try_new(branches)?;
    let commit_input = Arc::new(CoalescePartitionsExec::new(union));

    Ok(Arc::new(IcebergCommitExec::new(
        commit_input,
        table_url,
        node.target_lakehouse_table().cloned(),
        snapshot_update_kind,
    )))
}

type ScanBucket = ((String, CompressionTypeVariant), Vec<(String, u64)>);

/// Bucket fallback files by `(format, compression)`: one `FileScanConfig`
/// carries a single compression type, so files that differ in either dimension
/// must not share a scan.
fn group_by_format(files: &[(String, u64)]) -> Vec<ScanBucket> {
    let mut groups: HashMap<(String, CompressionTypeVariant), Vec<(String, u64)>> = HashMap::new();
    for (f, size) in files {
        let lower = f.to_ascii_lowercase();
        let ext = if lower.ends_with(".csv") {
            "csv"
        } else if lower.ends_with(".json") || lower.ends_with(".jsonl") {
            "json"
        } else if lower.ends_with(".parquet") {
            "parquet"
        } else {
            "csv"
        };
        groups
            .entry((ext.to_string(), infer_source_compression(f)))
            .or_default()
            .push((f.clone(), *size));
    }
    groups.into_iter().collect()
}

fn infer_source_compression(path: &str) -> CompressionTypeVariant {
    let lower = path.to_ascii_lowercase();
    for variant in [
        CompressionTypeVariant::GZIP,
        CompressionTypeVariant::BZIP2,
        CompressionTypeVariant::XZ,
        CompressionTypeVariant::ZSTD,
    ] {
        let ext = FileCompressionType::from(variant).get_ext();
        if lower.ends_with(&ext) {
            return variant;
        }
    }
    CompressionTypeVariant::UNCOMPRESSED
}

fn build_fallback_scan(
    session: &dyn Session,
    files: &[(String, u64)],
    format: &str,
    compression: CompressionTypeVariant,
    table_schema: &ArrowSchema,
) -> Result<Arc<dyn ExecutionPlan>> {
    let parsed_url = url::Url::parse(&files[0].0)
        .map_err(|e| DataFusionError::Plan(format!("invalid file URL: {e}")))?;
    let store_url_str = &parsed_url[..url::Position::BeforePath];
    let object_store_url = ObjectStoreUrl::parse(store_url_str)
        .map_err(|e| DataFusionError::Plan(format!("invalid object store URL: {e}")))?;

    // No plan-time chunking (deferred with `12` bounded ranges): one group per
    // file. Compressed sources decline byte-range splitting outright — a range
    // inside a compressed stream cannot be decoded — and uncompressed files
    // read whole until chunking lands.
    let file_groups: Vec<FileGroup> = files
        .iter()
        .map(|(path, size)| {
            let parsed = url::Url::parse(path)
                .map_err(|e| DataFusionError::Plan(format!("invalid file URL: {e}")))?;
            let key = url_to_object_path(&parsed)?;
            Ok(FileGroup::new(vec![PartitionedFile::new(
                key.to_string(),
                *size,
            )]))
        })
        .collect::<Result<_>>()?;

    let source: Arc<dyn FileSource> = match format {
        "csv" => {
            // Header default follows the session catalog config, mirroring the
            // listing CSV read path.
            let csv_options = CsvOptions {
                has_header: Some(session.config_options().catalog.has_header),
                newlines_in_values: Some(session.config_options().catalog.newlines_in_values),
                ..Default::default()
            };
            Arc::new(CsvSource::new(Arc::new(table_schema.clone())).with_csv_options(csv_options)?)
        }
        "json" => Arc::new(JsonSource::new(Arc::new(table_schema.clone()))),
        "parquet" => {
            // Mirror the provider/listing scan path: session parquet options,
            // a cached footer reader, and the (name-based) schema-evolution
            // adapter for files whose schema differs from the table schema.
            let parquet_options = TableParquetOptions {
                global: session.config_options().execution.parquet.clone(),
                ..Default::default()
            };
            let store = session
                .runtime_env()
                .object_store(object_store_url.clone())?;
            let metadata_cache = session
                .runtime_env()
                .cache_manager
                .get_file_metadata_cache();
            let reader_factory =
                Arc::new(CachedParquetFileReaderFactory::new(store, metadata_cache));
            Arc::new(
                ParquetSource::new(Arc::new(table_schema.clone()))
                    .with_table_parquet_options(parquet_options)
                    .with_parquet_file_reader_factory(reader_factory),
            )
        }
        _ => {
            return plan_err!("unsupported fallback format: {format}");
        }
    };

    let config = FileScanConfigBuilder::new(object_store_url, source)
        .with_file_groups(file_groups)
        // Compression is decided by `group_by_format`, which buckets by
        // `(extension, compression)`; every file in `files` therefore shares it.
        .with_file_compression_type(FileCompressionType::from(compression))
        .with_expr_adapter(Some(Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {})))
        .build();

    let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
    // Every partition issues its own concurrent object-store reads, so cap the
    // fan-out: one group per file would otherwise scale partitions with the
    // glob width.
    if scan.output_partitioning().partition_count() > LOAD_SCAN_MAX_PARTITIONS {
        Ok(Arc::new(RepartitionExec::try_new(
            scan,
            Partitioning::RoundRobinBatch(LOAD_SCAN_MAX_PARTITIONS),
        )?))
    } else {
        Ok(scan)
    }
}

fn repartition_scan_for_load(
    scan: Arc<dyn ExecutionPlan>,
    partition_columns: &[CatalogPartitionField],
) -> Result<Arc<dyn ExecutionPlan>> {
    if partition_columns.is_empty() {
        Ok(scan)
    } else {
        let names = partition_columns
            .iter()
            .map(|field| field.column.clone())
            .collect::<Vec<_>>();
        repartition_for_write(scan, &names)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
    use datafusion::prelude::SessionConfig;
    use datafusion_common::parsers::CompressionTypeVariant;
    use datafusion_datasource::file_compression_type::FileCompressionType;
    use datafusion_datasource::file_scan_config::FileScanConfig;
    use sail_common_datafusion::catalog::CatalogPartitionField;

    use super::*;

    const UNCOMPRESSED: CompressionTypeVariant = CompressionTypeVariant::UNCOMPRESSED;
    const GZIP: CompressionTypeVariant = CompressionTypeVariant::GZIP;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("p", DataType::Utf8, true),
            Field::new("v", DataType::Utf8, true),
        ])
    }

    fn test_session_state(target_partitions: usize) -> SessionState {
        let config = SessionConfig::new().with_target_partitions(target_partitions);
        SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::new(RuntimeEnv::default()))
            .build()
    }

    fn csv_files(prefix: &str, ext: &str, sizes: &[u64]) -> Vec<(String, u64)> {
        sizes
            .iter()
            .enumerate()
            .map(|(i, size)| (format!("memory:///{prefix}/f{i}{ext}"), *size))
            .collect()
    }

    /// Clone the fallback scan's `FileScanConfig` out for grouping inspection.
    fn scan_config(scan: &Arc<dyn ExecutionPlan>) -> Result<FileScanConfig> {
        use std::any::Any;

        let exec = scan.downcast_ref::<DataSourceExec>().ok_or_else(|| {
            DataFusionError::Plan("fallback scan is not a DataSourceExec".to_string())
        })?;
        let source = exec.data_source().as_ref() as &dyn Any;
        source
            .downcast_ref::<FileScanConfig>()
            .ok_or_else(|| {
                DataFusionError::Plan("fallback data source is not a FileScanConfig".to_string())
            })
            .cloned()
    }

    #[test]
    fn extension_fallback_buckets_unknown_as_csv() {
        let files = vec![
            ("memory:///m/a.txt".to_string(), 100),
            ("memory:///m/b.csv".to_string(), 100),
            ("memory:///m/c.jsonl".to_string(), 100),
            ("memory:///m/d.parquet".to_string(), 100),
        ];
        let mut groups = group_by_format(&files);
        groups.sort_by(|a, b| a.0.0.cmp(&b.0.0));

        // Unknown extensions fall back to the CSV scan; `.jsonl` joins `json`.
        let formats: Vec<&str> = groups.iter().map(|((f, _), _)| f.as_str()).collect();
        assert_eq!(formats, vec!["csv", "json", "parquet"]);
        let csv = groups.iter().find(|((f, _), _)| f == "csv").unwrap();
        assert_eq!(csv.1.len(), 2);
    }

    #[test]
    fn small_csv_files_keep_one_group_per_file() -> Result<()> {
        let state = test_session_state(4);
        let files = csv_files("small", ".csv", &[1_000_000, 2_000_000, 3_000_000]);
        let scan = build_fallback_scan(&state, &files, "csv", UNCOMPRESSED, &test_schema())?;

        // Below the cap: no tiling, one task per file, no cap wrapper.
        assert!(scan.downcast_ref::<RepartitionExec>().is_none());
        assert_eq!(scan.output_partitioning().partition_count(), 3);
        let config = scan_config(&scan)?;
        assert_eq!(config.file_groups.len(), 3);
        for group in &config.file_groups {
            assert_eq!(group.len(), 1);
            for file in group.files() {
                assert!(file.range.is_none(), "small files must not be range-split");
            }
        }
        Ok(())
    }

    #[test]
    fn compressed_csv_files_are_never_range_split() -> Result<()> {
        let state = test_session_state(16);
        let files = csv_files("compressed", ".csv.gz", &[300_000_000, 300_000_000]);
        let scan = build_fallback_scan(&state, &files, "csv", GZIP, &test_schema())?;

        // Compressed sources decline byte-range splitting; each file stays a
        // single whole-file group. (Ranges here would fail at read time.)
        assert_eq!(scan.output_partitioning().partition_count(), 2);
        let config = scan_config(&scan)?;
        assert_eq!(config.file_groups.len(), 2);
        for group in &config.file_groups {
            for file in group.files() {
                assert!(
                    file.range.is_none(),
                    "compressed files must not be range-split"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn fallback_scan_partitions_are_capped() -> Result<()> {
        let state = test_session_state(64);
        let files = csv_files("capped", ".csv", &[1_000_000; 20]);
        let scan = build_fallback_scan(&state, &files, "csv", UNCOMPRESSED, &test_schema())?;

        // The scan must not fan out to the session's full parallelism: every
        // partition issues its own concurrent object-store reads.
        assert!(
            scan.output_partitioning().partition_count() <= LOAD_SCAN_MAX_PARTITIONS,
            "fallback scan parallelism must be capped at {LOAD_SCAN_MAX_PARTITIONS}"
        );
        assert!(
            scan.downcast_ref::<RepartitionExec>().is_some(),
            "wide scans must be wrapped to cap fan-out"
        );
        Ok(())
    }

    #[test]
    fn mixed_compression_directory_splits_into_separate_scans() -> Result<()> {
        let state = test_session_state(4);
        let files = vec![
            ("memory:///mixed/a.csv".to_string(), 1_000_000),
            ("memory:///mixed/b.csv.gz".to_string(), 1_000_000),
            ("memory:///mixed/c.csv".to_string(), 2_000_000),
        ];
        let groups = group_by_format(&files);

        // One `FileScanConfig` carries a single compression type, so plain and
        // gzip members of the same extension must not share a scan.
        assert_eq!(groups.len(), 2, "expected one group per compression");
        let plain = groups
            .iter()
            .find(|((_, c), _)| *c == UNCOMPRESSED)
            .ok_or_else(|| DataFusionError::Plan("missing uncompressed group".to_string()))?;
        let gzip = groups
            .iter()
            .find(|((_, c), _)| *c == GZIP)
            .ok_or_else(|| DataFusionError::Plan("missing gzip group".to_string()))?;
        assert_eq!(plain.1.len(), 2);
        assert_eq!(gzip.1.len(), 1);

        // Each scan then reports the compression of its own bucket.
        let plain_scan = build_fallback_scan(&state, &plain.1, "csv", plain.0.1, &test_schema())?;
        let gzip_scan = build_fallback_scan(&state, &gzip.1, "csv", gzip.0.1, &test_schema())?;
        assert_eq!(
            scan_config(&plain_scan)?.file_compression_type,
            FileCompressionType::from(CompressionTypeVariant::UNCOMPRESSED)
        );
        assert_eq!(
            scan_config(&gzip_scan)?.file_compression_type,
            FileCompressionType::from(CompressionTypeVariant::GZIP)
        );
        Ok(())
    }

    #[test]
    fn unpartitioned_load_skips_write_repartition() -> Result<()> {
        let state = test_session_state(4);
        let files = csv_files("unpart", ".csv", &[1_000_000]);
        let scan = build_fallback_scan(&state, &files, "csv", UNCOMPRESSED, &test_schema())?;
        let partitions = scan.output_partitioning().partition_count();

        let planned = repartition_scan_for_load(scan, &[])?;
        assert!(
            planned.downcast_ref::<RepartitionExec>().is_none(),
            "unpartitioned LOAD must not introduce a RepartitionExec shuffle"
        );
        assert_eq!(planned.output_partitioning().partition_count(), partitions);
        Ok(())
    }

    #[test]
    fn partitioned_load_keeps_hash_repartition() -> Result<()> {
        let state = test_session_state(4);
        let files = csv_files("part", ".csv", &[1_000_000]);
        let scan = build_fallback_scan(&state, &files, "csv", UNCOMPRESSED, &test_schema())?;

        let partition_columns = vec![CatalogPartitionField {
            column: "p".to_string(),
            transform: None,
        }];
        let planned = repartition_scan_for_load(scan, &partition_columns)?;
        assert!(
            planned.downcast_ref::<RepartitionExec>().is_some(),
            "partitioned LOAD must keep the hash repartition for partition colocation"
        );
        Ok(())
    }
}

use std::sync::Arc;

use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::Result as DFResult;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, FileSource};
use datafusion::execution::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, GetExt};
use datafusion_datasource::file_compression_type::FileCompressionType;
use sail_common_datafusion::catalog::CatalogPartitionField;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::load_data::LoadDataNode;

use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::options::r#gen::IcebergWriteOptions;
use crate::physical::load_classifier::classify_source_files;
use crate::physical_plan::write_context::prepare_iceberg_write_context;
use crate::physical_plan::{
    IcebergCommitExec, IcebergLoadDataFastExec, IcebergWriterExec, IcebergWriterExecOptions,
};
use crate::spec::TableRequirement;
use crate::table::Table;
use crate::table_format::{
    IcebergTableFormat, catalog_managed_iceberg_from_options, metadata_location_from_options,
    split_iceberg_write_options_and_table_properties,
};
use crate::utils::partition_transform::catalog_partition_field_from_iceberg;
use crate::utils::{get_object_store_from_session, url_to_object_path};

pub async fn plan_load_data(
    session_state: &SessionState,
    node: &LoadDataNode,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let (clean_options, table_properties) =
        split_iceberg_write_options_and_table_properties(node.target_options().to_vec())?;
    let variant_shredding_option_presence =
        IcebergWriterExecOptions::variant_shredding_option_presence(&clean_options);
    let options = IcebergWriteOptions::resolve(session_state, clean_options)?;

    let table_url =
        IcebergTableFormat::parse_table_url(vec![node.target_location().to_string()]).await?;

    let metadata_location_resolved = catalog_managed_table
        .then(|| metadata_location.clone())
        .flatten();
    let table = Table::load_with_metadata_location(
        session_state,
        table_url.clone(),
        metadata_location_resolved,
    )
    .await?;
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
    let spec_id = metadata
        .default_partition_spec()
        .map(|s| s.spec_id())
        .unwrap_or(0);

    let partitioned = metadata
        .default_partition_spec()
        .map(|spec| !spec.fields().is_empty())
        .unwrap_or(false);

    let glob_cut = node.location().find('*').unwrap_or(node.location().len());
    let source_url = url::Url::parse(&node.location()[..glob_cut]).map_err(|e| {
        DataFusionError::Plan(format!(
            "invalid source location '{}': {e}",
            node.location()
        ))
    })?;
    let source_store = get_object_store_from_session(session_state, &source_url)?;
    let classified = classify_source_files(
        source_store,
        &source_url,
        node.location(),
        table_schema,
        &table_arrow_schema,
        spec_id,
        /* allow_fast = */ !partitioned,
    )
    .await?;

    let fast_files = classified.fast_files;
    let fallback_files = classified.fallback_files;

    let partition_columns: Vec<CatalogPartitionField> = match metadata.default_partition_spec() {
        Some(spec) => spec
            .fields()
            .iter()
            .map(|f| catalog_partition_field_from_iceberg(f.name.clone(), f.transform))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(DataFusionError::Plan)?,
        None => Vec::new(),
    };

    let snapshot_update_kind = if node.overwrite() {
        crate::operations::SnapshotUpdateKind::FullOverwrite
    } else {
        crate::operations::SnapshotUpdateKind::FastAppend
    };

    if fallback_files.is_empty() {
        let fast_exec: Arc<dyn ExecutionPlan> = Arc::new(IcebergLoadDataFastExec::new(
            fast_files,
            table_url.clone(),
            requirements,
            table_properties,
            node.target_lakehouse_table().cloned(),
        ));

        return Ok(Arc::new(IcebergCommitExec::new(
            fast_exec,
            table_url.clone(),
            node.target_lakehouse_table().cloned(),
            snapshot_update_kind,
        )));
    }

    let mut branches: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

    if !fast_files.is_empty() {
        let fast_exec: Arc<dyn ExecutionPlan> = Arc::new(IcebergLoadDataFastExec::new(
            fast_files,
            table_url.clone(),
            requirements.clone(),
            table_properties.clone(),
            node.target_lakehouse_table().cloned(),
        ));
        branches.push(fast_exec);
    }

    let mut writer_options = IcebergWriterExecOptions::from(options);
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

    for (format, files) in group_by_format(&fallback_files) {
        let scan =
            build_fallback_scan(session_state, &files, format.as_str(), &table_arrow_schema)?;
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

    let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(branches)?;
    let commit_input = Arc::new(CoalescePartitionsExec::new(union));

    Ok(Arc::new(IcebergCommitExec::new(
        commit_input,
        table_url.clone(),
        node.target_lakehouse_table().cloned(),
        snapshot_update_kind,
    )))
}

fn group_by_format(files: &[(String, u64)]) -> Vec<(String, Vec<(String, u64)>)> {
    // Group by (extension, compression): a FileScanConfig carries a single
    // compression type, so mixed-compression directories need separate scans.
    let mut groups: std::collections::HashMap<
        (String, CompressionTypeVariant),
        Vec<(String, u64)>,
    > = std::collections::HashMap::new();
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
    groups
        .into_iter()
        .map(|((ext, _), files)| (ext, files))
        .collect()
}

/// Colocate the fallback scan for the write only when the table is partitioned.
///
/// Unpartitioned tables skip the extra repartition: `build_fallback_scan` already
/// tiles source bytes across `target_partitions` via DataFusion's
/// `FileGroupPartitioner`, so each writer task reads a balanced slice without a
/// full shuffle collapsing write parallelism. Partitioned tables keep the
/// canonical hash repartition so each writer task owns a stable slice of
/// partition values.
fn repartition_scan_for_load(
    scan: Arc<dyn ExecutionPlan>,
    partition_columns: &[CatalogPartitionField],
) -> DFResult<Arc<dyn ExecutionPlan>> {
    if partition_columns.is_empty() {
        Ok(scan)
    } else {
        crate::physical_plan::plan_builder::repartition_for_iceberg_write(scan, partition_columns)
    }
}

fn build_fallback_scan(
    session_state: &SessionState,
    files: &[(String, u64)],
    format: &str,
    table_schema: &datafusion::arrow::datatypes::Schema,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let parsed_url = url::Url::parse(&files[0].0)
        .map_err(|e| DataFusionError::Plan(format!("invalid file URL: {e}")))?;
    let store_url_str = &parsed_url[..url::Position::BeforePath];
    let object_store_url = ObjectStoreUrl::parse(store_url_str)
        .map_err(|e| DataFusionError::Plan(format!("invalid object store URL: {e}")))?;

    // One file group per source file. Byte-range splitting for parallelism is
    // left to DataFusion's `FileGroupPartitioner` (via `repartitioned` below):
    // it tiles total bytes across `target_partitions`, declines compressed or
    // unsplittable sources, and splits each file at most once. Pre-splitting
    // here would only multiply `calculate_range` probes, and for compressed
    // files the surviving ranges would fail at read time
    // ("Reading compressed .csv in parallel is not supported").
    let file_groups: Vec<Vec<PartitionedFile>> = files
        .iter()
        .map(|(path, size)| {
            let parsed = url::Url::parse(path)
                .map_err(|e| DataFusionError::Plan(format!("invalid file URL: {e}")))?;
            let key = url_to_object_path(&parsed)?;
            Ok(vec![PartitionedFile::new(key.to_string(), *size)])
        })
        .collect::<DFResult<_>>()?;

    let source: Arc<dyn FileSource> = match format {
        "csv" => {
            // Sail's CsvSource: projected decoding, lossy UTF-8, byte-range
            // splitting, and projection pushdown over the stock source.
            // Header default follows the session catalog config.
            let has_header = session_state.config_options().catalog.has_header;
            let csv_options =
                datafusion_common::config::CsvOptions::default().with_has_header(has_header);
            Arc::new(
                sail_data_source::formats::csv::CsvSource::new(Arc::new(table_schema.clone()))
                    .with_csv_options(csv_options),
            )
        }
        "json" => Arc::new(datafusion::datasource::physical_plan::JsonSource::new(
            Arc::new(table_schema.clone()),
        )),
        "parquet" => {
            // Mirror the provider/listing scan path: session parquet options,
            // a cached footer reader, and the (name-based) schema-evolution
            // adapter for files whose schema differs from the table schema.
            let parquet_options = TableParquetOptions {
                global: session_state.config_options().execution.parquet.clone(),
                ..Default::default()
            };
            let store = session_state
                .runtime_env()
                .object_store(object_store_url.clone())?;
            let metadata_cache = session_state
                .runtime_env()
                .cache_manager
                .get_file_metadata_cache();
            let reader_factory =
                Arc::new(CachedParquetFileReaderFactory::new(store, metadata_cache));
            Arc::new(
                datafusion::datasource::physical_plan::ParquetSource::new(Arc::new(
                    table_schema.clone(),
                ))
                .with_table_parquet_options(parquet_options)
                .with_parquet_file_reader_factory(reader_factory),
            )
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "unsupported fallback format: {format}"
            )));
        }
    };

    let compression = infer_source_compression(&files[0].0);
    let config = FileScanConfigBuilder::new(object_store_url, source)
        .with_file_groups(file_groups.into_iter().map(FileGroup::new).collect())
        .with_file_compression_type(FileCompressionType::from(compression))
        .with_expr_adapter(Some(Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {})))
        .build();

    let target_partitions = session_state.config().target_partitions().max(1);
    let exec = DataSourceExec::from_data_source(config);
    let scan = match exec.repartitioned(target_partitions, session_state.config_options())? {
        Some(plan) => plan,
        None => exec,
    };

    Ok(scan)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
    use datafusion::prelude::SessionConfig;
    use datafusion_datasource::file_scan_config::FileScanConfig;
    use sail_common_datafusion::catalog::CatalogPartitionField;

    use super::*;

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
    fn scan_config(scan: &Arc<dyn ExecutionPlan>) -> DFResult<FileScanConfig> {
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
    fn small_csv_files_keep_one_group_per_file() -> DFResult<()> {
        let state = test_session_state(4);
        let files = csv_files("small", ".csv", &[1_000_000, 2_000_000, 3_000_000]);
        let scan = build_fallback_scan(&state, &files, "csv", &test_schema())?;

        // Below the partitioner floor: no tiling, one task per file.
        assert_eq!(scan.output_partitioning().partition_count(), 3);
        let config = scan_config(&scan)?;
        assert_eq!(config.file_groups.len(), 3);
        for group in &config.file_groups {
            assert_eq!(group.len(), 1);
            for file in group.iter() {
                assert!(file.range.is_none(), "small files must not be range-split");
            }
        }
        Ok(())
    }

    #[test]
    fn large_compressed_csv_files_are_not_range_split() -> DFResult<()> {
        let state = test_session_state(16);
        let files = csv_files("compressed", ".csv.gz", &[300_000_000, 300_000_000]);
        let scan = build_fallback_scan(&state, &files, "csv", &test_schema())?;

        // Compressed sources decline byte-range splitting; each file stays a
        // single whole-file group. (Ranges here would fail at read time.)
        assert_eq!(scan.output_partitioning().partition_count(), 2);
        let config = scan_config(&scan)?;
        assert_eq!(config.file_groups.len(), 2);
        for group in &config.file_groups {
            for file in group.iter() {
                assert!(
                    file.range.is_none(),
                    "compressed files must not be range-split"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn large_uncompressed_csv_files_tile_to_target_partitions() -> DFResult<()> {
        let state = test_session_state(4);
        let files = csv_files(
            "large",
            ".csv",
            &[1_000_000_000, 1_000_000_000, 1_000_000_000],
        );
        let scan = build_fallback_scan(&state, &files, "csv", &test_schema())?;

        // DataFusion's FileGroupPartitioner tiles total bytes across the
        // target partitions with exact, contiguous coverage.
        assert_eq!(scan.output_partitioning().partition_count(), 4);
        let config = scan_config(&scan)?;
        let mut covered: u64 = 0;
        for group in &config.file_groups {
            for file in group.iter() {
                let len = match &file.range {
                    Some(range) => (range.end - range.start) as u64,
                    None => file.object_meta.size,
                };
                covered += len;
            }
        }
        assert_eq!(covered, 3_000_000_000);
        Ok(())
    }

    #[test]
    fn unpartitioned_load_skips_write_repartition() -> DFResult<()> {
        let state = test_session_state(4);
        let files = csv_files("unpart", ".csv", &[1_000_000]);
        let scan = build_fallback_scan(&state, &files, "csv", &test_schema())?;
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
    fn partitioned_load_keeps_hash_repartition() -> DFResult<()> {
        let state = test_session_state(4);
        let files = csv_files("part", ".csv", &[1_000_000]);
        let scan = build_fallback_scan(&state, &files, "csv", &test_schema())?;

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

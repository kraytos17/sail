use std::sync::Arc;

use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::Result as DFResult;
use datafusion::datasource::listing::PartitionedFile;
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
    let mut groups: std::collections::HashMap<String, Vec<(String, u64)>> =
        std::collections::HashMap::new();
    for (f, size) in files {
        let ext = if f.ends_with(".csv") {
            "csv"
        } else if f.ends_with(".json") || f.ends_with(".jsonl") {
            "json"
        } else if f.ends_with(".parquet") {
            "parquet"
        } else {
            "csv"
        };
        groups
            .entry(ext.to_string())
            .or_default()
            .push((f.clone(), *size));
    }
    groups.into_iter().collect()
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
            let csv_options =
                datafusion_common::config::CsvOptions::default().with_has_header(true);
            Arc::new(
                datafusion::datasource::physical_plan::CsvSource::new(Arc::new(
                    table_schema.clone(),
                ))
                .with_csv_options(csv_options),
            )
        }
        "json" => Arc::new(datafusion::datasource::physical_plan::JsonSource::new(
            Arc::new(table_schema.clone()),
        )),
        "parquet" => Arc::new(datafusion::datasource::physical_plan::ParquetSource::new(
            Arc::new(table_schema.clone()),
        )),
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

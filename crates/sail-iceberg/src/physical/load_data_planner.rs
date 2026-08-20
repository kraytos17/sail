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

use std::sync::Arc;

use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::Result as DFResult;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    CsvSource, FileGroup, FileScanConfigBuilder, FileSource, JsonSource, ParquetSource,
};
use datafusion::execution::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, GetExt};
use datafusion_datasource::file_compression_type::FileCompressionType;
use sail_common_datafusion::catalog::CatalogPartitionField;
use sail_common_datafusion::datasource::{OptionLayer, PhysicalSinkMode};
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::load_data::LoadDataNode;

use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::options::r#gen::IcebergWriteOptions;
use crate::physical::load_classifier::classify_source_files;
use crate::physical_plan::planner::PlannerContext;
use crate::physical_plan::{
    IcebergCommitExec, IcebergLoadDataFastExec, IcebergWriterExec, IcebergWriterExecOptions,
};
use crate::spec::{Operation, TableRequirement};
use crate::table_format::IcebergTableFormat;
use crate::utils::partition_transform::catalog_partition_field_from_iceberg;

pub async fn plan_load_data(
    session_state: &SessionState,
    node: &LoadDataNode,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let options = IcebergWriteOptions::resolve(session_state, node.target_options().to_vec())?;

    let table_url =
        IcebergTableFormat::parse_table_url(vec![node.target_location().to_string()]).await?;

    let ctx = PlannerContext::new(
        session_state,
        options,
        table_url,
        node.target_lakehouse_table().cloned(),
    )
    .await?;

    let metadata = ctx.table().metadata();
    let operation = if node.overwrite() {
        Operation::Overwrite
    } else {
        Operation::Append
    };

    let requirements = vec![
        TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: metadata.last_column_id,
        },
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id,
        },
    ];

    let table_properties: Vec<(String, String)> = node
        .target_options()
        .iter()
        .filter_map(|layer| match layer {
            OptionLayer::TablePropertyList { items } => Some(items.clone()),
            _ => None,
        })
        .flatten()
        .collect();

    // Classify source files: parquet + schema match → fast register; else fallback.
    let table_schema = metadata.current_schema().ok_or_else(|| {
        datafusion::common::DataFusionError::Plan(
            "LOAD DATA: table has no current schema".to_string(),
        )
    })?;
    let table_arrow_schema = iceberg_schema_to_arrow(table_schema)?;
    let spec_id = metadata
        .default_partition_spec()
        .map(|s| s.spec_id())
        .unwrap_or(0);

    // A partitioned table must go through the rewrite fallback: the fast path registers
    // parquet files with empty partition tuples, which is invalid for a non-empty spec.
    let partitioned = metadata
        .default_partition_spec()
        .map(|spec| !spec.fields().is_empty())
        .unwrap_or(false);

    // Resolve the SOURCE object store from the source URL (any bucket), not the table's
    // store. Globs are truncated at the first `*` so the prefix parses as a valid URL.
    let glob_cut = node.location().find('*').unwrap_or(node.location().len());
    let source_url = url::Url::parse(&node.location()[..glob_cut]).map_err(|e| {
        datafusion::common::DataFusionError::Plan(format!(
            "invalid source location '{}': {e}",
            node.location()
        ))
    })?;
    let source_store = crate::utils::get_object_store_from_session(ctx.session(), &source_url)?;
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

    // Extract partition columns from the table metadata (mirror assemble_iceberg_commit_plan).
    let partition_columns: Vec<CatalogPartitionField> = {
        let meta = ctx.table().metadata();
        match meta.default_partition_spec() {
            Some(spec) => spec
                .fields()
                .iter()
                .map(|f| catalog_partition_field_from_iceberg(f.name.clone(), f.transform))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| datafusion::common::DataFusionError::Plan(e))?,
            None => Vec::new(),
        }
    };

    if fallback_files.is_empty() {
        let fast_exec: Arc<dyn ExecutionPlan> = Arc::new(IcebergLoadDataFastExec::new(
            fast_files,
            ctx.table_url().clone(),
            operation,
            requirements,
            table_properties,
            ctx.lakehouse_table().cloned(),
        ));

        return Ok(Arc::new(IcebergCommitExec::new(
            fast_exec,
            ctx.table_url().clone(),
            ctx.lakehouse_table().cloned(),
        )));
    }

    let mut branches: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

    if !fast_files.is_empty() {
        let fast_exec: Arc<dyn ExecutionPlan> = Arc::new(IcebergLoadDataFastExec::new(
            fast_files,
            ctx.table_url().clone(),
            operation.clone(),
            requirements.clone(),
            table_properties.clone(),
            ctx.lakehouse_table().cloned(),
        ));
        branches.push(fast_exec);
    }

    // Build fallback writer branches.
    // Group fallback files by extension; each format group gets ONE multi-partition writer.
    // The scan is fed straight into the writer: `IcebergWriterExec` accepts any input
    // distribution, and `build_fallback_scan` byte-range repartitions large files across
    // `target_partitions`, so big sets get multiple writer partitions while small sets
    // stay single-partition. No explicit repartition is inserted here.
    for (format, files) in group_by_format(&fallback_files) {
        let scan =
            build_fallback_scan(session_state, &files, format.as_str(), &table_arrow_schema)?;
        let writer_options = IcebergWriterExecOptions {
            commit_operation: Some(operation.clone()),
            lakehouse_table: ctx.lakehouse_table().cloned(),
            table_properties: table_properties.clone(),
            ..Default::default()
        };

        let writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergWriterExec::new(
            scan,
            ctx.table_url().clone(),
            partition_columns.clone(),
            PhysicalSinkMode::Append,
            true,
            writer_options,
            Some(Arc::new(table_arrow_schema.clone())),
        ));
        branches.push(writer);
    }

    // IcebergCommitExec is single-partition; gather every writer partition's action batches
    // first. With multiple format groups the writers are unioned, then coalesced.
    let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(branches)?;
    let commit_input = Arc::new(CoalescePartitionsExec::new(union));

    Ok(Arc::new(IcebergCommitExec::new(
        commit_input,
        ctx.table_url().clone(),
        ctx.lakehouse_table().cloned(),
    )))
}

/// Group a list of `(url, size)` fallback files by their extension (csv / json / parquet).
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

/// Build a `DataSourceExec` scan over the given `(url, size)` files for a specific format.
fn build_fallback_scan(
    session_state: &SessionState,
    files: &[(String, u64)],
    format: &str,
    table_schema: &datafusion::arrow::datatypes::Schema,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    // Derive the object store URL from the first file path.
    let parsed_url = url::Url::parse(&files[0].0)
        .map_err(|e| DataFusionError::Plan(format!("invalid file URL: {e}")))?;
    let store_url_str = &parsed_url[..url::Position::BeforePath];
    let object_store_url = ObjectStoreUrl::parse(store_url_str)
        .map_err(|e| DataFusionError::Plan(format!("invalid object store URL: {e}")))?;

    // One file group per file as the baseline. `PartitionedFile` paths are relative to the
    // store bound by `object_store_url` (mirror scan_by_data_files_exec), and carry the
    // real source size so `repartitioned` below can plan accurate byte ranges.
    let file_groups: Vec<Vec<PartitionedFile>> = files
        .iter()
        .map(|(path, size)| {
            let parsed = url::Url::parse(path)
                .map_err(|e| DataFusionError::Plan(format!("invalid file URL: {e}")))?;
            let key = crate::utils::url_to_object_path(&parsed)?;
            Ok(vec![PartitionedFile::new(key.to_string(), *size)])
        })
        .collect::<DFResult<_>>()?;

    let source: Arc<dyn FileSource> = match format {
        "csv" => {
            let csv_options =
                datafusion_common::config::CsvOptions::default().with_has_header(true);
            Arc::new(CsvSource::new(Arc::new(table_schema.clone())).with_csv_options(csv_options))
        }
        "json" => Arc::new(JsonSource::new(Arc::new(table_schema.clone()))),
        "parquet" => Arc::new(ParquetSource::new(Arc::new(table_schema.clone()))),
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

    // Byte-range repartition large files across the session's target partitions so the
    // fallback read + rewrite uses more parallelism than one task per file. DataFusion's
    // `DataSourceExec::repartitioned` (FileGroupPartitioner) splits the largest files
    // first and the CSV opener line-aligns chunk boundaries; it declines small (<
    // `optimizer.repartition_file_min_size`) and compressed inputs, keeping one task per
    // file in those cases. Formats that cannot be split safely gate via
    // `FileSource::supports_repartitioning` (e.g. CSV with `newlines_in_values`).
    let target_partitions = session_state.config().target_partitions().max(1);
    let exec = DataSourceExec::from_data_source(config);
    let scan = match exec.repartitioned(target_partitions, session_state.config_options())? {
        Some(plan) => plan,
        None => exec,
    };

    Ok(scan)
}

/// Infer the compression type of a source file from its extension.
///
/// Mirrors `sail-data-source`'s `infer_listing_compression` but operates on a single
/// path string (no listing samples). Unknown/absent extensions → uncompressed.
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
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::physical_plan::FileGroupPartitioner;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::prelude::SessionConfig;

    use super::*;

    const MB: u64 = 1024 * 1024;

    fn string_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ])
    }

    fn fallback_files(urls: &[(&str, u64)]) -> Vec<(String, u64)> {
        urls.iter().map(|(u, s)| (u.to_string(), *s)).collect()
    }

    fn session_with_parallelism(target_partitions: usize) -> SessionState {
        SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(target_partitions),
        )
        .state()
    }

    /// Mirror the file-group construction inside `build_fallback_scan` and run it through
    /// DataFusion's `FileGroupPartitioner` to assert the byte-range tiling contract.
    fn repartitioned_groups(
        files: &[(String, u64)],
        target_partitions: usize,
        min_size: usize,
    ) -> Vec<FileGroup> {
        let file_groups: Vec<FileGroup> = files
            .iter()
            .map(|(path, size)| {
                let parsed = url::Url::parse(path).unwrap();
                let key = crate::utils::url_to_object_path(&parsed).unwrap();
                FileGroup::new(vec![PartitionedFile::new(key.to_string(), *size)])
            })
            .collect();
        FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(min_size)
            .repartition_file_groups(&file_groups)
            .unwrap_or(file_groups)
    }

    #[tokio::test]
    async fn splits_large_csv_files_across_target_partitions() {
        let state = session_with_parallelism(4);
        let files = fallback_files(&[
            ("s3a://bucket/landing/gl_balances/a.csv", 400 * MB),
            ("s3a://bucket/landing/gl_balances/b.csv", 400 * MB),
        ]);
        let plan = build_fallback_scan(&state, &files, "csv", &string_schema()).unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 4);
    }

    #[test]
    fn repartitioned_groups_tile_file_bytes_exactly() {
        let files = fallback_files(&[
            ("s3a://bucket/landing/gl_balances/a.csv", 400 * MB),
            ("s3a://bucket/landing/gl_balances/b.csv", 400 * MB),
        ]);
        let groups = repartitioned_groups(&files, 4, 10 * MB as usize);

        assert_eq!(groups.len(), 4);
        for group in &groups {
            assert_eq!(group.len(), 1);
            assert!(group.files()[0].range.is_some());
        }

        // Each source file's chunks must tile [0, size) exactly, in order.
        for (path, size) in &files {
            let parsed = url::Url::parse(path).unwrap();
            let key = crate::utils::url_to_object_path(&parsed)
                .unwrap()
                .to_string();
            let mut chunks: Vec<&PartitionedFile> = groups
                .iter()
                .flat_map(FileGroup::iter)
                .filter(|f| f.object_meta.location.to_string() == key)
                .collect();
            chunks.sort_by_key(|f| f.range.as_ref().unwrap().start);
            assert!(!chunks.is_empty());
            assert_eq!(chunks[0].range.as_ref().unwrap().start, 0);
            assert_eq!(
                chunks.last().unwrap().range.as_ref().unwrap().end,
                *size as i64
            );
            for window in chunks.windows(2) {
                assert_eq!(
                    window[0].range.as_ref().unwrap().end,
                    window[1].range.as_ref().unwrap().start
                );
            }
        }
    }

    #[tokio::test]
    async fn keeps_single_partition_for_small_files() {
        let state = session_with_parallelism(4);
        let files = fallback_files(&[
            ("s3a://bucket/landing/gl_balances/a.csv", 4 * MB),
            ("s3a://bucket/landing/gl_balances/b.csv", 4 * MB),
        ]);
        let plan = build_fallback_scan(&state, &files, "csv", &string_schema()).unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 2);
    }

    #[tokio::test]
    async fn does_not_split_compressed_csv() {
        let state = session_with_parallelism(4);
        let files = fallback_files(&[
            ("s3a://bucket/landing/gl_balances/a.csv.gz", 400 * MB),
            ("s3a://bucket/landing/gl_balances/b.csv.gz", 400 * MB),
        ]);
        let plan = build_fallback_scan(&state, &files, "csv", &string_schema()).unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 2);
    }

    #[test]
    fn infers_compression_from_extension() {
        assert_eq!(
            infer_source_compression("s3a://bucket/data.csv"),
            CompressionTypeVariant::UNCOMPRESSED
        );
        assert_eq!(
            infer_source_compression("s3a://bucket/data.csv.gz"),
            CompressionTypeVariant::GZIP
        );
        assert_eq!(
            infer_source_compression("s3a://bucket/data.csv.bz2"),
            CompressionTypeVariant::BZIP2
        );
        assert_eq!(
            infer_source_compression("s3a://bucket/data.csv.xz"),
            CompressionTypeVariant::XZ
        );
        assert_eq!(
            infer_source_compression("s3a://bucket/data.csv.zst"),
            CompressionTypeVariant::ZSTD
        );
        // case-insensitive
        assert_eq!(
            infer_source_compression("s3a://bucket/DATA.CSV.GZ"),
            CompressionTypeVariant::GZIP
        );
        // parquet is not compressed-csv
        assert_eq!(
            infer_source_compression("s3a://bucket/data.parquet"),
            CompressionTypeVariant::UNCOMPRESSED
        );
    }
}

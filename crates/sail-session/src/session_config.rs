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

use std::str::FromStr;
use std::sync::Arc;

use datafusion::common::parquet_config::DFParquetWriterVersion;
use datafusion::prelude::SessionConfig;
use sail_common::config::AppConfig;

/// Applies the application-level execution, parquet, and optimizer settings to a
/// [`SessionConfig`].
///
/// Both the server (driver) and the worker session factories share this factory so the two
/// sides of a distributed job apply identical settings. DataFusion consults the session
/// config while a worker decodes and executes a serialized physical plan, so a worker that
/// ran on `SessionConfig::default()` would silently diverge from the driver that configured
/// e.g. `target_partitions`, parquet read options, or work-stealing behavior.
///
/// Note: server-only session mutators may still override settings on the driver session
/// after this factory runs; that remains an intentional extension point and is not applied
/// on workers.
pub struct SessionConfigFactory {
    config: Arc<AppConfig>,
}

impl SessionConfigFactory {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self { config }
    }

    pub fn apply_execution_config(&self, config: &mut SessionConfig) {
        let execution = &mut config.options_mut().execution;

        execution.batch_size = self.config.execution.batch_size;
        if self.config.execution.default_parallelism > 0 {
            execution.target_partitions = self.config.execution.default_parallelism;
        }
        execution.collect_statistics = self.config.execution.collect_statistics;
        execution.use_row_number_estimates_to_optimize_partitioning = self
            .config
            .execution
            .use_row_number_estimates_to_optimize_partitioning;
        execution.listing_table_ignore_subdirectory = false;
        // Sail executes each partition as an independent task that decodes its own physical
        // plan instance. DataFusion's file-stream work stealing shares ONE file queue across
        // sibling partitions, which is only correct when those partitions run in a single
        // process against a single plan instance. With per-task plan decoding, every
        // partition would otherwise drain the ENTIRE shared queue and re-read the whole
        // source `target_partitions` times (Nx rows) for any byte-range split scan (e.g. the
        // LOAD DATA fallback path). Disable it so each partition reads only its own file
        // group. This must stay identical on the driver and worker sessions.
        execution.enable_file_stream_work_stealing = false;
    }

    pub fn apply_optimizer_config(&self, config: &mut SessionConfig) {
        let optimizer = &mut config.options_mut().optimizer;
        optimizer.expand_views_at_output = self.config.optimizer.expand_views_at_output;
    }

    pub fn apply_execution_parquet_config(&self, config: &mut SessionConfig) {
        let parquet = &mut config.options_mut().execution.parquet;

        parquet.created_by = concat!("sail version ", env!("CARGO_PKG_VERSION")).into();
        parquet.enable_page_index = self.config.parquet.enable_page_index;
        parquet.pruning = self.config.parquet.pruning;
        parquet.skip_metadata = self.config.parquet.skip_metadata;
        parquet.metadata_size_hint = self.config.parquet.metadata_size_hint;
        parquet.pushdown_filters = self.config.parquet.pushdown_filters;
        parquet.reorder_filters = self.config.parquet.reorder_filters;
        parquet.schema_force_view_types = self.config.parquet.schema_force_view_types;
        parquet.binary_as_string = self.config.parquet.binary_as_string;
        parquet.max_predicate_cache_size = Some(self.config.parquet.max_predicate_cache_size);
        parquet.coerce_int96 = Some("us".to_string());
        parquet.data_pagesize_limit = self.config.parquet.data_page_size_limit;
        parquet.write_batch_size = self.config.parquet.write_batch_size;
        parquet.writer_version =
            DFParquetWriterVersion::from_str(self.config.parquet.writer_version.as_str())
                .unwrap_or_default();
        parquet.skip_arrow_metadata = self.config.parquet.skip_arrow_metadata;
        parquet.compression = Some(self.config.parquet.compression.clone());
        parquet.dictionary_enabled = Some(self.config.parquet.dictionary_enabled);
        parquet.dictionary_page_size_limit = self.config.parquet.dictionary_page_size_limit;
        parquet.statistics_enabled = Some(self.config.parquet.statistics_enabled.clone());
        parquet.max_row_group_size = self.config.parquet.max_row_group_size;
        parquet.column_index_truncate_length = self.config.parquet.column_index_truncate_length;
        parquet.statistics_truncate_length = self.config.parquet.statistics_truncate_length;
        parquet.data_page_row_count_limit = self.config.parquet.data_page_row_count_limit;
        parquet.encoding = self.config.parquet.encoding.clone();
        parquet.bloom_filter_on_read = self.config.parquet.bloom_filter_on_read;
        parquet.bloom_filter_on_write = self.config.parquet.bloom_filter_on_write;
        parquet.bloom_filter_fpp = Some(self.config.parquet.bloom_filter_fpp);
        parquet.bloom_filter_ndv = Some(self.config.parquet.bloom_filter_ndv);
        parquet.allow_single_file_parallelism = self.config.parquet.allow_single_file_parallelism;
        parquet.maximum_parallel_row_group_writers =
            self.config.parquet.maximum_parallel_row_group_writers;
        parquet.maximum_buffered_record_batches_per_stream = self
            .config
            .parquet
            .maximum_buffered_record_batches_per_stream;
        parquet.content_defined_chunking.enabled =
            self.config.parquet.content_defined_chunking.enabled;
        parquet.content_defined_chunking.min_chunk_size =
            self.config.parquet.content_defined_chunking.min_chunk_size;
        parquet.content_defined_chunking.max_chunk_size =
            self.config.parquet.content_defined_chunking.max_chunk_size;
        parquet.content_defined_chunking.norm_level =
            self.config.parquet.content_defined_chunking.norm_level;
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionConfig;

    use super::*;

    #[test]
    fn execution_config_matches_application_config() {
        let app_config = AppConfig::load().expect("load application config");
        let mut session_config = SessionConfig::default();
        let factory = SessionConfigFactory::new(Arc::new(app_config.clone()));

        factory.apply_execution_config(&mut session_config);

        let execution = &session_config.options().execution;
        assert_eq!(execution.batch_size, app_config.execution.batch_size);
        let default_target_partitions = SessionConfig::default()
            .options()
            .execution
            .target_partitions;
        let expected_target_partitions = if app_config.execution.default_parallelism > 0 {
            app_config.execution.default_parallelism
        } else {
            default_target_partitions
        };
        assert_eq!(execution.target_partitions, expected_target_partitions);
        assert_eq!(
            execution.collect_statistics,
            app_config.execution.collect_statistics
        );
        assert_eq!(
            execution.use_row_number_estimates_to_optimize_partitioning,
            app_config
                .execution
                .use_row_number_estimates_to_optimize_partitioning
        );
        assert!(!execution.enable_file_stream_work_stealing);
        assert!(!execution.listing_table_ignore_subdirectory);
    }

    #[test]
    fn parquet_config_matches_application_config() {
        let app_config = AppConfig::load().expect("load application config");
        let mut session_config = SessionConfig::default();
        let factory = SessionConfigFactory::new(Arc::new(app_config.clone()));

        factory.apply_execution_parquet_config(&mut session_config);

        let parquet = &session_config.options().execution.parquet;
        assert_eq!(
            parquet.enable_page_index,
            app_config.parquet.enable_page_index
        );
        assert_eq!(parquet.pruning, app_config.parquet.pruning);
        assert_eq!(parquet.skip_metadata, app_config.parquet.skip_metadata);
        assert_eq!(
            parquet.metadata_size_hint,
            app_config.parquet.metadata_size_hint
        );
        assert_eq!(
            parquet.pushdown_filters,
            app_config.parquet.pushdown_filters
        );
        assert_eq!(parquet.reorder_filters, app_config.parquet.reorder_filters);
        assert_eq!(
            parquet.schema_force_view_types,
            app_config.parquet.schema_force_view_types
        );
        assert_eq!(
            parquet.binary_as_string,
            app_config.parquet.binary_as_string
        );
        assert_eq!(
            parquet.writer_version.to_string(),
            app_config.parquet.writer_version
        );
        assert_eq!(
            parquet.compression.as_deref(),
            Some(app_config.parquet.compression.as_str())
        );
        assert_eq!(
            parquet.dictionary_enabled,
            Some(app_config.parquet.dictionary_enabled)
        );
        assert_eq!(
            parquet.statistics_enabled.as_deref(),
            Some(app_config.parquet.statistics_enabled.as_str())
        );
        assert_eq!(
            parquet.max_row_group_size,
            app_config.parquet.max_row_group_size
        );
    }

    #[test]
    fn optimizer_config_matches_application_config() {
        let app_config = AppConfig::load().expect("load application config");
        let mut session_config = SessionConfig::default();
        let factory = SessionConfigFactory::new(Arc::new(app_config.clone()));

        factory.apply_optimizer_config(&mut session_config);

        assert_eq!(
            session_config.options().optimizer.expand_views_at_output,
            app_config.optimizer.expand_views_at_output
        );
    }
}

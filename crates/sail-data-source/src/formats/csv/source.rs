use std::fmt;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::task::Poll;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::file_format::csv::CsvDecoder;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion_common::config::CsvOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{FileRange, PartitionedFile, RangeCalculation, TableSchema};
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, GetResultPayload, ObjectStore};
use regex::Regex;

use super::decoder::{LossyUtf8Reader, decode_utf8_lossy_stream};
use super::projected::{DecoderBatchReader, ProjectedCsvDecoder, ProjectedCsvOptions};
use crate::formats::range::calculate_range_bounded;

type CsvBatchReader = Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>> + Send>;

#[derive(Debug, Clone)]
pub struct CsvSource {
    options: CsvOptions,
    null_regex: Option<Regex>,
    batch_size: Option<usize>,
    table_schema: TableSchema,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
}

impl CsvSource {
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            options: CsvOptions::default(),
            null_regex: None,
            batch_size: None,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn with_csv_options(mut self, options: CsvOptions) -> Result<Self> {
        if let Some(pattern) = &options.null_regex {
            let regex = Regex::new(pattern.as_str()).map_err(|e| {
                DataFusionError::External(
                    crate::error::DataSourceError::InvalidOption {
                        key: "null_regex".to_string(),
                        value: pattern.clone(),
                        cause: Some(e.to_string()),
                    }
                    .into(),
                )
            })?;
            self.null_regex = Some(regex);
        }
        self.options = options;
        Ok(self)
    }

    pub fn options(&self) -> &CsvOptions {
        &self.options
    }

    fn has_header(&self) -> bool {
        self.options.has_header.unwrap_or(true)
    }

    fn truncate_rows(&self) -> bool {
        self.options.truncated_rows.unwrap_or(false)
    }

    fn delimiter(&self) -> u8 {
        self.options.delimiter
    }

    fn quote(&self) -> u8 {
        self.options.quote
    }

    fn terminator(&self) -> Option<u8> {
        self.options.terminator
    }

    fn comment(&self) -> Option<u8> {
        self.options.comment
    }

    fn escape(&self) -> Option<u8> {
        self.options.escape
    }

    fn open<R: Read + Send + 'static>(&self, reader: R) -> Result<CsvBatchReader> {
        match self.projected_decoder()? {
            Some(decoder) => Ok(Box::new(DecoderBatchReader::new(
                BufReader::new(reader),
                decoder,
            ))),
            None => Ok(Box::new(self.builder()?.build(reader)?)),
        }
    }

    fn batch_size(&self) -> Result<usize> {
        self.batch_size.ok_or_else(|| {
            DataFusionError::Internal("batch_size must be set before calling builder()".to_string())
        })
    }

    /// The projected decoder covers all-string file schemas (the Spark default
    /// `inferSchema=false`); typed columns keep the arrow decoder.
    fn projected_decoder(&self) -> Result<Option<ProjectedCsvDecoder>> {
        let schema = self.table_schema.file_schema();
        let all_strings = schema
            .fields()
            .iter()
            .all(|field| field.data_type() == &DataType::Utf8);
        if !all_strings {
            return Ok(None);
        }
        let decoder = ProjectedCsvDecoder::try_new(ProjectedCsvOptions {
            schema: Arc::clone(schema),
            projection: self.projection.file_indices.clone(),
            delimiter: self.delimiter(),
            quote: self.quote(),
            escape: self.escape(),
            comment: self.comment(),
            terminator: self.terminator(),
            has_header: self.has_header(),
            truncated_rows: self.truncate_rows(),
            batch_size: self.batch_size()?,
            null_regex: self.null_regex.clone(),
        })?;
        Ok(Some(decoder))
    }

    fn builder(&self) -> Result<csv::ReaderBuilder> {
        let mut builder = csv::ReaderBuilder::new(Arc::clone(self.table_schema.file_schema()))
            .with_delimiter(self.delimiter())
            .with_batch_size(self.batch_size()?)
            .with_header(self.has_header())
            .with_quote(self.quote())
            .with_truncated_rows(self.truncate_rows())
            .with_projection(self.projection.file_indices.clone());

        if let Some(null_regex) = self.null_regex.clone() {
            builder = builder.with_null_regex(null_regex);
        }
        if let Some(terminator) = self.terminator() {
            builder = builder.with_terminator(terminator);
        }
        if let Some(escape) = self.escape() {
            builder = builder.with_escape(escape);
        }
        if let Some(comment) = self.comment() {
            builder = builder.with_comment(comment);
        }

        Ok(builder)
    }
}

impl From<CsvSource> for Arc<dyn FileSource> {
    fn from(source: CsvSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for CsvSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let opener = Arc::new(CsvOpener {
            config: Arc::new(self.clone()),
            file_compression_type: base_config.file_compression_type,
            object_store,
            partition,
        }) as Arc<dyn FileOpener>;

        ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = Some(batch_size);
        Arc::new(source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &projection);
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn file_type(&self) -> &str {
        "csv"
    }

    fn supports_repartitioning(&self) -> bool {
        !self.options.newlines_in_values.unwrap_or(false)
    }

    fn fmt_extra(&self, format: DisplayFormatType, output: &mut fmt::Formatter) -> fmt::Result {
        match format {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(output, ", has_header={}", self.has_header())
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

struct CsvOpener {
    config: Arc<CsvSource>,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
    partition: usize,
}

impl FileOpener for CsvOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let mut has_header = self.config.has_header();
        if let Some(FileRange { start, .. }) = partitioned_file.range
            && start != 0
        {
            has_header = false;
        }

        let mut config = (*self.config).clone();
        config.options.has_header = Some(has_header);
        config.options.truncated_rows = Some(config.truncate_rows());

        let file_compression_type = self.file_compression_type;
        if partitioned_file.range.is_some() && file_compression_type.is_compressed() {
            return Err(DataFusionError::Internal(
                "Reading compressed .csv in parallel is not supported".to_string(),
            ));
        }

        let object_store = Arc::clone(&self.object_store);
        let terminator = self.config.terminator();
        let baseline_metrics = BaselineMetrics::new(&self.config.metrics, self.partition);

        Ok(Box::pin(async move {
            let calculated_range =
                calculate_range_bounded(&partitioned_file, &object_store, terminator).await?;
            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed());
                }
            };
            let result = object_store
                .get_opts(
                    &partitioned_file.object_meta.location,
                    GetOptions {
                        range,
                        ..Default::default()
                    },
                )
                .await?;

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    let decompressed = if partitioned_file.range.is_none() {
                        file_compression_type.convert_read(file)?
                    } else {
                        file.seek(SeekFrom::Start(result.range.start as _))?;
                        file_compression_type.convert_read(
                            file.take((result.range.end - result.range.start) as u64),
                        )?
                    };
                    let mut reader = config.open(LossyUtf8Reader::new(decompressed))?;
                    let iterator = std::iter::from_fn(move || {
                        let mut timer = baseline_metrics.elapsed_compute().timer();
                        let result = reader.next();
                        timer.stop();
                        result
                    });

                    Ok(futures::stream::iter(iterator)
                        .map_err(DataFusionError::from)
                        .boxed())
                }
                GetResultPayload::Stream(stream) => {
                    let input = stream.map_err(DataFusionError::from).boxed();
                    let input = file_compression_type.convert_stream(input)?;
                    let input = decode_utf8_lossy_stream(input).fuse();
                    let stream = match config.projected_decoder()? {
                        Some(decoder) => {
                            deserialize_stream(input, DecoderDeserializer::new(decoder))
                        }
                        None => {
                            let decoder = config.builder()?.build_decoder();
                            deserialize_stream(
                                input,
                                DecoderDeserializer::new(CsvDecoder::new(decoder)),
                            )
                        }
                    };

                    Ok(stream.map_err(DataFusionError::from).boxed())
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Schema;

    use super::*;

    fn empty_table_schema() -> TableSchema {
        TableSchema::new(Arc::new(Schema::empty()), vec![])
    }

    #[test]
    fn test_with_csv_options_compiles_null_regex() -> Result<()> {
        let csv = CsvOptions {
            null_regex: Some("null".to_string()),
            ..Default::default()
        };
        let source = CsvSource::new(empty_table_schema()).with_csv_options(csv)?;
        assert!(source.null_regex.is_some());
        Ok(())
    }

    #[test]
    fn test_with_csv_options_leaves_null_regex_unset_by_default() -> Result<()> {
        let source = CsvSource::new(empty_table_schema()).with_csv_options(CsvOptions::default())?;
        assert!(source.null_regex.is_none());
        Ok(())
    }

    #[test]
    fn test_with_csv_options_rejects_invalid_null_regex() {
        let csv = CsvOptions {
            null_regex: Some("(".to_string()),
            ..Default::default()
        };
        let result = CsvSource::new(empty_table_schema()).with_csv_options(csv);
        assert!(result.is_err());
    }
}

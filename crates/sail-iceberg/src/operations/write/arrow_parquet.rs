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

use bytes::Bytes;
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use super::async_buffer::AsyncShareableBuffer;

pub struct ParquetFileMeta {
    pub num_rows: u64,
    pub file_size: u64,
    pub parquet_metadata: ParquetMetaData,
}

pub struct ArrowParquetWriter {
    writer: Option<AsyncArrowWriter<AsyncShareableBuffer>>,
    buffer: Option<AsyncShareableBuffer>,
}

impl ArrowParquetWriter {
    pub fn try_new(
        schema: &datafusion::arrow::datatypes::Schema,
        props: WriterProperties,
    ) -> Result<Self, String> {
        let buffer = AsyncShareableBuffer::default();
        let writer =
            AsyncArrowWriter::try_new(buffer.clone(), Arc::new(schema.clone()), Some(props))
                .map_err(|e| format!("parquet writer error: {e}"))?;
        Ok(Self {
            writer: Some(writer),
            buffer: Some(buffer),
        })
    }

    pub async fn write_batch(
        &mut self,
        batch: &datafusion::arrow::array::RecordBatch,
    ) -> Result<(), String> {
        let writer = self.writer.as_mut().ok_or("writer closed")?;
        writer
            .write(batch)
            .await
            .map_err(|e| format!("parquet write: {e}"))
    }

    pub fn buffered_size(&self) -> u64 {
        let flushed = self.buffer.as_ref().map(|b| b.bytes_written()).unwrap_or(0);
        let in_progress = self
            .writer
            .as_ref()
            .map(|w| w.in_progress_size() as u64)
            .unwrap_or(0);
        flushed + in_progress
    }

    pub async fn close(mut self) -> Result<(Bytes, ParquetFileMeta), String> {
        let writer = self.writer.take().ok_or("writer already closed")?;
        let buffer = self.buffer.take().ok_or("buffer already taken")?;
        let metadata = writer
            .close()
            .await
            .map_err(|e| format!("parquet finish: {e}"))?;
        let buf = buffer
            .into_inner()
            .await
            .ok_or("failed to extract parquet buffer")?;
        let file_size = buf.len() as u64;
        let bytes = Bytes::from(buf);
        let num_rows = metadata.file_metadata().num_rows() as u64;
        Ok((
            bytes,
            ParquetFileMeta {
                num_rows,
                file_size,
                parquet_metadata: metadata,
            },
        ))
    }
}

pub(crate) fn build_writer_properties(
    table_properties: &[(String, String)],
    compression: Compression,
) -> Result<WriterProperties, String> {
    let mut builder = WriterProperties::builder().set_compression(compression);

    let prop = |key: &str| -> Option<&str> {
        table_properties
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.as_str())
    };

    if let Some(v) = prop("write.parquet.row-group-size-bytes") {
        let bytes: u64 = v
            .trim()
            .parse()
            .map_err(|_| format!("invalid write.parquet.row-group-size-bytes: {v}"))?;
        builder = builder.set_max_row_group_bytes(Some(bytes as usize));
    }
    if let Some(v) = prop("write.parquet.page-size-bytes") {
        let bytes: u64 = v
            .trim()
            .parse()
            .map_err(|_| format!("invalid write.parquet.page-size-bytes: {v}"))?;
        builder = builder.set_data_page_size_limit(bytes as usize);
    }
    if let Some(v) = prop("write.parquet.dict-size-bytes") {
        let bytes: u64 = v
            .trim()
            .parse()
            .map_err(|_| format!("invalid write.parquet.dict-size-bytes: {v}"))?;
        builder = builder.set_dictionary_page_size_limit(bytes as usize);
    }

    if let Some(v) = prop("write.metadata.metrics.default") {
        let stats = match v.trim().to_ascii_lowercase().as_str() {
            "none" => EnabledStatistics::None,
            "counts" => EnabledStatistics::Chunk,
            "full" => EnabledStatistics::Page,
            other => {
                return Err(format!(
                    "invalid write.metadata.metrics.default: {other} (expected none|counts|full)"
                ));
            }
        };
        builder = builder.set_statistics_enabled(stats);
    }

    for (k, v) in table_properties {
        let col = match k.strip_prefix("write.parquet.") {
            Some(rest) => rest,
            None => continue,
        };
        if let Some(name) = col.strip_prefix("stats-enabled.column.") {
            let enabled: bool = v.trim().parse().map_err(|_| format!("invalid {k}: {v}"))?;
            let stats = if enabled {
                EnabledStatistics::Page
            } else {
                EnabledStatistics::None
            };
            builder = builder
                .set_column_statistics_enabled(ColumnPath::new(vec![name.to_string()]), stats);
        } else if let Some(name) = col.strip_prefix("bloom-filter-fpp.column.") {
            let fpp: f64 = v.trim().parse().map_err(|_| format!("invalid {k}: {v}"))?;
            builder =
                builder.set_column_bloom_filter_fpp(ColumnPath::new(vec![name.to_string()]), fpp);
        } else if let Some(name) = col.strip_prefix("bloom-filter-ndv.column.") {
            let ndv: u64 = v.trim().parse().map_err(|_| format!("invalid {k}: {v}"))?;
            builder =
                builder.set_column_bloom_filter_ndv(ColumnPath::new(vec![name.to_string()]), ndv);
        } else if let Some(name) = col.strip_prefix("bloom-filter-enabled.column.") {
            let enabled: bool = v.trim().parse().map_err(|_| format!("invalid {k}: {v}"))?;
            builder = builder
                .set_column_bloom_filter_enabled(ColumnPath::new(vec![name.to_string()]), enabled);
        }
    }

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    use super::*;

    fn sample_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn sample_batch(rows: i32) -> RecordBatch {
        let id: Vec<i32> = (0..rows).collect();
        let name: Vec<String> = (0..rows).map(|i| format!("row-{i}")).collect();
        RecordBatch::try_new(
            Arc::new(sample_schema()),
            vec![
                Arc::new(Int32Array::from(id)),
                Arc::new(StringArray::from(name)),
            ],
        )
        .unwrap()
    }

    fn props(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn build_writer_properties_sets_sizes() {
        let properties = build_writer_properties(
            &props(&[
                ("write.parquet.row-group-size-bytes", "67108864"),
                ("write.parquet.page-size-bytes", "1048576"),
                ("write.parquet.dict-size-bytes", "524288"),
            ]),
            Compression::SNAPPY,
        )
        .unwrap();

        let root = ColumnPath::new(Vec::new());
        assert_eq!(properties.compression(&root), Compression::SNAPPY);
        assert_eq!(properties.max_row_group_bytes(), Some(67_108_864));
        assert_eq!(properties.data_page_size_limit(), 1_048_576);
        assert_eq!(properties.dictionary_page_size_limit(), 524_288);
    }

    #[test]
    fn build_writer_properties_sets_table_wide_statistics() {
        let properties = build_writer_properties(
            &props(&[("write.metadata.metrics.default", "none")]),
            Compression::ZSTD(Default::default()),
        )
        .unwrap();
        let root = ColumnPath::new(Vec::new());
        assert_eq!(
            properties.statistics_enabled(&root),
            EnabledStatistics::None
        );

        let properties = build_writer_properties(
            &props(&[("write.metadata.metrics.default", "full")]),
            Compression::ZSTD(Default::default()),
        )
        .unwrap();
        assert_eq!(
            properties.statistics_enabled(&root),
            EnabledStatistics::Page
        );
    }

    #[test]
    fn build_writer_properties_sets_per_column_stats_and_bloom() {
        let properties = build_writer_properties(
            &props(&[
                ("write.parquet.stats-enabled.column.name", "false"),
                ("write.parquet.bloom-filter-enabled.column.name", "true"),
                ("write.parquet.bloom-filter-fpp.column.name", "0.01"),
                ("write.parquet.bloom-filter-ndv.column.name", "5000"),
            ]),
            Compression::ZSTD(Default::default()),
        )
        .unwrap();

        let name_col = ColumnPath::new(vec!["name".to_string()]);
        assert_eq!(
            properties.statistics_enabled(&name_col),
            EnabledStatistics::None
        );
        let bloom = properties.bloom_filter_properties(&name_col).unwrap();
        assert_eq!(bloom.fpp, 0.01);
        assert_eq!(bloom.ndv, 5000);
    }

    #[test]
    fn build_writer_properties_ignores_unknown_keys() {
        let properties = build_writer_properties(
            &props(&[("write.parquet.unknown-key", "123"), ("some.other", "x")]),
            Compression::UNCOMPRESSED,
        )
        .unwrap();
        let root = ColumnPath::new(Vec::new());
        assert_eq!(properties.compression(&root), Compression::UNCOMPRESSED);
    }

    #[test]
    fn build_writer_properties_rejects_invalid_values() {
        assert!(
            build_writer_properties(
                &props(&[("write.metadata.metrics.default", "bogus")]),
                Compression::SNAPPY,
            )
            .is_err()
        );
        assert!(
            build_writer_properties(
                &props(&[("write.parquet.stats-enabled.column.id", "notabool")]),
                Compression::SNAPPY,
            )
            .is_err()
        );
        assert!(
            build_writer_properties(
                &props(&[("write.parquet.row-group-size-bytes", "abc")]),
                Compression::SNAPPY,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn buffered_size_reflects_written_bytes() {
        let mut writer =
            ArrowParquetWriter::try_new(&sample_schema(), WriterProperties::default()).unwrap();
        assert_eq!(writer.buffered_size(), 0);

        writer.write_batch(&sample_batch(1000)).await.unwrap();
        assert!(
            writer.buffered_size() > 0,
            "buffered_size should grow after a write"
        );

        let before = writer.buffered_size();
        writer.write_batch(&sample_batch(1000)).await.unwrap();
        let after = writer.buffered_size();
        assert!(after >= before, "buffered_size should be monotonic");
    }

    #[tokio::test]
    async fn close_returns_full_buffer_and_row_count() {
        let mut writer =
            ArrowParquetWriter::try_new(&sample_schema(), WriterProperties::default()).unwrap();
        writer.write_batch(&sample_batch(1234)).await.unwrap();
        let (bytes, meta) = writer.close().await.unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(meta.num_rows, 1234);
        assert_eq!(meta.file_size, bytes.len() as u64);
    }
}

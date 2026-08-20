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

use crate::operations::write::WriteOutcome;
use crate::operations::write::arrow_parquet::ParquetFileMeta;
use crate::spec::types::values::Literal;
use crate::spec::{DataContentType, DataFile, DataFileFormat, Datum};

pub struct DataFileWriter {
    pub partition_spec_id: i32,
    pub file_path: String,
    pub partition_values: Vec<Option<Literal>>,
}

impl DataFileWriter {
    pub fn new(
        partition_spec_id: i32,
        file_path: String,
        partition_values: Vec<Option<Literal>>,
    ) -> Self {
        Self {
            partition_spec_id,
            file_path,
            partition_values,
        }
    }

    pub fn finish(self, meta: ParquetFileMeta) -> Result<WriteOutcome, String> {
        let (
            column_sizes,
            value_counts,
            null_value_counts,
            lower_bounds,
            upper_bounds,
            split_offsets,
        ) = aggregate_from_parquet_metadata(&meta.parquet_metadata)?;

        let data_file = DataFile {
            content: DataContentType::Data,
            file_path: self.file_path,
            file_format: DataFileFormat::Parquet,
            partition: self.partition_values,
            record_count: meta.num_rows,
            file_size_in_bytes: meta.file_size,
            column_sizes,
            value_counts,
            null_value_counts,
            nan_value_counts: Default::default(),
            lower_bounds,
            upper_bounds,
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets,
            equality_ids: Vec::new(),
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: self.partition_spec_id,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        Ok(WriteOutcome { data_file })
    }
}

type AggregatedMetadata = (
    HashMap<i32, u64>,
    HashMap<i32, u64>,
    HashMap<i32, u64>,
    HashMap<i32, Datum>,
    HashMap<i32, Datum>,
    Vec<i64>,
);

fn aggregate_from_parquet_metadata(
    parquet_meta: &parquet::file::metadata::ParquetMetaData,
) -> Result<AggregatedMetadata, String> {
    let row_groups = parquet_meta.row_groups();
    let schema_descr = parquet_meta.file_metadata().schema_descr();

    let mut col_sizes: HashMap<i32, u64> = HashMap::new();
    let mut val_counts: HashMap<i32, u64> = HashMap::new();
    let mut null_counts: HashMap<i32, u64> = HashMap::new();
    let lower_bounds: HashMap<i32, Datum> = HashMap::new();
    let upper_bounds: HashMap<i32, Datum> = HashMap::new();
    let mut split_offsets: Vec<i64> = Vec::new();

    for rg in row_groups {
        if let Some(off) = rg.file_offset() {
            split_offsets.push(off);
        }
        for (column_index, c) in rg.columns().iter().enumerate() {
            let _path = c.column_descr().path().string();
            let leaf_info = c.column_descr().self_type().get_basic_info();
            let Some(field_id) = (if leaf_info.has_id() {
                Some(leaf_info.id())
            } else {
                let root_info = schema_descr.get_column_root(column_index).get_basic_info();
                if root_info.has_id() {
                    Some(root_info.id())
                } else {
                    None
                }
            }) else {
                continue;
            };
            *col_sizes.entry(field_id).or_insert(0) += c.compressed_size() as u64;
            *val_counts.entry(field_id).or_insert(0) += c.num_values() as u64;
            if let Some(stats) = c.statistics() {
                if let Some(n) = stats.null_count_opt() {
                    *null_counts.entry(field_id).or_insert(0) += n;
                }
                // Do not attempt to parse typed bounds here; leave empty per-field for now
                let _ = _path; // silence unused
            }
        }
    }

    Ok((
        col_sizes,
        val_counts,
        null_counts,
        lower_bounds,
        upper_bounds,
        split_offsets,
    ))
}

/// Same as `aggregate_from_parquet_metadata` but re-keys all per-column statistics
/// from parquet-embedded field IDs to Iceberg schema field IDs using a
/// `column_name → field_id` map. Columns not present in the map are dropped.
///
/// External parquet files carry no Iceberg field IDs, so this is required when
/// registering externally-produced parquet files via the `LOAD DATA` fast path.
pub(crate) fn aggregate_from_parquet_metadata_with_field_map(
    parquet_meta: &parquet::file::metadata::ParquetMetaData,
    field_id_map: &std::collections::HashMap<String, i32>,
) -> Result<AggregatedMetadata, String> {
    let row_groups = parquet_meta.row_groups();

    let mut col_sizes: HashMap<i32, u64> = HashMap::new();
    let mut val_counts: HashMap<i32, u64> = HashMap::new();
    let mut null_counts: HashMap<i32, u64> = HashMap::new();
    let lower_bounds: HashMap<i32, Datum> = HashMap::new();
    let upper_bounds: HashMap<i32, Datum> = HashMap::new();
    let mut split_offsets: Vec<i64> = Vec::new();

    for rg in row_groups {
        if let Some(off) = rg.file_offset() {
            split_offsets.push(off);
        }
        for c in rg.columns() {
            let col_path = c.column_descr().path().string();
            let col_name = col_path.split('.').last().unwrap_or(&col_path).to_string();
            let Some(&iceberg_field_id) = field_id_map.get(&col_name) else {
                continue;
            };
            *col_sizes.entry(iceberg_field_id).or_insert(0) += c.compressed_size() as u64;
            *val_counts.entry(iceberg_field_id).or_insert(0) += c.num_values() as u64;
            if let Some(stats) = c.statistics() {
                if let Some(n) = stats.null_count_opt() {
                    *null_counts.entry(iceberg_field_id).or_insert(0) += n;
                }
            }
        }
    }

    Ok((
        col_sizes,
        val_counts,
        null_counts,
        lower_bounds,
        upper_bounds,
        split_offsets,
    ))
}

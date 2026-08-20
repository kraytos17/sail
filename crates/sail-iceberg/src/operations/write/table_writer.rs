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

use datafusion::arrow::array::{ArrayRef, new_null_array};
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use url::Url;

use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::operations::write::base_writer::DataFileWriter;
use crate::operations::write::config::WriterConfig;
use crate::operations::write::file_writer::location_generator::{
    DefaultLocationGenerator, LocationGenerator,
};
use crate::operations::write::partition::split_record_batch_by_partition;
use crate::operations::write::variant_shredding::{
    VariantShreddingPlan, apply_variant_shredding_plan, build_variant_shredding_plan,
    unshred_shredded_variants_for_write,
};
use crate::spec::DataFile;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::types::NestedField;
use crate::spec::types::values::Literal;
use crate::utils::conversions::to_scalar;

enum PartitionWriterState {
    Pending {
        batches: Vec<RecordBatch>,
        num_rows: usize,
    },
    Open {
        writer: Box<ArrowParquetWriter>,
        variant_shredding_plan: Option<VariantShreddingPlan>,
    },
}

pub struct IcebergTableWriter {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub config: WriterConfig,
    pub generator: DefaultLocationGenerator,
    pub data_url: Url,
    // partition_dir -> writer
    writers: HashMap<String, PartitionWriterState>,
    // partition_dir -> partition values aligned with spec
    partition_values_map: HashMap<String, Vec<Option<Literal>>>,
    written: Vec<DataFile>,
    pub partition_spec_id: i32,
}

impl IcebergTableWriter {
    pub fn new(
        store: Arc<dyn object_store::ObjectStore>,
        root: ObjectPath,
        config: WriterConfig,
        partition_spec_id: i32,
        data_url: Url,
    ) -> Self {
        Self {
            generator: DefaultLocationGenerator::new(root),
            store,
            config,
            data_url,
            writers: HashMap::new(),
            partition_values_map: HashMap::new(),
            written: Vec::new(),
            partition_spec_id,
        }
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let spec = &self.config.partition_spec;
        let iceberg_schema = &self.config.iceberg_schema;

        if spec.fields.is_empty() {
            // Unpartitioned: write as-is once
            let partition_dir = String::new();
            self.partition_values_map
                .entry(partition_dir.clone())
                .or_default();
            let padded = Self::align_batch_with_table_schema(
                batch,
                &self.config.table_schema,
                self.config.iceberg_schema.as_ref(),
            )
            .map_err(|e| e.to_string())?;
            let normalized =
                unshred_shredded_variants_for_write(&padded, &self.config.table_schema)?;
            let aligned = cast_record_batch_relaxed_tz(&normalized, &self.config.table_schema)
                .map_err(|e| e.to_string())?;
            // Unpartitioned data files carry an empty partition tuple.
            self.write_aligned_batch(partition_dir, Vec::new(), aligned)
                .await?;
            return Ok(());
        }

        let parts = split_record_batch_by_partition(batch, spec, iceberg_schema)?;
        for p in parts.into_iter() {
            let partition_dir = p.partition_dir;
            let partition_values = p.partition_values.clone();
            self.partition_values_map
                .entry(partition_dir.clone())
                .or_insert(partition_values.clone());
            let padded = Self::align_batch_with_table_schema(
                &p.record_batch,
                &self.config.table_schema,
                self.config.iceberg_schema.as_ref(),
            )
            .map_err(|e| e.to_string())?;
            let normalized =
                unshred_shredded_variants_for_write(&padded, &self.config.table_schema)?;
            let aligned = cast_record_batch_relaxed_tz(&normalized, &self.config.table_schema)
                .map_err(|e| e.to_string())?;
            self.write_aligned_batch(partition_dir, partition_values, aligned)
                .await?;
        }

        Ok(())
    }

    async fn write_aligned_batch(
        &mut self,
        partition_dir: String,
        partition_values: Vec<Option<Literal>>,
        batch: RecordBatch,
    ) -> Result<(), String> {
        let state = self
            .writers
            .remove(&partition_dir)
            .map(Ok)
            .unwrap_or_else(|| self.new_partition_writer_state())?;
        let state = self.write_partition_state(state, batch).await?;

        // Roll a new data file once the total bytes buffered (flushed + in-progress row group)
        // reach the target file size. This bounds how large any single parquet file grows
        // (previously a partition kept one unbounded writer until close), mirroring
        // DataFusion's ArrowWriter convention that the DeltaLake writer also follows.
        // A `target_file_size` of 0 disables rolling (single file per partition until close).
        let needs_roll = matches!(
            &state,
            PartitionWriterState::Open { writer, .. }
                if self.config.target_file_size > 0
                    && writer.buffered_size() >= self.config.target_file_size
        );
        if needs_roll {
            let writer = self.finish_partition_state(state).await?;
            self.record_finished_writer(partition_dir, partition_values, writer)
                .await?;
            return Ok(());
        }

        self.writers.insert(partition_dir, state);
        Ok(())
    }

    fn new_partition_writer_state(&self) -> Result<PartitionWriterState, String> {
        if self.config.variant_shredding.enabled {
            Ok(PartitionWriterState::Pending {
                batches: Vec::new(),
                num_rows: 0,
            })
        } else {
            Ok(PartitionWriterState::Open {
                writer: Box::new(self.new_arrow_writer(self.config.table_schema.clone())?),
                variant_shredding_plan: None,
            })
        }
    }

    async fn write_partition_state(
        &mut self,
        state: PartitionWriterState,
        batch: RecordBatch,
    ) -> Result<PartitionWriterState, String> {
        match state {
            PartitionWriterState::Pending {
                mut batches,
                mut num_rows,
            } => {
                num_rows += batch.num_rows();
                batches.push(batch);
                if num_rows >= self.config.variant_shredding.inference_buffer_size.max(1) {
                    self.open_and_write_pending_batches(batches).await
                } else {
                    Ok(PartitionWriterState::Pending { batches, num_rows })
                }
            }
            PartitionWriterState::Open {
                mut writer,
                variant_shredding_plan,
            } => {
                let batch = if let Some(plan) = variant_shredding_plan.as_ref() {
                    apply_variant_shredding_plan(&batch, plan)?
                } else {
                    batch
                };
                writer.write_batch(&batch).await?;
                Ok(PartitionWriterState::Open {
                    writer,
                    variant_shredding_plan,
                })
            }
        }
    }

    async fn open_and_write_pending_batches(
        &mut self,
        batches: Vec<RecordBatch>,
    ) -> Result<PartitionWriterState, String> {
        let plan = build_variant_shredding_plan(
            &self.config.table_schema,
            &batches,
            self.config.variant_shredding.inference_buffer_size,
            self.config.variant_shredding.inference_node_budget,
        )?;
        let plan = (!plan.is_noop()).then_some(plan);
        let physical_batches = batches
            .into_iter()
            .map(|batch| {
                if let Some(plan) = plan.as_ref() {
                    apply_variant_shredding_plan(&batch, plan)
                } else {
                    Ok(batch)
                }
            })
            .collect::<std::result::Result<Vec<_>, String>>()?;

        let schema = physical_batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| self.config.table_schema.clone());
        let mut writer = self.new_arrow_writer(schema)?;
        for batch in physical_batches {
            writer.write_batch(&batch).await?;
        }
        Ok(PartitionWriterState::Open {
            writer: Box::new(writer),
            variant_shredding_plan: plan,
        })
    }

    fn new_arrow_writer(&self, schema: SchemaRef) -> Result<ArrowParquetWriter, String> {
        for (i, f) in schema.fields().iter().enumerate() {
            log::trace!(
                "iceberg.table_writer.writer_schema: field[{}]='{}' type={:?} field_id_meta={:?}",
                i,
                f.name(),
                f.data_type(),
                f.metadata().get(PARQUET_FIELD_ID_META_KEY)
            );
        }
        ArrowParquetWriter::try_new(schema.as_ref(), self.config.writer_properties.clone())
    }

    async fn finish_partition_state(
        &mut self,
        state: PartitionWriterState,
    ) -> Result<ArrowParquetWriter, String> {
        match state {
            PartitionWriterState::Pending { batches, .. } => {
                let PartitionWriterState::Open { writer, .. } =
                    self.open_and_write_pending_batches(batches).await?
                else {
                    return Err("failed to open pending Iceberg partition writer".to_string());
                };
                Ok(*writer)
            }
            PartitionWriterState::Open { writer, .. } => Ok(*writer),
        }
    }

    pub async fn flush_partition(
        &mut self,
        partition_dir: &str,
        partition_values: Vec<Option<Literal>>,
    ) -> Result<(), String> {
        if let Some(state) = self.writers.remove(partition_dir) {
            let writer = self.finish_partition_state(state).await?;
            self.record_finished_writer(partition_dir.to_string(), partition_values, writer)
                .await?;
        }
        Ok(())
    }

    /// Close an `ArrowParquetWriter`, write it to the store, and record its `DataFile`.
    ///
    /// Shared by both anomalous flushes (`flush_partition` at close) and size-based rolling
    /// (`write_aligned_batch`) so every produced file follows the same put + manifest path.
    async fn record_finished_writer(
        &mut self,
        partition_dir: String,
        partition_values: Vec<Option<Literal>>,
        writer: ArrowParquetWriter,
    ) -> Result<(), String> {
        let (bytes, meta) = writer.close().await?;
        let (rel, full) = self
            .generator
            .with_partition_dir(Some(partition_dir.as_str()));
        log::trace!("iceberg.table_writer.flush_partition.writing: {}", full);
        self.store
            .put(&full, object_store::PutPayload::from(bytes))
            .await
            .map_err(|e| e.to_string())?;
        log::trace!(
            "iceberg.table_writer.flush_partition.written: rel={} full={}",
            rel,
            full
        );
        // Prevent a leading partition segment containing ':' from being parsed as a URI scheme.
        let file_path = match self.data_url.join(&format!("./{rel}")) {
            Ok(u) => u.to_string(),
            Err(_) => {
                format!("{}{}", self.data_url.as_str(), rel)
            }
        };
        let df = DataFileWriter::new(self.partition_spec_id, file_path, partition_values)
            .finish(meta)?
            .data_file;
        self.written.push(df);
        Ok(())
    }

    pub async fn close(mut self) -> Result<Vec<DataFile>, String> {
        let keys: Vec<String> = self.writers.keys().cloned().collect();
        for k in keys {
            let vals = self
                .partition_values_map
                .remove(&k)
                .unwrap_or_default()
                .into_iter()
                .collect();
            self.flush_partition(&k, vals).await?;
        }
        Ok(self.written)
    }

    fn align_batch_with_table_schema(
        batch: &RecordBatch,
        table_schema: &SchemaRef,
        iceberg_schema: &IcebergSchema,
    ) -> Result<RecordBatch, DataFusionError> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(table_schema.fields().len());
        let mut schema_fields: Vec<FieldRef> = Vec::with_capacity(table_schema.fields().len());

        for field in table_schema.fields() {
            match batch.schema().index_of(field.name()) {
                Ok(idx) => {
                    columns.push(batch.column(idx).clone());
                    schema_fields.push(Arc::new(batch.schema().field(idx).clone()));
                }
                Err(_) => {
                    let array =
                        Self::build_missing_column_array(field, iceberg_schema, batch.num_rows())?;
                    columns.push(array);
                    schema_fields.push(field.clone());
                }
            }
        }

        let aligned_schema = Arc::new(Schema::new(schema_fields));
        Ok(RecordBatch::try_new(aligned_schema, columns)?)
    }

    fn build_missing_column_array(
        field: &FieldRef,
        iceberg_schema: &IcebergSchema,
        num_rows: usize,
    ) -> Result<ArrayRef, DataFusionError> {
        let iceberg_field = iceberg_schema.field_by_name(field.name()).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Column '{}' missing from Iceberg schema during alignment",
                field.name()
            ))
        })?;

        if let Some(array) = Self::default_array_for_field(iceberg_field.as_ref(), num_rows)? {
            return Ok(array);
        }

        if field.is_nullable() {
            return Ok(new_null_array(field.data_type(), num_rows));
        }

        Err(DataFusionError::Plan(format!(
            "Column '{}' is required but missing in input batch and has no default value",
            field.name()
        )))
    }

    fn default_array_for_field(
        field: &NestedField,
        num_rows: usize,
    ) -> Result<Option<ArrayRef>, DataFusionError> {
        let literal = field
            .write_default
            .as_ref()
            .or(field.initial_default.as_ref());
        if let Some(lit) = literal {
            let scalar = to_scalar(lit, field.field_type.as_ref())?;
            let array = scalar
                .to_array_of_size(num_rows)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            return Ok(Some(array));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::operations::write::config::VariantShreddingConfig;
    use crate::spec::NestedField;
    use crate::spec::partition::UnboundPartitionSpec;
    use crate::spec::types::{PrimitiveType, Type};

    fn test_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn test_iceberg_schema() -> IcebergSchema {
        IcebergSchema::builder()
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

    fn sample_batch(rows: i32) -> RecordBatch {
        let id: Vec<i32> = (0..rows).collect();
        let name: Vec<String> = (0..rows).map(|i| format!("row-{i}")).collect();
        RecordBatch::try_new(
            Arc::new(test_arrow_schema()),
            vec![
                Arc::new(Int32Array::from(id)),
                Arc::new(StringArray::from(name)),
            ],
        )
        .unwrap()
    }

    fn table_writer(target_file_size: u64) -> (IcebergTableWriter, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = WriterConfig {
            table_schema: Arc::new(test_arrow_schema()),
            partition_columns: Vec::new(),
            writer_properties: WriterProperties::default(),
            target_file_size,
            write_batch_size: 8192,
            num_indexed_cols: 0,
            stats_columns: None,
            iceberg_schema: Arc::new(test_iceberg_schema()),
            partition_spec: UnboundPartitionSpec { fields: vec![] },
            variant_shredding: VariantShreddingConfig::default(),
        };
        let writer = IcebergTableWriter::new(
            store.clone(),
            ObjectPath::from("table"),
            config,
            0,
            Url::parse("memory://bucket/table").unwrap(),
        );
        (writer, store)
    }

    #[tokio::test]
    async fn rolls_multiple_data_files_at_target_size() {
        let (mut writer, _store) = table_writer(1024); // tiny target forces rolling
        let mut expected_rows = 0u64;
        for _ in 0..20 {
            let batch = sample_batch(5000);
            expected_rows += batch.num_rows() as u64;
            writer.write(&batch).await.unwrap();
        }

        let data_files = writer.close().await.unwrap();
        assert!(
            data_files.len() > 1,
            "expected multiple rolled data files, got {}",
            data_files.len()
        );
        let actual_rows: u64 = data_files.iter().map(|df| df.record_count).sum();
        assert_eq!(actual_rows, expected_rows);
        // Unpartitioned files carry an empty partition tuple.
        assert!(data_files.iter().all(|df| df.partition.is_empty()));
    }

    #[tokio::test]
    async fn zero_target_file_size_keeps_single_file() {
        let (mut writer, _store) = table_writer(0); // 0 disables rolling
        for _ in 0..20 {
            let batch = sample_batch(5000);
            writer.write(&batch).await.unwrap();
        }

        let data_files = writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);
    }
}

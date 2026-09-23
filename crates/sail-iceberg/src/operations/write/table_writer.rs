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

struct PartitionWriter {
    partition_dir: String,
    state: PartitionWriterState,
}

/// Minimum file size uploaded as S3 multipart.
///
/// Below this everything (manifests, metadata, small and delete files) keeps
/// the single-shot `put()`: one request is cheaper and cannot strand parts.
/// At or above it the file goes up in `MULTIPART_PART_SIZE` parts, each
/// retried independently, so a mid-upload reset re-sends one part instead of
/// the whole file.
const MULTIPART_PUT_THRESHOLD: u64 = 128 * 1024 * 1024;

/// Part size for multipart uploads: far above S3's 5 MB minimum, so a
/// 512 MB file completes in 8 parts with per-part restart on failure.
const MULTIPART_PART_SIZE: usize = 64 * 1024 * 1024;

fn use_multipart_put(size: u64) -> bool {
    size >= MULTIPART_PUT_THRESHOLD
}

/// Uploads `bytes` to `location` as an S3 multipart upload.
///
/// Parts upload concurrently; completion happens only when every part is
/// durable. Any part failure (or a failed completion) aborts the upload so
/// no partial object or orphaned parts are left behind.
async fn put_file_multipart(
    store: &dyn object_store::ObjectStore,
    location: &ObjectPath,
    bytes: bytes::Bytes,
    part_size: usize,
) -> Result<(), String> {
    if bytes.is_empty() {
        return Err("multipart upload requires non-empty content".to_string());
    }
    let mut upload = store
        .put_multipart_opts(location, object_store::PutMultipartOptions::default())
        .await
        .map_err(|e| e.to_string())?;
    // Zero-copy slices: parts borrow the buffer, which outlives `complete()`.
    let mut remaining = bytes;
    let mut parts = Vec::with_capacity(remaining.len().div_ceil(part_size));
    while !remaining.is_empty() {
        let take = remaining.len().min(part_size);
        let chunk = remaining.split_to(take);
        // Part indices are assigned synchronously in call order; the returned
        // futures then upload concurrently.
        parts.push(upload.put_part(object_store::PutPayload::from(chunk)));
    }
    if let Err(e) = futures::future::try_join_all(parts).await {
        let _ = upload.abort().await;
        return Err(e.to_string());
    }
    match upload.complete().await {
        Ok(_) => Ok(()),
        Err(e) => {
            let _ = upload.abort().await;
            Err(e.to_string())
        }
    }
}

pub struct IcebergTableWriter {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub config: WriterConfig,
    pub generator: DefaultLocationGenerator,
    pub data_url: Url,
    // Typed partition tuple -> writer.
    writers: HashMap<Vec<Option<Literal>>, PartitionWriter>,
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
            let partition_values = Vec::new();
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
            self.write_aligned_batch(partition_values, partition_dir, aligned)
                .await?;
            return Ok(());
        }

        let parts = split_record_batch_by_partition(batch, spec, iceberg_schema)?;
        for p in parts.into_iter() {
            let partition_dir = p.partition_dir;
            let partition_values = p.partition_values;
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
            self.write_aligned_batch(partition_values, partition_dir, aligned)
                .await?;
        }

        Ok(())
    }

    async fn write_aligned_batch(
        &mut self,
        partition_values: Vec<Option<Literal>>,
        partition_dir: String,
        batch: RecordBatch,
    ) -> Result<(), String> {
        let (partition_dir, state) = match self.writers.remove(&partition_values) {
            Some(writer) => (writer.partition_dir, writer.state),
            None => (partition_dir, self.new_partition_writer_state()?),
        };
        let state = self.write_partition_state(state, batch).await?;

        let needs_roll = matches!(
            &state,
            PartitionWriterState::Open { writer, .. }
                if self.config.target_file_size > 0
                    && writer.buffered_size() >= self.config.target_file_size
        );
        if needs_roll {
            self.flush_partition(state, &partition_dir, partition_values)
                .await?;
            return Ok(());
        }

        self.writers.insert(
            partition_values,
            PartitionWriter {
                partition_dir,
                state,
            },
        );
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

    async fn flush_partition(
        &mut self,
        state: PartitionWriterState,
        partition_dir: &str,
        partition_values: Vec<Option<Literal>>,
    ) -> Result<(), String> {
        let writer = self.finish_partition_state(state).await?;
        let (bytes, meta) = writer.close().await?;
        let (rel, full) = self.generator.with_partition_dir(Some(partition_dir));
        log::trace!("iceberg.table_writer.flush_partition.writing: {}", full);
        if use_multipart_put(bytes.len() as u64) {
            put_file_multipart(self.store.as_ref(), &full, bytes, MULTIPART_PART_SIZE).await?;
        } else {
            self.store
                .put(&full, object_store::PutPayload::from(bytes))
                .await
                .map_err(|e| e.to_string())?;
        }
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
        for (partition_values, writer) in std::mem::take(&mut self.writers) {
            self.flush_partition(writer.state, &writer.partition_dir, partition_values)
                .await?;
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
    #![expect(clippy::expect_used)]

    use std::ops::Range;
    use std::sync::atomic::{AtomicBool, Ordering};

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::path::Path;
    use object_store::{
        GetOptions, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
        PutOptions, PutPayload, PutResult,
    };

    use super::*;

    fn test_bytes(len: usize) -> Vec<u8> {
        // Deterministic pseudo-random content (xorshift64).
        let mut state = 0x2545_f491_4f6c_dd1du64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 11) as u8
            })
            .collect()
    }

    #[test]
    fn multipart_threshold_boundary() {
        assert!(!use_multipart_put(MULTIPART_PUT_THRESHOLD - 1));
        assert!(use_multipart_put(MULTIPART_PUT_THRESHOLD));
        assert!(!use_multipart_put(0));
    }

    #[test]
    fn multipart_put_round_trips_bytes_identically() {
        futures::executor::block_on(async {
            let store = object_store::memory::InMemory::new();
            let path = Path::from("data/multipart.bin");
            // 2.5 MB in 1 MB parts exercises multi-part completion.
            let bytes = test_bytes(2_500_000);
            put_file_multipart(&store, &path, bytes::Bytes::from(bytes.clone()), 1_000_000)
                .await
                .expect("multipart upload");
            let back = store
                .get(&path)
                .await
                .expect("object present")
                .bytes()
                .await
                .expect("readable");
            assert_eq!(back.as_ref(), bytes.as_slice());
        });
    }

    #[test]
    fn multipart_put_rejects_empty_content() {
        futures::executor::block_on(async {
            let store = object_store::memory::InMemory::new();
            let err = put_file_multipart(
                &store,
                &Path::from("data/empty.bin"),
                bytes::Bytes::new(),
                1_000,
            )
            .await
            .expect_err("empty upload must fail");
            assert!(err.contains("non-empty"), "unexpected error: {err}");
        });
    }

    /// `MultipartUpload` decorator that fails the second part and records
    /// whether `abort()` was invoked.
    #[derive(Debug)]
    struct FailSecondPartUpload {
        inner: Box<dyn MultipartUpload>,
        parts: usize,
        aborted: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl MultipartUpload for FailSecondPartUpload {
        fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
            self.parts += 1;
            if self.parts >= 2 {
                return Box::pin(async move {
                    Err(object_store::Error::Generic {
                        store: "test",
                        source: Box::new(std::io::Error::other("injected part failure")),
                    })
                });
            }
            self.inner.put_part(data)
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.inner.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborted.store(true, Ordering::SeqCst);
            self.inner.abort().await
        }
    }

    /// `ObjectStore` that delegates to memory except for multipart uploads,
    /// which fail on the second part.
    #[derive(Debug)]
    struct FailSecondPartStore {
        memory: Arc<object_store::memory::InMemory>,
        aborted: Arc<AtomicBool>,
    }

    impl std::fmt::Display for FailSecondPartStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailSecondPartStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailSecondPartStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.memory.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            let inner = self.memory.put_multipart_opts(location, opts).await?;
            Ok(Box::new(FailSecondPartUpload {
                inner,
                parts: 0,
                aborted: Arc::clone(&self.aborted),
            }))
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.memory.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.memory.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.memory.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.memory.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.memory.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.memory.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn multipart_put_aborts_on_part_failure() {
        futures::executor::block_on(async {
            let aborted = Arc::new(AtomicBool::new(false));
            let store = FailSecondPartStore {
                memory: Arc::new(object_store::memory::InMemory::new()),
                aborted: Arc::clone(&aborted),
            };
            let path = Path::from("data/aborted.bin");
            let err = put_file_multipart(
                &store,
                &path,
                bytes::Bytes::from(test_bytes(2_500_000)),
                1_000_000,
            )
            .await
            .expect_err("part failure must fail the upload");
            assert!(
                err.contains("injected part failure"),
                "unexpected error: {err}"
            );
            assert!(
                aborted.load(Ordering::SeqCst),
                "failed upload must call abort()"
            );
            assert!(
                store.head(&path).await.is_err(),
                "aborted upload must leave no object"
            );
        });
    }
}

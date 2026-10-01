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

//! Shared parquet access helpers for Iceberg physical planning.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use futures::future::BoxFuture;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

/// An [`AsyncFileReader`] over an object store range, sized up front so the
/// reader does not need a `HEAD` request to learn the file length.
#[derive(Clone)]
pub(crate) struct ObjectStoreParquetReader {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    size: u64,
}

impl ObjectStoreParquetReader {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: ObjectPath, size: u64) -> Self {
        Self { store, path, size }
    }
}

impl AsyncFileReader for ObjectStoreParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        Box::pin(async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(parquet_object_store_error)
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        Box::pin(async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(parquet_object_store_error)
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let size = self.size;
        Box::pin(async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        })
    }
}

pub(crate) fn parquet_object_store_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

/// Parquet file metadata read from a footer, without decoding row data.
pub(crate) struct ParquetFooterInfo {
    pub parquet_metadata: ParquetMetaData,
    pub arrow_schema: SchemaRef,
    pub row_count: u64,
    pub file_size: u64,
}

/// Read the footer of a parquet file (schema, row count, column metadata).
pub(crate) async fn read_parquet_footer(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    file_size: u64,
) -> Result<ParquetFooterInfo, String> {
    let file_path = ObjectPath::from(path);
    let reader = ObjectStoreParquetReader::new(Arc::clone(store), file_path, file_size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| format!("failed to read parquet footer for {path}: {e}"))?;

    let parquet_metadata = builder.metadata().clone();
    let arrow_schema = builder.schema().clone();
    let row_count = parquet_metadata.file_metadata().num_rows() as u64;
    Ok(ParquetFooterInfo {
        parquet_metadata: parquet_metadata.as_ref().clone(),
        arrow_schema,
        row_count,
        file_size,
    })
}

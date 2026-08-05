use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::ParquetMetaData;

/// Result from reading a parquet file's footer metadata without decoding data rows.
pub(crate) struct ParquetFooterInfo {
    pub parquet_metadata: ParquetMetaData,
    pub arrow_schema: SchemaRef,
    pub row_count: u64,
    pub file_size: u64,
}

/// Read the footer (metadata + schema) of a parquet file from an object store.
///
/// This does NOT decode any row data — only the metadata + schema are loaded.
pub(crate) async fn read_parquet_footer(
    store: &Arc<dyn ObjectStore>,
    path: &str,
) -> Result<ParquetFooterInfo, String> {
    let file_path = object_store::path::Path::from(path);
    let file_meta = store
        .head(&file_path)
        .await
        .map_err(|e| format!("failed to head parquet file {path}: {e}"))?;
    let file_size = file_meta.size as u64;

    let reader =
        ParquetObjectReader::new(Arc::clone(store), file_path.clone()).with_file_size(file_size);

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

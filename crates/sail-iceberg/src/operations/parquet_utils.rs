use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use object_store::ObjectStore;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::ParquetMetaData;

pub(crate) struct ParquetFooterInfo {
    pub parquet_metadata: ParquetMetaData,
    pub arrow_schema: SchemaRef,
    pub row_count: u64,
    pub file_size: u64,
}

pub(crate) async fn read_parquet_footer(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    file_size: u64,
) -> Result<ParquetFooterInfo, String> {
    let file_path = object_store::path::Path::from(path);

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

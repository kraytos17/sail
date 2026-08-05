use std::sync::Arc;

use datafusion::arrow::compute::concat_batches;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use futures::stream::once;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use url::Url;

use crate::physical_plan::action_schema::{
    encode_add_data_files, encode_commit_meta, iceberg_action_schema, CommitMeta,
};
use crate::spec::{DataFile, Operation, TableRequirement};

/// An execution plan that emits a single iceberg-action-schema batch containing
/// pre-built `DataFile` records and a `CommitMeta`, without decoding or re-encoding
/// any data. Used by the `LOAD DATA` fast path.
///
/// This plan has no children; it is a source node whose `execute()` method
/// reads the pre-constructed DataFiles (from parquet footers, classified at plan time)
/// and encodes them into the action batch that `IcebergCommitExec` consumes.
#[derive(Debug)]
pub struct IcebergLoadDataFastExec {
    data_files: Vec<DataFile>,
    table_url: Url,
    operation: Operation,
    requirements: Vec<TableRequirement>,
    table_properties: Vec<(String, String)>,
    lakehouse_table: Option<LakehouseExecutionContext>,
    reported_row_count: u64,
    cache: Arc<PlanProperties>,
}

impl IcebergLoadDataFastExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_files: Vec<DataFile>,
        table_url: Url,
        operation: Operation,
        requirements: Vec<TableRequirement>,
        table_properties: Vec<(String, String)>,
        lakehouse_table: Option<LakehouseExecutionContext>,
        reported_row_count: u64,
    ) -> Self {
        let schema = iceberg_action_schema()
            .unwrap_or_else(|_| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            data_files,
            table_url,
            operation,
            requirements,
            table_properties,
            lakehouse_table,
            reported_row_count,
            cache,
        }
    }

    pub fn data_files(&self) -> &[DataFile] {
        &self.data_files
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }

    pub fn requirements(&self) -> &[TableRequirement] {
        &self.requirements
    }

    pub fn table_properties(&self) -> &[(String, String)] {
        &self.table_properties
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.lakehouse_table.as_ref()
    }

    pub fn reported_row_count(&self) -> u64 {
        self.reported_row_count
    }
}

impl ExecutionPlan for IcebergLoadDataFastExec {
    fn name(&self) -> &'static str {
        "IcebergLoadDataFastExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("IcebergLoadDataFastExec requires no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "IcebergLoadDataFastExec can only be executed in a single partition"
            );
        }

        let data_files = self.data_files.clone();
        let table_uri = self.table_url.to_string();
        let operation = self.operation.clone();
        let requirements = self.requirements.clone();
        let table_properties = self.table_properties.clone();
        let lakehouse_table = self.lakehouse_table.clone();
        let row_count_sum: u64 = data_files.iter().map(|df| df.record_count).sum();

        let future = async move {
            let commit_meta = CommitMeta {
                table_uri,
                row_count: row_count_sum,
                operation,
                requirements,
                table_properties,
                lakehouse_table,
                schema: None,
                partition_spec: None,
                touched_file_paths: vec![],
                overwrite_predicate: None,
                overwrite_partition_values: None,
            };

            let batch_schema = iceberg_action_schema()?;
            let batches = vec![
                encode_add_data_files(data_files)?,
                encode_commit_meta(commit_meta)?,
            ];
            let batch = concat_batches(&batch_schema, &batches)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            Ok(batch)
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for IcebergLoadDataFastExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergLoadDataFastExec(table_path={}, files={})",
                    self.table_url,
                    self.data_files.len()
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: iceberg")?;
                write!(
                    f,
                    "table_path={}, files={}",
                    self.table_url,
                    self.data_files.len()
                )
            }
        }
    }
}

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
use datafusion_common::{DataFusionError, Result, internal_err};
use futures::stream::once;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use url::Url;

use crate::physical_plan::action_schema::{
    CommitMeta, encode_add_data_files, encode_commit_meta, iceberg_action_schema,
};
use crate::spec::{DataFile, TableRequirement};

#[derive(Debug)]
pub struct IcebergLoadDataFastExec {
    data_files: Vec<DataFile>,
    table_url: Url,
    requirements: Vec<TableRequirement>,
    table_properties: Vec<(String, String)>,
    lakehouse_table: Option<LakehouseExecutionContext>,
    cache: Arc<PlanProperties>,
}

impl IcebergLoadDataFastExec {
    pub fn new(
        data_files: Vec<DataFile>,
        table_url: Url,
        requirements: Vec<TableRequirement>,
        table_properties: Vec<(String, String)>,
        lakehouse_table: Option<LakehouseExecutionContext>,
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
            requirements,
            table_properties,
            lakehouse_table,
            cache,
        }
    }

    pub fn data_files(&self) -> &[DataFile] {
        &self.data_files
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
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
        let requirements = self.requirements.clone();
        let table_properties = self.table_properties.clone();
        let lakehouse_table = self.lakehouse_table.clone();
        let row_count_sum: u64 = data_files.iter().map(|df| df.record_count).sum();

        let future = async move {
            let commit_meta = CommitMeta {
                table_uri,
                row_count: row_count_sum,
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

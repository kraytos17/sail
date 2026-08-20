use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::utils::items::ItemTaker;

/// Leaf extension node for `LOAD DATA INPATH '<path>' [OVERWRITE] INTO TABLE ns.tbl`.
///
/// This is a command node with no logical children. It carries the source location and
/// target table context; the physical planner (`plan_load_data` in sail-iceberg) performs
/// the file classification and builds the fast-register / rewrite plan.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct LoadDataNode {
    /// Source path: `s3a://…` file, glob, or directory.
    location: String,
    /// `LOAD DATA LOCAL` — not supported in v1; always `false`.
    local: bool,
    /// `OVERWRITE` → full-table replace.
    overwrite: bool,
    target_format: String,
    /// Table location.
    target_location: String,
    target_table_name: Vec<String>,
    target_options: Vec<OptionLayer>,
    target_lakehouse_table: Option<LakehouseExecutionContext>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl LoadDataNode {
    pub fn new(
        location: String,
        local: bool,
        overwrite: bool,
        target_format: String,
        target_location: String,
        target_table_name: Vec<String>,
        target_options: Vec<OptionLayer>,
        target_lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Self {
        Self {
            location,
            local,
            overwrite,
            target_format,
            target_location,
            target_table_name,
            target_options,
            target_lakehouse_table,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn is_local(&self) -> bool {
        self.local
    }

    pub fn overwrite(&self) -> bool {
        self.overwrite
    }

    pub fn target_format(&self) -> &str {
        &self.target_format
    }

    pub fn target_location(&self) -> &str {
        &self.target_location
    }

    pub fn target_table_name(&self) -> &[String] {
        &self.target_table_name
    }

    pub fn target_options(&self) -> &[OptionLayer] {
        &self.target_options
    }

    pub fn target_lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.target_lakehouse_table.as_ref()
    }
}

impl UserDefinedLogicalNodeCore for LoadDataNode {
    fn name(&self) -> &str {
        "LoadData"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        let table = self
            .target_table_name
            .last()
            .map(|s| s.as_str())
            .unwrap_or(&self.target_location);
        write!(
            f,
            "LoadData: table={}, format={}, path={}, overwrite={}",
            table, self.target_format, self.location, self.overwrite
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(Self {
            location: self.location.clone(),
            local: self.local,
            overwrite: self.overwrite,
            target_format: self.target_format.clone(),
            target_location: self.target_location.clone(),
            target_table_name: self.target_table_name.clone(),
            target_options: self.target_options.clone(),
            target_lakehouse_table: self.target_lakehouse_table.clone(),
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::utils::items::ItemTaker;
use serde::{Deserialize, Serialize};

/// A resolved `CALL <catalog>.system.<procedure>(...)` procedure.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum CallProcedure {
    /// `CALL <catalog>.system.rollback_to_snapshot('<ns>.<table>', <snapshot_id>)`
    RollbackToSnapshot { table: String, snapshot_id: i64 },
    /// `CALL <catalog>.system.set_current_snapshot('<ns>.<table>', <snapshot_id|ref>)`.
    ///
    /// Exactly one of `snapshot_id` or `ref` (a branch/tag name) is provided; the other is
    /// `None`. A `ref` is resolved to its referenced snapshot id at plan time.
    SetCurrentSnapshot {
        table: String,
        snapshot_id: Option<i64>,
        r#ref: Option<String>,
    },
    /// `CALL <catalog>.system.expire_snapshots('<ns>.<table>', [TIMESTAMP '<older_than>'], [<retain_last>])`.
    ///
    /// `older_than_ms` and `retain_last` are optional; defaults are resolved from table
    /// properties at plan time (`history.expire.max-snapshot-age-ms` / `min-snapshots-to-keep`).
    ExpireSnapshots {
        table: String,
        older_than_ms: Option<i64>,
        retain_last: Option<i32>,
    },
}

impl CallProcedure {
    /// The target table reference (`<ns>.<table>`) this procedure operates on.
    pub fn table_name(&self) -> &str {
        match self {
            CallProcedure::RollbackToSnapshot { table, .. }
            | CallProcedure::SetCurrentSnapshot { table, .. }
            | CallProcedure::ExpireSnapshots { table, .. } => table,
        }
    }
}

/// Leaf extension node for `CALL <catalog>.system.<procedure>(...)`.
///
/// This is a driver-side command node with no logical children. It carries the resolved
/// procedure and the target table context; the physical planner (`plan_call_procedure` in
/// sail-iceberg) loads the table metadata and commits the procedure's `TableUpdate`s.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct CallProcedureNode {
    /// The resolved procedure (name + validated arguments).
    procedure: CallProcedure,
    /// Table location.
    target_location: String,
    target_table_name: Vec<String>,
    target_options: Vec<OptionLayer>,
    target_lakehouse_table: Option<LakehouseExecutionContext>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl CallProcedureNode {
    pub fn new(
        procedure: CallProcedure,
        target_location: String,
        target_table_name: Vec<String>,
        target_options: Vec<OptionLayer>,
        target_lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Self {
        Self {
            procedure,
            target_location,
            target_table_name,
            target_options,
            target_lakehouse_table,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn procedure(&self) -> &CallProcedure {
        &self.procedure
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

impl UserDefinedLogicalNodeCore for CallProcedureNode {
    fn name(&self) -> &str {
        "CallProcedure"
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
            "CallProcedure: table={}, procedure={:?}",
            table, self.procedure
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(Self {
            procedure: self.procedure.clone(),
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

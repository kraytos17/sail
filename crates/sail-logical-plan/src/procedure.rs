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

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::lakesource::LakeSourceProcedureOperation;
use sail_common_datafusion::utils::items::ItemTaker;

/// Options for a `CALL <catalog>.system.<procedure>(...)` extension node.
///
/// The table format is resolved by the resolver from the target table (or an
/// explicit format-qualified procedure name); the physical planner dispatches
/// on it to the format's own planner.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct ProcedureOptions {
    pub format: String,
    pub procedure_name: Vec<String>,
    pub operation: LakeSourceProcedureOperation,
    pub target_table: Option<Vec<String>>,
    pub target_path: Option<String>,
    pub target_options: Vec<OptionLayer>,
    pub target_lakehouse_table: Option<LakehouseExecutionContext>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct ProcedureNode {
    #[educe(PartialOrd(ignore))]
    options: ProcedureOptions,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl ProcedureNode {
    pub fn new(options: ProcedureOptions) -> Self {
        Self {
            options,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn options(&self) -> &ProcedureOptions {
        &self.options
    }
}

impl UserDefinedLogicalNodeCore for ProcedureNode {
    fn name(&self) -> &str {
        "Procedure"
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
        write!(f, "Procedure: options={:?}", self.options)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(self.clone())
    }
}

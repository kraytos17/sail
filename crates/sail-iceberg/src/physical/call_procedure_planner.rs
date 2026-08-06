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

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use sail_logical_plan::call_procedure::{CallProcedure, CallProcedureNode};

use crate::physical_plan::call_procedure_exec::{
    compute_procedure_updates, procedure_requirements, resolve_target_snapshot_id,
    CallProcedureOutput,
};
use crate::physical_plan::CallProcedureExec;
use crate::table::Table;
use crate::table_format::IcebergTableFormat;

/// Plans `CALL <catalog>.system.<procedure>(...)` into a driver-side
/// [`CallProcedureExec`].
///
/// Loads the target table metadata at plan time, computes the procedure's
/// `TableUpdate`s (validating arguments such as snapshot existence) and its spec-shaped
/// output row, and passes them into the exec, which performs the commit at execution time.
pub async fn plan_call_procedure(
    session_state: &SessionState,
    node: &CallProcedureNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table_url =
        IcebergTableFormat::parse_table_url(vec![node.target_location().to_string()]).await?;

    let table = Table::load(session_state, table_url.clone()).await?;
    let metadata = table.metadata();
    let updates = compute_procedure_updates(node.procedure(), metadata)?;
    let requirements = procedure_requirements(metadata);
    let output = compute_procedure_output(node.procedure(), metadata)?;

    // Capture the plan-time metadata for procedures that need it after the commit
    // (expire_snapshots computes its physical-GC candidates from the pre-commit state).
    let pre_commit_metadata =
        matches!(node.procedure(), CallProcedure::ExpireSnapshots { .. }).then(|| metadata.clone());

    let exec = CallProcedureExec::new_with_pre_commit_metadata(
        node.procedure().clone(),
        table_url,
        node.target_lakehouse_table().cloned(),
        updates,
        requirements,
        output,
        pre_commit_metadata,
    );
    Ok(Arc::new(exec))
}

/// Computes the spec-shaped output row for a procedure against the current metadata.
///
/// `previous_snapshot_id` is the `main` ref's snapshot id before the commit (the same
/// value the `RefSnapshotIdMatch` requirement asserts); `current_snapshot_id` is the
/// procedure's target snapshot.
fn compute_procedure_output(
    procedure: &CallProcedure,
    metadata: &crate::spec::TableMetadata,
) -> Result<CallProcedureOutput> {
    match procedure {
        CallProcedure::RollbackToSnapshot { .. } | CallProcedure::SetCurrentSnapshot { .. } => {
            let previous = metadata
                .refs
                .get(crate::spec::snapshots::MAIN_BRANCH)
                .map(|r| r.snapshot_id)
                .or(metadata.current_snapshot_id)
                .unwrap_or(0);
            let current = resolve_target_snapshot_id(procedure, metadata)?.unwrap_or(0);
            Ok(CallProcedureOutput::SnapshotRef {
                previous_snapshot_id: previous,
                current_snapshot_id: current,
            })
        }
        CallProcedure::ExpireSnapshots { .. } => Ok(CallProcedureOutput::ExpireSnapshots {
            // Real counts are filled in at execution time after the physical GC pass.
            deleted_data_files_count: 0,
            deleted_position_delete_files_count: 0,
            deleted_equality_delete_files_count: 0,
            deleted_manifest_files_count: 0,
            deleted_manifest_lists_count: 0,
            deleted_statistics_files_count: 0,
        }),
    }
}

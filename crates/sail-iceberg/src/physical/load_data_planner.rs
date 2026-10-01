use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{Result, not_impl_err};
use datafusion::physical_plan::ExecutionPlan;
use sail_logical_plan::load_data::LoadDataNode;

/// Physical planning for `LOAD DATA ... INTO TABLE`.
///
/// The source classification and the fast/fallback split land in a follow-up
/// stage; until then the boundary is explicit rather than silently omitted.
pub(crate) async fn plan_load_data(
    _session: &dyn Session,
    _node: &LoadDataNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    not_impl_err!("LOAD DATA execution")
}

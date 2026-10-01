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

//! Physical planning for `LOAD DATA ... INTO TABLE <iceberg table>`.

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use sail_logical_plan::load_data::LoadDataNode;

use crate::lake_source::{
    IcebergLakeSource, catalog_managed_iceberg_from_options, metadata_location_from_options,
    resolve_iceberg_metadata_location,
};
use crate::operations::SnapshotUpdateKind;
use crate::physical::load_classifier::classify_source_files;
use crate::physical_plan::{IcebergCommitExec, IcebergLoadDataFastExec};
use crate::spec::TableRequirement;
use crate::table::Table;
use crate::utils::get_object_store_from_session;

pub(crate) async fn plan_load_data(
    session: &dyn Session,
    node: &LoadDataNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table_url =
        IcebergLakeSource::parse_table_url(vec![node.target_location().to_string()]).await?;

    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let metadata_location_for_load = resolve_iceberg_metadata_location(
        node.target_lakehouse_table(),
        metadata_location,
        catalog_managed_table,
    )?;
    let table =
        Table::load_with_metadata_location(session, table_url.clone(), metadata_location_for_load)
            .await?;

    // Row lineage needs no action here. For V3 the commit assigns per-file
    // `first_row_id` from the table's next row id (`materialize_inherited_entry`
    // in `operations/snapshot.rs`), skipping any file that already carries one.
    // Below V3 there is no lineage at all. The classifier therefore leaves
    // `first_row_id` as `None` on purpose: filling it in at plan time would make
    // the commit skip the file and leave a gap in the numbering.
    let metadata = table.metadata();
    let requirements = vec![
        TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: metadata.last_column_id,
        },
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id,
        },
    ];

    let table_schema = metadata.current_schema().ok_or_else(|| {
        DataFusionError::Plan("LOAD DATA: table has no current schema".to_string())
    })?;
    let table_arrow_schema =
        crate::datasource::type_converter::iceberg_schema_to_arrow(table_schema)?;
    let default_spec = metadata.default_partition_spec();
    let spec_id = default_spec.map(|s| s.spec_id()).unwrap_or(0);
    let partitioned = default_spec.is_some_and(|spec| !spec.fields().is_empty());

    // The listing prefix is everything before the first glob, so the store can
    // be resolved without parsing the pattern itself.
    let glob_cut = node.location().find('*').unwrap_or(node.location().len());
    let source_url = url::Url::parse(&node.location()[..glob_cut]).map_err(|e| {
        DataFusionError::Plan(format!(
            "invalid source location '{}': {e}",
            node.location()
        ))
    })?;
    let source_store = get_object_store_from_session(session, &source_url)?;

    let classified = classify_source_files(
        source_store,
        &source_url,
        node.location(),
        table_schema,
        &table_arrow_schema,
        spec_id,
        // A partitioned table must rewrite so partition values come from the path.
        !partitioned,
    )
    .await?;

    if !classified.fallback_files.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "LOAD DATA fallback scan (rewriting source files)".to_string(),
        ));
    }

    let snapshot_update_kind = if node.overwrite() {
        SnapshotUpdateKind::FullOverwrite
    } else {
        SnapshotUpdateKind::FastAppend
    };

    let fast_exec: Arc<dyn ExecutionPlan> = Arc::new(IcebergLoadDataFastExec::new(
        classified.fast_files,
        table_url.clone(),
        requirements,
        // Table properties are applied only when bootstrapping new metadata.
        Vec::new(),
        node.target_lakehouse_table().cloned(),
    ));

    Ok(Arc::new(IcebergCommitExec::new(
        fast_exec,
        table_url,
        node.target_lakehouse_table().cloned(),
        snapshot_update_kind,
    )))
}

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

//! Read-only `TableProvider`s for the Iceberg metadata tables `snapshots` and
//! `refs`, served as single-batch `DataSourceExec` scans so the optimizer and
//! batch-coalescing pipeline apply unchanged.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Int32Builder, Int64Builder, MapArray, StringArray, StringBuilder, StructArray,
    TimestampMicrosecondBuilder,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::TableType;
use datafusion::logical_expr::expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::spec::{Snapshot, SnapshotRetention, TableMetadata};

/// A read-only `TableProvider` exposing an Iceberg metadata table.
#[derive(Debug)]
pub struct IcebergMetadataTableProvider {
    metadata: TableMetadata,
    metadata_type: sail_common_datafusion::catalog::iceberg::IcebergMetadataTableType,
    schema: SchemaRef,
}

impl IcebergMetadataTableProvider {
    pub fn new(
        metadata: TableMetadata,
        metadata_type: sail_common_datafusion::catalog::iceberg::IcebergMetadataTableType,
    ) -> Self {
        use sail_common_datafusion::catalog::iceberg::IcebergMetadataTableType;
        let schema = Arc::new(match metadata_type {
            IcebergMetadataTableType::Snapshots => snapshots_schema(),
            IcebergMetadataTableType::Refs => refs_schema(),
        });
        Self {
            metadata,
            metadata_type,
            schema,
        }
    }

    pub fn build_batch(&self) -> Result<RecordBatch> {
        use sail_common_datafusion::catalog::iceberg::IcebergMetadataTableType;
        match self.metadata_type {
            IcebergMetadataTableType::Snapshots => snapshots_batch(&self.metadata),
            IcebergMetadataTableType::Refs => refs_batch(&self.metadata),
        }
    }
}

#[async_trait]
impl TableProvider for IcebergMetadataTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        Ok(vec![
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch = self.build_batch()?;
        // The projection is applied by `MemorySourceConfig` at execution time
        // (its `schema` remains the full schema); an empty projection falls back
        // to reading every column.
        let projection = projection.filter(|p| !p.is_empty()).cloned();
        let source =
            MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&self.schema), projection)?;
        Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
    }
}

fn snapshots_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "committed_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("operation", DataType::Utf8, true),
        Field::new("manifest_list", DataType::Utf8, true),
        Field::new(
            "summary",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, false),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ])
}

fn snapshots_batch(metadata: &TableMetadata) -> Result<RecordBatch> {
    let schema = Arc::new(snapshots_schema());
    let snapshots = &metadata.snapshots;

    let mut committed_at = TimestampMicrosecondBuilder::with_capacity(snapshots.len());
    let mut snapshot_id = Int64Builder::with_capacity(snapshots.len());
    let mut parent_id = Int64Builder::with_capacity(snapshots.len());
    let mut operation = StringBuilder::with_capacity(snapshots.len(), 16);
    let mut manifest_list = StringBuilder::with_capacity(snapshots.len(), 64);

    for s in snapshots {
        committed_at.append_value(s.timestamp_ms() * 1000);
        snapshot_id.append_value(s.snapshot_id());
        match s.parent_snapshot_id() {
            Some(id) => parent_id.append_value(id),
            None => parent_id.append_null(),
        }
        operation.append_value(s.summary().operation.as_str());
        if s.manifest_list().is_empty() {
            manifest_list.append_null();
        } else {
            manifest_list.append_value(s.manifest_list());
        }
    }

    let summary = build_summary_map(snapshots)?;

    let columns: Vec<ArrayRef> = vec![
        Arc::new(committed_at.finish()),
        Arc::new(snapshot_id.finish()),
        Arc::new(parent_id.finish()),
        Arc::new(operation.finish()),
        Arc::new(manifest_list.finish()),
        summary,
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn build_summary_map(snapshots: &[Snapshot]) -> Result<ArrayRef> {
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        false,
    ));

    let mut keys: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(snapshots.len() + 1);
    offsets.push(0);

    for s in snapshots {
        keys.push("operation".to_string());
        values.push(s.summary().operation.as_str().to_string());
        for (k, v) in &s.summary().additional_properties {
            keys.push(k.clone());
            values.push(v.clone());
        }
        offsets.push(keys.len() as i32);
    }

    let key_array = Arc::new(StringArray::from(keys)) as ArrayRef;
    let value_array = Arc::new(StringArray::from(values)) as ArrayRef;
    let entries_struct = StructArray::try_new(
        match entries_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        vec![key_array, value_array],
        None,
    )?;
    let map_array = MapArray::try_new(
        entries_field,
        OffsetBuffer::new(offsets.into()),
        entries_struct,
        None,
        false,
    )?;
    Ok(Arc::new(map_array))
}

fn refs_schema() -> Schema {
    Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("max_reference_age_in_ms", DataType::Int64, true),
        Field::new("min_snapshots_to_keep", DataType::Int32, true),
        Field::new("max_snapshot_age_in_ms", DataType::Int64, true),
    ])
}

fn refs_batch(metadata: &TableMetadata) -> Result<RecordBatch> {
    let schema = Arc::new(refs_schema());
    let refs = &metadata.refs;
    let names: Vec<&String> = refs.keys().collect();

    let mut name = StringBuilder::with_capacity(names.len(), 16);
    let mut ref_type = StringBuilder::with_capacity(names.len(), 8);
    let mut snapshot_id = Int64Builder::with_capacity(names.len());
    let mut max_ref_age = Int64Builder::with_capacity(names.len());
    let mut min_snapshots = Int32Builder::with_capacity(names.len());
    let mut max_snapshot_age = Int64Builder::with_capacity(names.len());

    for n in &names {
        let r = &refs[*n];
        name.append_value(n.as_str());
        ref_type.append_value(if r.is_branch() { "branch" } else { "tag" });
        snapshot_id.append_value(r.snapshot_id);
        match &r.retention {
            SnapshotRetention::Branch {
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                max_ref_age_ms,
            } => {
                match max_ref_age_ms {
                    Some(v) => max_ref_age.append_value(*v),
                    None => max_ref_age.append_null(),
                }
                match min_snapshots_to_keep {
                    Some(v) => min_snapshots.append_value(*v),
                    None => min_snapshots.append_null(),
                }
                match max_snapshot_age_ms {
                    Some(v) => max_snapshot_age.append_value(*v),
                    None => max_snapshot_age.append_null(),
                }
            }
            SnapshotRetention::Tag { max_ref_age_ms } => {
                match max_ref_age_ms {
                    Some(v) => max_ref_age.append_value(*v),
                    None => max_ref_age.append_null(),
                }
                min_snapshots.append_null();
                max_snapshot_age.append_null();
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(name.finish()),
        Arc::new(ref_type.finish()),
        Arc::new(snapshot_id.finish()),
        Arc::new(max_ref_age.finish()),
        Arc::new(min_snapshots.finish()),
        Arc::new(max_snapshot_age.finish()),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use sail_common_datafusion::catalog::iceberg::IcebergMetadataTableType;

    use super::*;
    use crate::spec::snapshots::{Snapshot, SnapshotReference, SnapshotRetention, Summary};
    use crate::spec::{
        FormatVersion, Operation, PartitionSpec, Schema as IcebergSchema, TableMetadata,
    };

    fn schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_schema_id(0)
            .build()
            .expect("schema")
    }

    fn snapshot(id: i64, parent: Option<i64>) -> Snapshot {
        let mut builder = Snapshot::builder()
            .with_snapshot_id(id)
            .with_sequence_number(id)
            .with_timestamp_ms(id * 1_000)
            .with_manifest_list(format!("metadata/snap-{id}.avro"))
            .with_summary(Summary::new(Operation::Append).with_property("added-records", "3"));
        if let Some(parent) = parent {
            builder = builder.with_parent_snapshot_id(parent);
        }
        builder.build().expect("snapshot")
    }

    fn metadata(
        snapshots: Vec<Snapshot>,
        refs: HashMap<String, SnapshotReference>,
    ) -> TableMetadata {
        let current = snapshots.iter().map(|s| s.snapshot_id()).max().unwrap_or(0);
        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: None,
            location: "s3://bucket/tbl".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 0,
            last_column_id: 0,
            schemas: vec![schema()],
            current_schema_id: 0,
            partition_specs: vec![PartitionSpec::unpartitioned_spec()],
            default_spec_id: 0,
            last_partition_id: 0,
            properties: HashMap::new(),
            current_snapshot_id: (current > 0).then_some(current),
            next_row_id: None,
            encryption_keys: vec![],
            snapshots,
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![],
            default_sort_order_id: None,
            refs,
            statistics: vec![],
            partition_statistics: vec![],
        }
    }

    #[test]
    fn snapshots_batch_materializes_rows() {
        let metadata = metadata(
            vec![snapshot(1, None), snapshot(2, Some(1))],
            HashMap::new(),
        );
        let provider =
            IcebergMetadataTableProvider::new(metadata, IcebergMetadataTableType::Snapshots);
        let batch = provider.build_batch().expect("snapshots batch");
        assert_eq!(batch.num_rows(), 2);

        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot_id column");
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);

        // parent_id is null for the first snapshot, set for the second.
        let parents = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("parent_id column");
        assert!(parents.is_null(0));
        assert_eq!(parents.value(1), 1);

        let operation = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("operation column");
        assert_eq!(operation.value(0), "append");
    }

    #[tokio::test]
    async fn scan_applies_projection_and_empty_projection_reads_all_columns() {
        use datafusion::physical_plan::ExecutionPlanProperties;
        use datafusion::prelude::SessionContext;

        let metadata = metadata(vec![snapshot(1, None)], HashMap::new());
        let provider =
            IcebergMetadataTableProvider::new(metadata, IcebergMetadataTableType::Snapshots);
        let ctx = SessionContext::new();

        // A single-column projection yields a one-column scan.
        let projected = provider
            .scan(&ctx.state(), Some(&vec![1usize]), &[], None)
            .await
            .expect("projected scan");
        assert_eq!(projected.schema().fields().len(), 1);
        assert_eq!(projected.schema().field(0).name(), "snapshot_id");
        assert_eq!(projected.output_partitioning().partition_count(), 1);

        // An empty projection falls back to the full schema.
        let full = provider
            .scan(&ctx.state(), Some(&vec![]), &[], None)
            .await
            .expect("full scan");
        assert_eq!(full.schema().fields().len(), 6);
    }

    #[test]
    fn refs_batch_distinguishes_branch_and_tag() {
        let refs = HashMap::from([
            (
                "main".to_string(),
                SnapshotReference {
                    snapshot_id: 2,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: Some(2),
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: Some(10),
                    },
                },
            ),
            (
                "audit".to_string(),
                SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                },
            ),
        ]);
        let metadata = metadata(vec![snapshot(1, None), snapshot(2, Some(1))], refs);
        let provider = IcebergMetadataTableProvider::new(metadata, IcebergMetadataTableType::Refs);
        let batch = provider.build_batch().expect("refs batch");
        assert_eq!(batch.num_rows(), 2);

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("type column");
        let row = |name: &str| (0..batch.num_rows()).position(|i| names.value(i) == name);
        let main = row("main").expect("main ref");
        let audit = row("audit").expect("audit ref");
        assert_eq!(types.value(main), "branch");
        assert_eq!(types.value(audit), "tag");

        // Branch carries min_snapshots_to_keep; tag leaves it null.
        let min_keep = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("min_snapshots column");
        assert_eq!(min_keep.value(main), 2);
        assert!(min_keep.is_null(audit));
    }
}

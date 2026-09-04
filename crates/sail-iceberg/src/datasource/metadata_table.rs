use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Int32Builder, Int64Builder, MapArray, RecordBatch, StringArray, StringBuilder,
    StructArray, TimestampMicrosecondBuilder,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::catalog::IcebergMetadataTableType;

use crate::spec::snapshots::SnapshotRetention;
use crate::spec::{Snapshot, TableMetadata};

#[derive(Debug)]
pub(crate) struct IcebergMetadataTableProvider {
    metadata: TableMetadata,
    metadata_type: IcebergMetadataTableType,
    schema: SchemaRef,
}

impl IcebergMetadataTableProvider {
    pub fn new(
        _table_uri: impl ToString,
        metadata: TableMetadata,
        metadata_type: IcebergMetadataTableType,
    ) -> Self {
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
        _filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            _filters.len()
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
        let source =
            MemorySourceConfig::try_new(&[vec![batch]], self.schema(), projection.cloned())?;
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
        Arc::new(summary),
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
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::collections::HashMap;

    use datafusion::arrow::array::Array;

    use super::*;
    use crate::spec::snapshots::{SnapshotReference, SnapshotRetention, Summary};
    use crate::spec::{FormatVersion, Operation, Schema};

    fn sample_metadata() -> TableMetadata {
        let mut table_meta = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: None,
            location: "s3://bucket/tbl".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 0,
            last_column_id: 0,
            schemas: vec![Schema::builder().with_schema_id(0).build().unwrap()],
            current_schema_id: 0,
            partition_specs: vec![
                crate::spec::partition::spec::PartitionSpec::unpartitioned_spec(),
            ],
            default_spec_id: 0,
            last_partition_id: 0,
            properties: HashMap::new(),
            current_snapshot_id: None,
            next_row_id: None,
            encryption_keys: vec![],
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![],
            default_sort_order_id: None,
            refs: HashMap::new(),
            statistics: vec![],
            partition_statistics: vec![],
        };

        let snap1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(1_000)
            .with_manifest_list("s3://bucket/m1.avro")
            .with_summary(Summary::new(Operation::Append))
            .build()
            .unwrap();
        let snap2 = Snapshot::builder()
            .with_snapshot_id(2)
            .with_parent_snapshot_id(1)
            .with_sequence_number(2)
            .with_timestamp_ms(2_000)
            .with_manifest_list("s3://bucket/m2.avro")
            .with_summary(Summary::new(Operation::Overwrite))
            .build()
            .unwrap();
        table_meta.snapshots = vec![snap1, snap2];

        table_meta.refs = HashMap::from([
            (
                "main".to_string(),
                SnapshotReference {
                    snapshot_id: 2,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: Some(10),
                        max_snapshot_age_ms: Some(86_400_000),
                        max_ref_age_ms: Some(1_728_000_000),
                    },
                },
            ),
            (
                "v1".to_string(),
                SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Tag {
                        max_ref_age_ms: Some(1000),
                    },
                },
            ),
        ]);
        table_meta
    }

    #[test]
    fn snapshots_metadata_table_materializes_rows() {
        let provider = IcebergMetadataTableProvider::new(
            "s3://bucket/tbl",
            sample_metadata(),
            IcebergMetadataTableType::Snapshots,
        );
        let batch = provider.build_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let snapshot_id = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(snapshot_id.values(), &[1, 2]);

        let parent_id = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert!(parent_id.is_null(0));
        assert_eq!(parent_id.value(1), 1);
    }

    #[test]
    fn refs_metadata_table_materializes_rows() {
        let provider = IcebergMetadataTableProvider::new(
            "s3://bucket/tbl",
            sample_metadata(),
            IcebergMetadataTableType::Refs,
        );
        let batch = provider.build_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let name = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let ref_type = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let snapshot_id = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();

        let mut by_name = std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            by_name.insert(
                name.value(i).to_string(),
                (ref_type.value(i).to_string(), snapshot_id.value(i)),
            );
        }
        assert_eq!(by_name["main"], ("branch".to_string(), 2));
        assert_eq!(by_name["v1"], ("tag".to_string(), 1));
    }
}

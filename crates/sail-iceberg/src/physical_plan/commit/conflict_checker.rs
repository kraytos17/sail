use std::collections::HashSet;

use datafusion_common::{DataFusionError, Result};
use log::{debug, warn};

use super::IcebergCommitInfo;
use crate::spec::{
    DataFile, Operation, PartitionSpec, TableMetadata, TableRequirement, TableUpdate,
};

/// Identifies a specific partition value for conflict detection.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct PartitionKey {
    pub spec_id: i32,
    /// Stringified partition values (field_name -> value_string).
    pub values: Vec<(String, String)>,
}

impl PartitionKey {
    pub fn from_data_file(file: &DataFile, spec: &PartitionSpec) -> Self {
        let fields = spec.fields();
        let values: Vec<_> = file
            .partition()
            .iter()
            .zip(fields.iter())
            .map(|(value, field)| (field.name.clone(), format!("{:?}", value)))
            .collect();
        Self {
            spec_id: spec.spec_id(),
            values,
        }
    }
}

/// Schema-level changes made by a transaction.
#[derive(Debug, Clone)]
pub struct TransactionSchemaChange {
    pub old_schema_id: i32,
    pub new_schema_id: i32,
    pub added_columns: Vec<String>,
    pub removed_columns: Vec<String>,
    /// (column_name, old_type, new_type)
    pub type_changes: Vec<(String, String, String)>,
    /// (old_name, new_name)
    pub renames: Vec<(String, String)>,
}

/// Reason for semantic conflict.
#[derive(Debug, Clone)]
pub enum ConflictReason {
    /// Table UUID changed (table was recreated).
    TableUuidChanged {
        expected: Option<uuid::Uuid>,
        actual: Option<uuid::Uuid>,
    },
    /// Incompatible schema changes between concurrent transactions.
    IncompatibleSchemaChange { detail: String },
    /// Both transactions wrote to the same partition.
    ConcurrentOverwrite {
        partition: String,
        winning_operation: Operation,
        our_operation: Operation,
    },
    /// Winning commit deleted files that our transaction depends on.
    ConcurrentDeleteRead { files: Vec<String> },
    /// Both transactions deleted the same files.
    ConcurrentDeleteDelete { files: Vec<String> },
    /// Current schema ID changed (our schema change conflicts).
    SchemaIdChanged { expected: i32, actual: i32 },
    /// Field ID changed (our partition/schema change conflicts).
    FieldIdChanged { expected: i32, actual: i32 },
    /// Uncategorized conflict — treated as semantic, fails immediately.
    Unknown { message: String },
}

/// Result of running conflict detection.
#[derive(Debug)]
pub enum ConflictCheckResult {
    /// No semantic conflict — transaction can proceed with updated state.
    NoConflict {
        /// Updated metadata reflecting winning commit's changes.
        updated_metadata: TableMetadata,
    },
    /// Semantic conflict detected — transaction must fail.
    Conflict {
        /// Reason for the conflict.
        reason: ConflictReason,
    },
}

/// Information about the current (rejected) transaction.
pub struct TransactionInfo {
    /// Table metadata at transaction start (what we loaded).
    pub read_metadata: TableMetadata,

    /// Operation being performed.
    pub operation: Operation,

    /// Data files being added by our transaction.
    pub added_files: Vec<DataFile>,

    /// Data files being removed (for Overwrite/Delete/Replace).
    pub removed_files: Vec<DataFile>,

    /// Partition values affected by our transaction.
    pub partition_values: HashSet<PartitionKey>,

    /// Whether our transaction read the entire table.
    pub read_whole_table: bool,

    /// Schema changes (if any) — None means no schema change.
    pub schema_changes: Option<TransactionSchemaChange>,

    /// Our transaction's commit requirements.
    pub requirements: Vec<TableRequirement>,

    /// Our transaction's metadata updates.
    pub updates: Vec<TableUpdate>,
}

impl TransactionInfo {
    /// Build a `TransactionInfo` from the commit execution context.
    pub fn new(read_metadata: &TableMetadata, commit_info: &IcebergCommitInfo) -> Self {
        let partition_spec = read_metadata
            .default_partition_spec()
            .cloned()
            .unwrap_or_else(PartitionSpec::unpartitioned_spec);

        let partition_values: HashSet<PartitionKey> = commit_info
            .data_files
            .iter()
            .map(|f| PartitionKey::from_data_file(f, &partition_spec))
            .collect();

        Self {
            read_metadata: read_metadata.clone(),
            operation: commit_info.operation.clone(),
            added_files: commit_info.data_files.clone(),
            removed_files: Vec::new(),
            partition_values,
            read_whole_table: false,
            schema_changes: None,
            requirements: commit_info.requirements.clone(),
            updates: commit_info.updates.clone(),
        }
    }
}

/// Summary of the commit that succeeded ahead of ours.
pub struct WinningCommitSummary {
    /// Full table metadata AFTER the winning commit.
    pub metadata: TableMetadata,

    /// Operation performed by the winning commit.
    pub operation: Operation,

    /// Files added by the winning commit.
    pub added_files: Vec<DataFile>,

    /// Files removed by the winning commit.
    pub removed_files: Vec<DataFile>,

    /// Partition values affected by the winning commit.
    pub partition_values: HashSet<PartitionKey>,

    /// Schema changes by the winning commit.
    pub schema_changes: Option<TransactionSchemaChange>,

    /// Snapshot ID of the winning commit.
    pub snapshot_id: i64,

    /// Parent snapshot ID (what the winning commit read).
    pub parent_snapshot_id: Option<i64>,
}

/// Semantic conflict checker for Iceberg commits.
///
/// Analyzes the winning commit to determine if a rejected transaction
/// can safely proceed or must fail.
pub struct ConflictChecker {
    pub transaction: TransactionInfo,
    pub winning_commit: WinningCommitSummary,
}

impl ConflictChecker {
    /// Run all conflict checks and return the result.
    pub fn check_conflicts(&self) -> Result<ConflictCheckResult> {
        debug!(
            "Starting Iceberg conflict check: our_op={:?}, winning_op={:?}",
            self.transaction.operation, self.winning_commit.operation
        );

        self.check_table_uuid()?;
        self.check_schema_compatibility()?;
        self.check_partition_conflicts()?;
        self.check_file_conflicts()?;

        debug!("No semantic conflicts detected");

        let updated_metadata = self.winning_commit.metadata.clone();

        Ok(ConflictCheckResult::NoConflict { updated_metadata })
    }

    fn check_table_uuid(&self) -> Result<()> {
        let expected = self.transaction.read_metadata.table_uuid;
        let actual = self.winning_commit.metadata.table_uuid;

        if expected != actual {
            warn!(
                "Table UUID changed: expected {:?}, actual {:?}",
                expected, actual
            );
            return Err(DataFusionError::Plan(format!(
                "Iceberg commit conflict: table UUID changed (expected {:?}, found {:?})",
                expected, actual
            )));
        }

        Ok(())
    }

    fn check_schema_compatibility(&self) -> Result<()> {
        let our_change = match &self.transaction.schema_changes {
            Some(c) => c,
            None => return Ok(()),
        };

        let winning_change = match &self.winning_commit.schema_changes {
            Some(c) => c,
            None => {
                let expected_schema_id = our_change.old_schema_id;
                let actual_schema_id = self.winning_commit.metadata.current_schema_id;

                if actual_schema_id != expected_schema_id
                    && actual_schema_id != our_change.new_schema_id
                {
                    return Err(DataFusionError::Plan(format!(
                        "Iceberg commit conflict: schema ID changed (expected {}, found {})",
                        expected_schema_id, actual_schema_id,
                    )));
                }
                return Ok(());
            }
        };

        if !our_change.type_changes.is_empty() || !winning_change.type_changes.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Iceberg commit conflict: concurrent type changes (ours={:?}, winning={:?})",
                our_change.type_changes, winning_change.type_changes,
            )));
        }

        if !our_change.removed_columns.is_empty() || !winning_change.removed_columns.is_empty() {
            return Err(DataFusionError::Plan(
                "Iceberg commit conflict: concurrent column removals".to_string(),
            ));
        }

        let our_added: HashSet<_> = our_change.added_columns.iter().collect();
        let winning_added: HashSet<_> = winning_change.added_columns.iter().collect();
        let overlap: Vec<_> = our_added.intersection(&winning_added).collect();

        if !overlap.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Iceberg commit conflict: both transactions added columns {:?}",
                overlap,
            )));
        }

        Ok(())
    }

    fn check_partition_conflicts(&self) -> Result<()> {
        if self.transaction.partition_values.is_empty()
            || self.winning_commit.partition_values.is_empty()
        {
            return Ok(());
        }

        let overlap: HashSet<_> = self
            .transaction
            .partition_values
            .intersection(&self.winning_commit.partition_values)
            .collect();

        if overlap.is_empty() {
            return Ok(());
        }

        // Case 1: Both are pure appends to the same partition — safe.
        if self.transaction.operation == Operation::Append
            && self.winning_commit.operation == Operation::Append
        {
            return Ok(());
        }

        // Case 2: Our append + winning overwrite → conflict.
        if self.transaction.operation == Operation::Append
            && self.winning_commit.operation == Operation::Overwrite
        {
            let partition_str = format!("{:?}", overlap.iter().next().unwrap());
            return Err(DataFusionError::Plan(format!(
                "Iceberg commit conflict: concurrent overwrite and append on partition {}",
                partition_str,
            )));
        }

        // Case 3: Overwrite + anything on same partition → conflict.
        if self.transaction.operation == Operation::Overwrite
            || self.winning_commit.operation == Operation::Overwrite
        {
            let partition_str = format!("{:?}", overlap.iter().next().unwrap());
            return Err(DataFusionError::Plan(format!(
                "Iceberg commit conflict: concurrent overwrite on partition {} (our_op={:?}, winning_op={:?})",
                partition_str, self.transaction.operation, self.winning_commit.operation,
            )));
        }

        // Case 4: Two deletes on same partition — fall through to file-level checks.
        if self.transaction.operation == Operation::Delete
            && self.winning_commit.operation == Operation::Delete
        {
            return Ok(());
        }

        let partition_str = format!("{:?}", overlap.iter().next().unwrap());
        Err(DataFusionError::Plan(format!(
            "Iceberg commit conflict: concurrent operations on partition {} (our_op={:?}, winning_op={:?})",
            partition_str, self.transaction.operation, self.winning_commit.operation,
        )))
    }

    fn check_file_conflicts(&self) -> Result<()> {
        let our_file_set: HashSet<&str> = self
            .transaction
            .added_files
            .iter()
            .chain(self.transaction.removed_files.iter())
            .map(|f| f.file_path.as_str())
            .collect();

        let winning_removed_set: HashSet<&str> = self
            .winning_commit
            .removed_files
            .iter()
            .map(|f| f.file_path.as_str())
            .collect();

        let files_deleted_by_winner: Vec<_> =
            our_file_set.intersection(&winning_removed_set).collect();

        if !files_deleted_by_winner.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Iceberg commit conflict: winning commit deleted files our transaction depends on: {:?}",
                files_deleted_by_winner,
            )));
        }

        let our_removed_set: HashSet<&str> = self
            .transaction
            .removed_files
            .iter()
            .map(|f| f.file_path.as_str())
            .collect();

        let double_deletes: Vec<_> = our_removed_set.intersection(&winning_removed_set).collect();

        if !double_deletes.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Iceberg commit conflict: both transactions deleted the same files: {:?}",
                double_deletes,
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::spec::{DataContentType, DataFileFormat};

    fn test_partition_key(spec_id: i32, key: &str, value: &str) -> PartitionKey {
        PartitionKey {
            spec_id,
            values: vec![(key.to_string(), value.to_string())],
        }
    }

    fn test_data_file(path: &str) -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: vec![],
            record_count: 100,
            file_size_in_bytes: 1000,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: 0,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn minimal_table_metadata(table_uuid: Option<uuid::Uuid>) -> TableMetadata {
        let uuid_str = table_uuid
            .map(|u| format!("\"{}\"", u))
            .unwrap_or_else(|| "null".to_string());
        let json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": {uuid_str},
                "location": "s3://dummy/test-table",
                "last-updated-ms": 0,
                "last-column-id": 1,
                "schemas": [
                    {{
                        "type": "struct",
                        "schema-id": 0,
                        "fields": [
                            {{"id": 1, "name": "col", "type": "string", "required": false}}
                        ]
                    }}
                ],
                "current-schema-id": 0
            }}"#
        );
        TableMetadata::from_json(json.as_bytes())
            .expect("minimal table metadata JSON should be valid")
    }

    fn make_txn(
        operation: Operation,
        added_files: Vec<DataFile>,
        removed_files: Vec<DataFile>,
        partition_values: HashSet<PartitionKey>,
        schema_changes: Option<TransactionSchemaChange>,
        table_uuid: Option<uuid::Uuid>,
    ) -> TransactionInfo {
        TransactionInfo {
            read_metadata: minimal_table_metadata(table_uuid),
            operation,
            added_files,
            removed_files,
            partition_values,
            read_whole_table: false,
            schema_changes,
            requirements: vec![],
            updates: vec![],
        }
    }

    fn make_winning(
        operation: Operation,
        added_files: Vec<DataFile>,
        removed_files: Vec<DataFile>,
        partition_values: HashSet<PartitionKey>,
        schema_changes: Option<TransactionSchemaChange>,
        table_uuid: Option<uuid::Uuid>,
    ) -> WinningCommitSummary {
        WinningCommitSummary {
            metadata: minimal_table_metadata(table_uuid),
            operation,
            added_files,
            removed_files,
            partition_values,
            schema_changes,
            snapshot_id: 1,
            parent_snapshot_id: None,
        }
    }

    #[test]
    fn test_table_uuid_changed_detected() {
        let txn = make_txn(
            Operation::Append,
            vec![],
            vec![],
            HashSet::new(),
            None,
            Some(uuid::Uuid::new_v4()),
        );
        let winning = make_winning(
            Operation::Append,
            vec![],
            vec![],
            HashSet::new(),
            None,
            Some(uuid::Uuid::new_v4()),
        );
        let checker = ConflictChecker {
            transaction: txn,
            winning_commit: winning,
        };

        let result = checker.check_table_uuid();
        assert!(result.is_err());
    }

    #[test]
    fn test_table_uuid_same_passes() {
        let uuid_val = uuid::Uuid::new_v4();
        let txn = make_txn(
            Operation::Append,
            vec![],
            vec![],
            HashSet::new(),
            None,
            Some(uuid_val),
        );
        let winning = make_winning(
            Operation::Append,
            vec![],
            vec![],
            HashSet::new(),
            None,
            Some(uuid_val),
        );
        let checker = ConflictChecker {
            transaction: txn,
            winning_commit: winning,
        };

        let result = checker.check_table_uuid();
        assert!(result.is_ok());
    }

    #[test]
    fn test_dual_append_same_partition_passes() {
        let mut txn_partitions = HashSet::new();
        txn_partitions.insert(test_partition_key(0, "col1", "value1"));
        let txn = make_txn(
            Operation::Append,
            vec![],
            vec![],
            txn_partitions,
            None,
            Some(uuid::Uuid::new_v4()),
        );

        let mut winning_partitions = HashSet::new();
        winning_partitions.insert(test_partition_key(0, "col1", "value1"));
        let winning = make_winning(
            Operation::Append,
            vec![],
            vec![],
            winning_partitions,
            None,
            Some(uuid::Uuid::new_v4()),
        );

        let checker = ConflictChecker {
            transaction: txn,
            winning_commit: winning,
        };

        let result = checker.check_partition_conflicts();
        assert!(result.is_ok());
    }

    #[test]
    fn test_append_and_overwrite_same_partition_conflicts() {
        let mut txn_partitions = HashSet::new();
        txn_partitions.insert(test_partition_key(0, "col1", "value1"));
        let txn = make_txn(
            Operation::Append,
            vec![],
            vec![],
            txn_partitions,
            None,
            Some(uuid::Uuid::new_v4()),
        );

        let mut winning_partitions = HashSet::new();
        winning_partitions.insert(test_partition_key(0, "col1", "value1"));
        let winning = make_winning(
            Operation::Overwrite,
            vec![],
            vec![],
            winning_partitions,
            None,
            Some(uuid::Uuid::new_v4()),
        );

        let checker = ConflictChecker {
            transaction: txn,
            winning_commit: winning,
        };

        let result = checker.check_partition_conflicts();
        assert!(result.is_err());
    }

    #[test]
    fn test_different_partitions_passes() {
        let mut txn_partitions = HashSet::new();
        txn_partitions.insert(test_partition_key(0, "col1", "valueA"));
        let txn = make_txn(
            Operation::Overwrite,
            vec![],
            vec![],
            txn_partitions,
            None,
            Some(uuid::Uuid::new_v4()),
        );

        let mut winning_partitions = HashSet::new();
        winning_partitions.insert(test_partition_key(0, "col1", "valueB"));
        let winning = make_winning(
            Operation::Overwrite,
            vec![],
            vec![],
            winning_partitions,
            None,
            Some(uuid::Uuid::new_v4()),
        );

        let checker = ConflictChecker {
            transaction: txn,
            winning_commit: winning,
        };

        let result = checker.check_partition_conflicts();
        assert!(result.is_ok());
    }

    #[test]
    fn test_winning_deletes_file_we_added_conflicts() {
        let txn = make_txn(
            Operation::Append,
            vec![test_data_file("/data/file1.parquet")],
            vec![],
            HashSet::new(),
            None,
            Some(uuid::Uuid::new_v4()),
        );

        let winning = make_winning(
            Operation::Delete,
            vec![],
            vec![test_data_file("/data/file1.parquet")],
            HashSet::new(),
            None,
            Some(uuid::Uuid::new_v4()),
        );

        let checker = ConflictChecker {
            transaction: txn,
            winning_commit: winning,
        };

        let result = checker.check_file_conflicts();
        assert!(result.is_err());
    }

    #[test]
    fn test_non_overlapping_files_passes() {
        let txn = make_txn(
            Operation::Append,
            vec![test_data_file("/data/file1.parquet")],
            vec![],
            HashSet::new(),
            None,
            Some(uuid::Uuid::new_v4()),
        );

        let winning = make_winning(
            Operation::Delete,
            vec![],
            vec![test_data_file("/data/file2.parquet")],
            HashSet::new(),
            None,
            Some(uuid::Uuid::new_v4()),
        );

        let checker = ConflictChecker {
            transaction: txn,
            winning_commit: winning,
        };

        let result = checker.check_file_conflicts();
        assert!(result.is_ok());
    }

    #[test]
    fn test_concurrent_column_additions_different_names_passes() {
        let txn = make_txn(
            Operation::Append,
            vec![],
            vec![],
            HashSet::new(),
            Some(TransactionSchemaChange {
                old_schema_id: 0,
                new_schema_id: 1,
                added_columns: vec!["col_a".to_string()],
                removed_columns: vec![],
                type_changes: vec![],
                renames: vec![],
            }),
            Some(uuid::Uuid::new_v4()),
        );

        let winning = make_winning(
            Operation::Append,
            vec![],
            vec![],
            HashSet::new(),
            Some(TransactionSchemaChange {
                old_schema_id: 0,
                new_schema_id: 2,
                added_columns: vec!["col_b".to_string()],
                removed_columns: vec![],
                type_changes: vec![],
                renames: vec![],
            }),
            Some(uuid::Uuid::new_v4()),
        );

        let checker = ConflictChecker {
            transaction: txn,
            winning_commit: winning,
        };

        let result = checker.check_schema_compatibility();
        assert!(result.is_ok());
    }

    #[test]
    fn test_concurrent_type_change_conflicts() {
        let txn = make_txn(
            Operation::Append,
            vec![],
            vec![],
            HashSet::new(),
            Some(TransactionSchemaChange {
                old_schema_id: 0,
                new_schema_id: 1,
                added_columns: vec![],
                removed_columns: vec![],
                type_changes: vec![("col_a".to_string(), "int".to_string(), "bigint".to_string())],
                renames: vec![],
            }),
            Some(uuid::Uuid::new_v4()),
        );

        let winning = make_winning(
            Operation::Append,
            vec![],
            vec![],
            HashSet::new(),
            Some(TransactionSchemaChange {
                old_schema_id: 0,
                new_schema_id: 2,
                added_columns: vec![],
                removed_columns: vec![],
                type_changes: vec![],
                renames: vec![],
            }),
            Some(uuid::Uuid::new_v4()),
        );

        let checker = ConflictChecker {
            transaction: txn,
            winning_commit: winning,
        };

        let result = checker.check_schema_compatibility();
        assert!(result.is_err());
    }
}

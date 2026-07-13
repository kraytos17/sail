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

use async_trait::async_trait;

use super::{
    ActionCommit, SnapshotProduceOperation, SnapshotProducer, Transaction, TransactionAction,
};
use crate::io::StoreContext;
use crate::spec::manifest::ManifestMetadata;
use crate::spec::manifest_list::ManifestFile;
use crate::spec::DataFile;

pub struct OverwriteAction {
    added_data_files: Vec<DataFile>,
    store_ctx: Option<StoreContext>,
    manifest_metadata: Option<ManifestMetadata>,
    row_lineage_start_row_id: Option<i64>,
    /// Pre-filtered parent manifest entries for predicate/partition overwrite.
    /// When set, only these entries are carried forward; others are replaced.
    /// When None, ALL parent entries are replaced (full table overwrite).
    parent_manifest_entries: Option<Vec<ManifestFile>>,
}

impl Default for OverwriteAction {
    fn default() -> Self {
        Self::new()
    }
}

impl OverwriteAction {
    pub fn new() -> Self {
        Self {
            added_data_files: Vec::new(),
            store_ctx: None,
            manifest_metadata: None,
            row_lineage_start_row_id: None,
            parent_manifest_entries: None,
        }
    }

    pub fn add_file(&mut self, file: DataFile) {
        self.added_data_files.push(file);
    }

    pub fn with_store_context(mut self, store_ctx: StoreContext) -> Self {
        self.store_ctx = Some(store_ctx);
        self
    }

    pub fn with_manifest_metadata(mut self, metadata: ManifestMetadata) -> Self {
        self.manifest_metadata = Some(metadata);
        self
    }

    pub fn with_row_lineage_start_row_id(mut self, start_row_id: Option<i64>) -> Self {
        self.row_lineage_start_row_id = start_row_id;
        self
    }

    pub fn with_parent_manifest_entries(mut self, entries: Option<Vec<ManifestFile>>) -> Self {
        self.parent_manifest_entries = entries;
        self
    }
}

#[async_trait]
impl TransactionAction for OverwriteAction {
    async fn commit(self: Arc<Self>, tx: &Transaction) -> Result<ActionCommit, String> {
        let mut producer = SnapshotProducer::new(
            tx,
            self.added_data_files.clone(),
            self.store_ctx.clone(),
            self.manifest_metadata.clone(),
        )
        .with_row_lineage_start_row_id(self.row_lineage_start_row_id);

        if let Some(ref entries) = self.parent_manifest_entries {
            producer = producer.with_parent_manifest_entries(Some(entries.clone()));
        }

        struct OverwriteOperation;
        impl SnapshotProduceOperation for OverwriteOperation {
            fn operation(&self) -> &'static str {
                "overwrite"
            }
        }

        producer.commit(OverwriteOperation).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::types::values::Literal;
    use crate::spec::{DataContentType, DataFile, DataFileFormat};

    fn dummy_data_file(path: &str) -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: vec![],
            record_count: 0,
            file_size_in_bytes: 0,
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
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

    #[test]
    fn test_overwrite_action_new() {
        let action = OverwriteAction::new();
        assert!(action.added_data_files.is_empty());
        assert!(action.store_ctx.is_none());
        assert!(action.manifest_metadata.is_none());
        assert!(action.row_lineage_start_row_id.is_none());
        assert!(action.parent_manifest_entries.is_none());
    }

    #[test]
    fn test_overwrite_action_add_file() {
        let mut action = OverwriteAction::new();
        assert_eq!(action.added_data_files.len(), 0);
        action.add_file(dummy_data_file("file1.parquet"));
        assert_eq!(action.added_data_files.len(), 1);
        action.add_file(dummy_data_file("file2.parquet"));
        assert_eq!(action.added_data_files.len(), 2);
    }

    #[test]
    fn test_overwrite_action_builders() {
        let action = OverwriteAction::new()
            .with_row_lineage_start_row_id(Some(42))
            .with_parent_manifest_entries(Some(vec![]));
        assert_eq!(action.row_lineage_start_row_id, Some(42));
        assert!(action.parent_manifest_entries.is_some());
        assert!(action.parent_manifest_entries.unwrap().is_empty());
    }

    #[test]
    fn test_overwrite_action_default() {
        let action = OverwriteAction::default();
        assert!(action.added_data_files.is_empty());
        assert!(action.store_ctx.is_none());
    }

    #[test]
    fn test_overwrite_action_add_file_chain() {
        let mut action = OverwriteAction::new().with_row_lineage_start_row_id(Some(99));
        action.add_file(dummy_data_file("a.parquet"));
        action.add_file(dummy_data_file("b.parquet"));
        assert_eq!(action.row_lineage_start_row_id, Some(99));
        assert_eq!(action.added_data_files.len(), 2);
        assert_eq!(action.added_data_files[0].file_path, "a.parquet");
        assert_eq!(action.added_data_files[1].file_path, "b.parquet");
    }
}

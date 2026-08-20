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

pub mod commit_exec;

use sail_common_datafusion::catalog::LakehouseExecutionContext;
use serde::{Deserialize, Serialize};

use crate::spec::{DataFile, Operation, PartitionSpec, Schema, TableRequirement, TableUpdate};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IcebergCommitInfo {
    pub table_uri: String,
    pub row_count: u64,
    pub data_files: Vec<DataFile>,
    pub manifest_path: String,
    pub manifest_list_path: String,
    pub updates: Vec<TableUpdate>,
    pub requirements: Vec<TableRequirement>,
    pub table_properties: Vec<(String, String)>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    pub operation: Operation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<PartitionSpec>,
    /// File paths rewritten by row-level operations. Used to determine which parent
    /// manifests to keep vs replace when committing.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub touched_file_paths: Vec<String>,
    /// JSON-encoded `Vec<(String, String)>` partition column equality pairs from
    /// `INSERT ... REPLACE WHERE`. Used to keep only non-matching parent manifests
    /// when committing a predicate overwrite.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overwrite_predicate: Option<String>,
    /// JSON-encoded partition-value tuples rewritten by an `OverwritePartitions`
    /// write. Used to keep only non-matching parent manifests at commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overwrite_partition_values: Option<String>,
}

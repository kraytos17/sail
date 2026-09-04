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

use std::collections::HashMap;

use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::variant::DEFAULT_VARIANT_INFERENCE_NODE_BUDGET;
use serde::{Deserialize, Serialize};

use crate::operations::write::config::VariantShreddingConfig;
use crate::options::r#gen::IcebergWriteOptions;

const PARQUET_SHRED_VARIANTS: &str = "write.parquet.shred-variants";
const PARQUET_VARIANT_INFERENCE_BUFFER_SIZE: &str = "write.parquet.variant-inference-buffer-size";
const SHRED_VARIANTS_OPTION_KEYS: &[&str] = &["shred-variants", "shred_variants", "shredVariants"];
const VARIANT_INFERENCE_BUFFER_SIZE_OPTION_KEYS: &[&str] = &[
    "variant-inference-buffer-size",
    "variant_inference_buffer_size",
    "variantInferenceBufferSize",
];

#[derive(Debug, Clone, Copy, Default)]
pub struct VariantShreddingOptionPresence {
    shred_variants: bool,
    variant_inference_buffer_size: bool,
}

/// Options for the Iceberg writer execution plan.
/// This is a subset of `IcebergWriteOptions` containing only the fields used
/// during physical writing. It derives serde for use in the physical plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergWriterExecOptions {
    pub merge_schema: bool,
    pub overwrite_schema: bool,
    pub write_data_path: Option<String>,
    pub write_folder_storage_path: Option<String>,
    pub table_properties: Vec<(String, String)>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    pub shred_variants: bool,
    pub shred_variants_explicit: bool,
    pub variant_inference_buffer_size: usize,
    pub variant_inference_buffer_size_explicit: bool,
    pub compression_codec: String,
    pub target_file_size: u64,
    pub touched_file_paths: Vec<String>,
    pub overwrite_predicate: Option<String>,
}

impl Default for IcebergWriterExecOptions {
    fn default() -> Self {
        Self {
            merge_schema: false,
            overwrite_schema: false,
            write_data_path: None,
            write_folder_storage_path: None,
            table_properties: vec![],
            lakehouse_table: None,
            shred_variants: false,
            shred_variants_explicit: false,
            variant_inference_buffer_size: 100,
            variant_inference_buffer_size_explicit: false,
            compression_codec: "zstd".to_string(),
            target_file_size: 134_217_728,
            touched_file_paths: vec![],
            overwrite_predicate: None,
        }
    }
}

impl From<IcebergWriteOptions> for IcebergWriterExecOptions {
    fn from(options: IcebergWriteOptions) -> Self {
        Self {
            merge_schema: options.merge_schema,
            overwrite_schema: options.overwrite_schema,
            write_data_path: options.write_data_path,
            write_folder_storage_path: options.write_folder_storage_path,
            table_properties: vec![],
            lakehouse_table: None,
            shred_variants: options.shred_variants,
            shred_variants_explicit: false,
            variant_inference_buffer_size: options.variant_inference_buffer_size,
            variant_inference_buffer_size_explicit: false,
            compression_codec: options
                .compression_codec
                .unwrap_or_else(|| "zstd".to_string()),
            target_file_size: options.target_file_size_bytes.unwrap_or(134_217_728),
            touched_file_paths: vec![],
            overwrite_predicate: None,
        }
    }
}

impl IcebergWriterExecOptions {
    pub fn variant_shredding_option_presence(
        layers: &[OptionLayer],
    ) -> VariantShreddingOptionPresence {
        let mut presence = VariantShreddingOptionPresence::default();
        for layer in layers {
            match layer {
                OptionLayer::OptionList { items } => {
                    for (key, _) in items {
                        if SHRED_VARIANTS_OPTION_KEYS
                            .iter()
                            .any(|candidate| key.eq_ignore_ascii_case(candidate))
                        {
                            presence.shred_variants = true;
                        }
                        if VARIANT_INFERENCE_BUFFER_SIZE_OPTION_KEYS
                            .iter()
                            .any(|candidate| key.eq_ignore_ascii_case(candidate))
                        {
                            presence.variant_inference_buffer_size = true;
                        }
                    }
                }
                OptionLayer::TablePropertyList { items } => {
                    for (key, _) in items {
                        if key == PARQUET_SHRED_VARIANTS {
                            presence.shred_variants = true;
                        }
                        if key == PARQUET_VARIANT_INFERENCE_BUFFER_SIZE {
                            presence.variant_inference_buffer_size = true;
                        }
                    }
                }
                OptionLayer::TableLocation { .. }
                | OptionLayer::AsOfTimestamp { .. }
                | OptionLayer::AsOfIntegerVersion { .. }
                | OptionLayer::AsOfStringVersion { .. } => {}
            }
        }
        presence
    }

    pub fn apply_variant_shredding_option_presence(
        &mut self,
        presence: VariantShreddingOptionPresence,
    ) {
        self.shred_variants_explicit = presence.shred_variants;
        self.variant_inference_buffer_size_explicit = presence.variant_inference_buffer_size;
    }

    pub fn variant_shredding_config(
        &self,
        table_properties: &HashMap<String, String>,
    ) -> Result<VariantShreddingConfig> {
        let enabled = if self.shred_variants_explicit {
            self.shred_variants
        } else {
            table_properties
                .get(PARQUET_SHRED_VARIANTS)
                .map(|value| parse_bool_property(PARQUET_SHRED_VARIANTS, value))
                .transpose()?
                .unwrap_or(self.shred_variants)
        };

        let inference_buffer_size = if self.variant_inference_buffer_size_explicit {
            self.variant_inference_buffer_size
        } else {
            table_properties
                .get(PARQUET_VARIANT_INFERENCE_BUFFER_SIZE)
                .map(|value| parse_usize_property(PARQUET_VARIANT_INFERENCE_BUFFER_SIZE, value))
                .transpose()?
                .unwrap_or(self.variant_inference_buffer_size)
        };

        Ok(VariantShreddingConfig {
            enabled,
            inference_buffer_size,
            inference_node_budget: DEFAULT_VARIANT_INFERENCE_NODE_BUDGET,
        })
    }
}

fn parse_bool_property(key: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(DataFusionError::Plan(format!(
            "invalid Iceberg table property {key} value: {value}"
        ))),
    }
}

fn parse_usize_property(key: &str, value: &str) -> Result<usize> {
    value.parse::<usize>().map_err(|_| {
        DataFusionError::Plan(format!(
            "invalid Iceberg table property {key} value: {value}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::r#gen::IcebergWriteOptions;

    const DEFAULT_TARGET_FILE_SIZE: u64 = 134_217_728;

    fn write_options(
        compression_codec: &str,
        target_file_size_bytes: Option<u64>,
    ) -> IcebergWriteOptions {
        IcebergWriteOptions {
            overwrite_schema: false,
            merge_schema: false,
            shred_variants: false,
            variant_inference_buffer_size: 100,
            compression_codec: compression_codec.to_string(),
            write_data_path: None,
            write_folder_storage_path: None,
            target_file_size_bytes,
        }
    }

    #[test]
    fn from_preserves_compression_codec() {
        let exec_opts = IcebergWriterExecOptions::from(write_options("snappy", None));
        assert_eq!(exec_opts.compression_codec, "snappy");
    }

    #[test]
    fn from_maps_target_file_size() {
        let exec_opts =
            IcebergWriterExecOptions::from(write_options("zstd", Some(64 * 1024 * 1024)));
        assert_eq!(exec_opts.target_file_size, 64 * 1024 * 1024);

        let exec_opts = IcebergWriterExecOptions::from(write_options("zstd", None));
        assert_eq!(exec_opts.target_file_size, DEFAULT_TARGET_FILE_SIZE);
    }

    #[test]
    fn default_target_file_size_is_iceberg_default() {
        assert_eq!(
            IcebergWriterExecOptions::default().target_file_size,
            DEFAULT_TARGET_FILE_SIZE
        );
    }
}

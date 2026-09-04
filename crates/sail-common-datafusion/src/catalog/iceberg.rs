use super::managed;

pub const ICEBERG_TABLE_TYPE_KEY: &str = managed::TABLE_TYPE_KEY;
pub const ICEBERG_CLASSIFICATION_KEY: &str = managed::CLASSIFICATION_KEY;
pub const ICEBERG_TABLE_TYPE_VALUE: &str = "iceberg";
pub const ICEBERG_METADATA_LOCATION_KEY: &str = managed::METADATA_LOCATION_KEY;
pub const ICEBERG_METADATA_LOCATION_UNDERSCORE_KEY: &str =
    managed::METADATA_LOCATION_UNDERSCORE_KEY;
pub const ICEBERG_METADATA_LOCATION_KEYS: &[&str] = managed::METADATA_LOCATION_KEYS;
pub const ICEBERG_PREVIOUS_METADATA_LOCATION_KEY: &str = managed::PREVIOUS_METADATA_LOCATION_KEY;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IcebergMetadataTableType {
    Snapshots,
    Refs,
}

impl IcebergMetadataTableType {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "snapshots" => Some(Self::Snapshots),
            "refs" => Some(Self::Refs),
            _ => None,
        }
    }
}

impl std::fmt::Display for IcebergMetadataTableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Snapshots => write!(f, "snapshots"),
            Self::Refs => write!(f, "refs"),
        }
    }
}

pub fn is_iceberg_table_marker(key: &str, value: &str) -> bool {
    managed::is_table_format_marker(key, value, ICEBERG_TABLE_TYPE_VALUE)
}

pub fn is_iceberg_table_properties<'a, I>(properties: I) -> bool
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    properties.into_iter().any(|(key, value)| {
        managed::is_table_format_marker(key.trim(), value.trim(), ICEBERG_TABLE_TYPE_VALUE)
            || managed::is_metadata_location_key(key.trim())
    })
}

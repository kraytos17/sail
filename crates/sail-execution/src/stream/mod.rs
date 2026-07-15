pub mod error;
pub mod merge;
pub mod reader;
pub mod writer;

pub mod gen_ {
    tonic::include_proto!("sail.stream");
}

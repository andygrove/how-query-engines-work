/// include the generated protobuf source as a submodule
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/ballista.rs"));
}

pub mod client;
pub mod cluster;
pub mod error;
pub mod execution;
pub mod logical_plan;

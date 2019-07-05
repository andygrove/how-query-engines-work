/// include the generated protobuf source as a submodule
pub mod ballista_proto {
    include!(concat!(env!("OUT_DIR"), "/ballista.rs"));
}

pub mod client;
pub mod execution;
pub mod logical_plan;

// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This crate contains code generated from the Ballista Protocol Buffer Definition as well
//! as convenience code for interacting with the generated code.

pub const BALLISTA_PROTO_VERSION: &str = env!("CARGO_PKG_VERSION");

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/ballista.protobuf.rs"));
}

pub mod to_proto;

/// Error
#[derive(Debug)]
pub enum BallistaProtoError {
    General(String),
}

/// Create an empty ExprNode
pub fn empty_expr_node() -> protobuf::LogicalExprNode {
    protobuf::LogicalExprNode {
        alias: None,
        column_name: "".to_owned(),
        has_column_name: false,
        literal_string: "".to_owned(),
        has_literal_string: false,
        literal_int: 0,
        literal_uint: 0,
        literal_f32: 0.0,
        literal_f64: 0.0,
        has_literal_i8: false,
        has_literal_i16: false,
        has_literal_i32: false,
        has_literal_i64: false,
        has_literal_u8: false,
        has_literal_u16: false,
        has_literal_u32: false,
        has_literal_u64: false,
        has_literal_f32: false,
        has_literal_f64: false,
        binary_expr: None,
        aggregate_expr: None,
    }
}

/// Create an empty LogicalPlanNode
pub fn empty_logical_plan_node() -> protobuf::LogicalPlanNode {
    protobuf::LogicalPlanNode {
        csv_scan: None,
        parquet_scan: None,
        input: None,
        projection: None,
        selection: None,
        limit: None,
        aggregate: None,
        join: None,
    }
}

/// Create an empty PhysicalPlanNode
pub fn empty_physical_plan_node() -> protobuf::PhysicalPlanNode {
    protobuf::PhysicalPlanNode {
        scan: None,
        input: None,
        projection: None,
        selection: None,
        global_limit: None,
        local_limit: None,
        shuffle_reader: None,
        hash_aggregate: None,
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn sanity_check() {
        let _ = empty_logical_plan_node();
        let _ = empty_expr_node();
        let _ = empty_physical_plan_node();
    }
}

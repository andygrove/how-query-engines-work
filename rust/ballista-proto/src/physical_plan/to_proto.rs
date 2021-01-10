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

//! Serde code to convert Arrow schemas and DataFusion logical plans to Ballista protocol
//! buffer format, allowing DataFusion logical plans to be serialized and transmitted between
//! processes.

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::{empty_expr_node, empty_physical_plan_node, protobuf, BallistaProtoError};

use datafusion::physical_plan::expressions::{BinaryExpr, Column};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};

impl TryInto<protobuf::PhysicalPlanNode> for Arc<dyn ExecutionPlan> {
    type Error = BallistaProtoError;

    fn try_into(self) -> Result<protobuf::PhysicalPlanNode, Self::Error> {
        let plan = self.as_any();
        if let Some(exec) = plan.downcast_ref::<ProjectionExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            let mut node = empty_physical_plan_node();
            node.input = Some(Box::new(input));
            let expr1 = exec
                .expr()
                .iter()
                .map(|expr| expr.0.clone().try_into().unwrap())
                .collect();
            node.projection = Some(protobuf::ProjectionExecNode {
                expr: expr1, //::<Result<Vec<_>, BallistaProtoError>>
            });
            Ok(node)
        } else {
            Err(BallistaProtoError::General(format!(
                "physical plan to_proto {:?}",
                self
            )))
        }
    }
}

impl TryFrom<Arc<dyn PhysicalExpr>> for protobuf::LogicalExprNode {
    type Error = BallistaProtoError;

    fn try_from(value: Arc<dyn PhysicalExpr>) -> Result<Self, Self::Error> {
        let expr = value.as_any();
        if let Some(expr) = expr.downcast_ref::<Column>() {
            let mut node = empty_expr_node();
            node.column_name = expr.name().to_owned();
            node.has_column_name = true;
            Ok(node)
        } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
            let mut node = empty_expr_node();
            node.binary_expr = Some(Box::new(protobuf::BinaryExprNode {
                l: Some(Box::new(expr.left().to_owned().try_into()?)),
                r: Some(Box::new(expr.right().to_owned().try_into()?)),
                op: format!("{:?}", expr.op()),
            }));
            Ok(node)
        } else {
            Err(BallistaProtoError::General(format!(
                "unsupported physical expression {:?}",
                value
            )))
        }
    }
}

// impl TryInto<protobuf::LogicalExprNode> for Arc<dyn PhysicalExpr> {
//     type Error = BallistaProtoError;
//
//     fn try_into(self) -> Result<protobuf::LogicalExprNode, Self::Error> {
//         let expr = self.as_any();
//         if let Some(expr) = expr.downcast_ref::<Column>() {
//             let mut node = empty_expr_node();
//             node.column_name = expr.name().to_owned();
//             node.has_column_name = true;
//             Ok(node)
//         } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
//             let mut node = empty_expr_node();
//             node.binary_expr = protobuf::BinaryExprNode {
//                 l: Some(Box::new(expr.left().to_owned().into())),
//                 r: Some(Box::new(expr.right().to_owned().into())),
//             };
//             Ok(node)
//         } else {
//             Err(BallistaProtoError::General(format!(
//                 "physical plan to_proto {:?}",
//                 self
//             )))
//         }
//     }
// }

// match self {
//     PhysicalPlan::Projection(exec) => {

//     }
//     PhysicalPlan::Filter(exec) => {
//         let input: protobuf::PhysicalPlanNode = exec.child.as_ref().try_into()?;
//         let mut node = empty_physical_plan_node();
//         node.input = Some(Box::new(input));
//         node.selection = Some(protobuf::SelectionExecNode {
//             expr: Some(exec.as_ref().filter_expr.as_ref().try_into()?),
//         });
//         Ok(node)
//     }
//     PhysicalPlan::HashAggregate(exec) => {
//         let input: protobuf::PhysicalPlanNode = exec.child.as_ref().try_into()?;
//         let mut node = empty_physical_plan_node();
//         node.input = Some(Box::new(input));
//         node.hash_aggregate = Some(protobuf::HashAggregateExecNode {
//             mode: match exec.mode {
//                 AggregateMode::Partial => protobuf::AggregateMode::Partial,
//                 AggregateMode::Final => protobuf::AggregateMode::Final,
//                 AggregateMode::Complete => protobuf::AggregateMode::Complete,
//             }
//                 .into(),
//             group_expr: exec
//                 .group_expr
//                 .iter()
//                 .map(|expr| expr.try_into())
//                 .collect::<Result<Vec<_>, BallistaError>>()?,
//             aggr_expr: exec
//                 .aggr_expr
//                 .iter()
//                 .map(|expr| expr.try_into())
//                 .collect::<Result<Vec<_>, BallistaError>>()?,
//         });
//         Ok(node)
//     }
//     PhysicalPlan::CsvScan(exec) => {
//         let mut node = empty_physical_plan_node();
//         node.scan = Some(protobuf::ScanExecNode {
//             path: exec.path.clone(),
//             filename: exec.filenames.clone(),
//             projection: exec
//                 .projection
//                 .as_ref()
//                 .unwrap()
//                 .iter()
//                 .map(|n| *n as u32)
//                 .collect(),
//             file_format: "csv".to_owned(),
//             schema: Some(exec.original_schema().as_ref().try_into()?),
//             has_header: exec.has_header,
//             batch_size: exec.batch_size as u32,
//         });
//         Ok(node)
//     }
//     PhysicalPlan::ParquetScan(exec) => {
//         let mut node = empty_physical_plan_node();
//         node.scan = Some(protobuf::ScanExecNode {
//             path: exec.path.clone(),
//             filename: exec.filenames.clone(),
//             projection: exec
//                 .projection
//                 .as_ref()
//                 .unwrap()
//                 .iter()
//                 .map(|n| *n as u32)
//                 .collect(),
//             file_format: "parquet".to_owned(),
//             schema: Some(exec.parquet_schema.as_ref().try_into()?),
//             has_header: false,
//             batch_size: exec.batch_size as u32,
//         });
//         Ok(node)
//     }
//     PhysicalPlan::ShuffleReader(exec) => {
//         let mut node = empty_physical_plan_node();
//
//         let shuffle_id: Vec<protobuf::ShuffleId> = exec
//             .shuffle_id
//             .iter()
//             .map(|s| s.try_into())
//             .collect::<Result<_, _>>()?;
//
//         node.shuffle_reader = Some(protobuf::ShuffleReaderExecNode {
//             schema: Some(exec.schema().as_ref().try_into()?),
//             shuffle_id,
//         });
//         Ok(node)
//     }

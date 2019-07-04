use crate::ballista_proto::{ColumnIndex, ExprNode, File, LogicalPlanNode, Projection};

/// include the generated protobuf source as a submodule
pub mod ballista_proto {
    include!(concat!(env!("OUT_DIR"), "/ballista.rs"));
}

pub struct Expr {}

pub struct LogicalPlan {
    plan: Box<LogicalPlanNode>,
}

impl LogicalPlan {
    /// Get a reference to the internal protobuf representation of the plan
    pub fn to_proto(&self) -> LogicalPlanNode {
        self.plan.as_ref().clone()
    }
}

impl LogicalPlan {
    /// Create a projection onto this relation
    pub fn projection(&self, column_index: Vec<usize>) -> LogicalPlan {
        // convert indices into expressions
        let expr: Vec<ExprNode> = column_index
            .iter()
            .map(|i| ExprNode {
                column_index: Some(ColumnIndex { index: *i as u32 }),
                binary_expr: None,
            })
            .collect();

        let mut plan = empty_plan_node();
        plan.projection = Some(Projection { expr });
        plan.input = Some(Box::new(self.plan.as_ref().clone()));
        LogicalPlan { plan }
    }
}

fn empty_plan_node() -> Box<LogicalPlanNode> {
    Box::new(LogicalPlanNode {
        file: None,
        input: None,
        projection: None,
        selection: None,
        limit: None,
    })
}

pub fn read_file(filename: &str) -> LogicalPlan {
    LogicalPlan {
        plan: Box::new(LogicalPlanNode {
            file: Some(File {
                filename: filename.to_string(),
            }),
            input: None,
            projection: None,
            selection: None,
            limit: None,
        }),
    }
}

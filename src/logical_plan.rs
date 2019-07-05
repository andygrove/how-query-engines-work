use crate::proto;

use arrow::datatypes::{DataType, Field, Schema};

pub struct Expr {}

pub struct LogicalPlan {
    plan: Box<proto::LogicalPlanNode>,
}

impl LogicalPlan {
    /// Get a reference to the internal protobuf representation of the plan
    pub fn to_proto(&self) -> proto::LogicalPlanNode {
        self.plan.as_ref().clone()
    }
}

impl LogicalPlan {
    /// Create a projection onto this relation
    pub fn projection(&self, column_index: Vec<usize>) -> LogicalPlan {
        // convert indices into expressions
        let expr: Vec<proto::ExprNode> = column_index
            .iter()
            .map(|i| proto::ExprNode {
                column_index: Some(proto::ColumnIndex { index: *i as u32 }),
                binary_expr: None,
            })
            .collect();

        let mut plan = empty_plan_node();
        plan.projection = Some(proto::Projection { expr });
        plan.input = Some(Box::new(self.plan.as_ref().clone()));
        LogicalPlan { plan }
    }
}

fn empty_plan_node() -> Box<proto::LogicalPlanNode> {
    Box::new(proto::LogicalPlanNode {
        file: None,
        input: None,
        projection: None,
        selection: None,
        limit: None,
    })
}

/// Create a logical plan representing a file
pub fn read_file(filename: &str, schema: &Schema) -> LogicalPlan {
    let schema_proto = proto::Schema {
        columns: schema
            .fields()
            .iter()
            .map(|field| proto::Field {
                name: field.name().to_string(),
                arrow_type: 0, // TODO
                nullable: true,
                children: vec![],
            })
            .collect(),
    };

    let mut plan = empty_plan_node();
    plan.file = Some(proto::File {
        filename: filename.to_string(),
        schema: Some(schema_proto),
    });
    LogicalPlan { plan }
}

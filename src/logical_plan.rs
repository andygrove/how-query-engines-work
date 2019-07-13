use crate::error::{BallistaError, Result};
use crate::proto;

use arrow::datatypes::{DataType, Schema};
use datafusion::logicalplan::{Expr as DFExpr, LogicalPlan as DFPlan};

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
    pub fn projection(&self, column_index: Vec<usize>) -> Result<LogicalPlan> {
        // convert indices into expressions
        let expr: Vec<proto::ExprNode> = column_index
            .iter()
            .map(|i| proto::ExprNode {
                column_index: Some(proto::ColumnIndex { index: *i as u32 }),
                binary_expr: None,
                aggregate_expr: None,
            })
            .collect();

        let mut plan = empty_plan_node();
        plan.projection = Some(proto::Projection { expr });
        plan.input = Some(Box::new(self.plan.as_ref().clone()));
        Ok(LogicalPlan { plan })
    }

    pub fn selection(&self, _expr: &proto::ExprNode) -> Result<LogicalPlan> {
        Err(BallistaError::NotImplemented)
    }

    pub fn aggregate(
        &self,
        group_expr: Vec<proto::ExprNode>,
        aggr_expr: Vec<proto::ExprNode>,
    ) -> Result<LogicalPlan> {
        let mut plan = empty_plan_node();
        plan.aggregate = Some(proto::Aggregate {
            group_expr,
            aggr_expr,
        });
        plan.input = Some(Box::new(self.plan.as_ref().clone()));
        Ok(LogicalPlan { plan })
    }

    pub fn limit(&self, _limit: usize) -> Result<LogicalPlan> {
        Err(BallistaError::NotImplemented)
    }
}

pub fn column(i: usize) -> proto::ExprNode {
    let mut zexpr = empty_expr_node();
    zexpr.column_index = Some(proto::ColumnIndex { index: i as u32 });
    zexpr
}

pub fn min(expr: &proto::ExprNode) -> proto::ExprNode {
    let mut zexpr = empty_expr_node();
    zexpr.aggregate_expr = Some(Box::new(proto::AggregateExpr {
        aggr_function: 0, //TODO should reference proto::AggregateFunction::Min
        expr: Some(Box::new(expr.clone())),
    }));
    zexpr
}

pub fn max(expr: &proto::ExprNode) -> proto::ExprNode {
    let mut zexpr = empty_expr_node();
    zexpr.aggregate_expr = Some(Box::new(proto::AggregateExpr {
        aggr_function: 1, //TODO should reference proto::AggregateFunction::Max
        expr: Some(Box::new(expr.clone())),
    }));
    zexpr
}
fn empty_expr_node() -> proto::ExprNode {
    proto::ExprNode {
        column_index: None,
        binary_expr: None,
        aggregate_expr: None,
    }
}
fn empty_plan_node() -> Box<proto::LogicalPlanNode> {
    Box::new(proto::LogicalPlanNode {
        file: None,
        input: None,
        projection: None,
        selection: None,
        limit: None,
        aggregate: None,
    })
}

fn to_proto_type(arrow_type: &DataType) -> i32 {
    match arrow_type {
        DataType::Utf8 => 3,
        _ => unimplemented!(),
    }
}

/// Create a logical plan representing a file
pub fn read_file(filename: &str, schema: &Schema) -> LogicalPlan {
    let schema_proto = proto::Schema {
        columns: schema
            .fields()
            .iter()
            .map(|field| proto::Field {
                name: field.name().to_string(),
                arrow_type: to_proto_type(field.data_type()),
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
fn from_arrow_type(arrow_type: &DataType) -> Result<i32> {
    match arrow_type {
        DataType::Boolean => Ok(1),
        DataType::UInt8 => Ok(2),
        DataType::Int8 => Ok(3),
        DataType::UInt16 => Ok(4),
        DataType::Int16 => Ok(5),
        DataType::UInt32 => Ok(6),
        DataType::Int32 => Ok(7),
        DataType::UInt64 => Ok(8),
        DataType::Int64 => Ok(9),
        DataType::Float32 => Ok(10),
        DataType::Float64 => Ok(11),
        DataType::Utf8 => Ok(12),
        _ => Err(BallistaError::General(format!(
            "No conversion for data type {:?}",
            arrow_type
        ))),
        //TODO add others
        // 0 => NONE
        //        HALF_FLOAT = 10;
        //        BINARY = 14;
        //        FIXED_SIZE_BINARY = 15;
        //        DATE32 = 16;
        //        DATE64 = 17;
        //        TIMESTAMP = 18;
        //        TIME32 = 19;
        //        TIME64 = 20;
        //        INTERVAL = 21;
        //        DECIMAL = 22;
        //        LIST = 23;
        //        STRUCT = 24;
        //        UNION = 25;
        //        DICTIONARY = 26;
        //        MAP = 27;
    }
}


pub fn create_ballista_schema(schema: &Schema) -> Result<proto::Schema> {
    let fields = schema.fields().iter().map(|field| {
        proto::Field {
            name: field.name().clone(),
            arrow_type: from_arrow_type(field.data_type()).unwrap(),
            nullable: true,
            children: vec![]
        }
    }).collect();
    Ok(proto::Schema { columns: fields })
}

/// Convert a DataFusion plan into a Ballista protobuf plan
pub fn convert_to_ballista_plan(plan: &DFPlan) -> Result<LogicalPlan> {
    match plan {
        DFPlan::TableScan {
            table_name,
            schema,
            projection,
            ..
        } => {
            let file = read_file(table_name, schema.as_ref());
            Ok(file.projection(projection.as_ref().unwrap().to_vec())?)
        }
        DFPlan::Aggregate {
            input,
            group_expr,
            aggr_expr,
            ..
        } => {
            let input = convert_to_ballista_plan(input)?;
            let group_expr: Vec<proto::ExprNode> = group_expr
                .iter()
                .map(|expr| map_expr(expr))
                .collect::<Result<Vec<proto::ExprNode>>>()?;
            let aggr_expr: Vec<proto::ExprNode> = aggr_expr
                .iter()
                .map(|expr| map_expr(expr))
                .collect::<Result<Vec<proto::ExprNode>>>()?;
            input.aggregate(group_expr, aggr_expr)
        }
        _ => Err(BallistaError::NotImplemented),
    }
}

/// map DataFusion expression to Ballista expression
fn map_expr(expr: &DFExpr) -> Result<proto::ExprNode> {
    match expr {
        DFExpr::Column(i) => Ok(column(*i)),
        _ => Err(BallistaError::NotImplemented),
    }
}


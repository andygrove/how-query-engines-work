use std::sync::Arc;

use crate::error::{BallistaError, Result};
use crate::proto;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::logicalplan::{Expr, LogicalPlan as DFPlan};

fn to_arrow_type(proto_type: i32) -> DataType {
    match proto_type {
        3 => DataType::Utf8,
        6 => DataType::UInt32,
        7 => DataType::Int32,
        8 => DataType::UInt64,
        9 => DataType::Int64,
        11 => DataType::Float32,
        12 => DataType::Float64,
        _ => unimplemented!(), //        NONE = 0;     // arrow::Type::NA
                               //        BOOL = 1;     // arrow::Type::BOOL
                               //        UINT8 = 2;    // arrow::Type::UINT8
                               //        INT8 = 3;     // arrow::Type::INT8
                               //        UINT16 = 4;   // represents arrow::Type fields in src/arrow/type.h
                               //        INT16 = 5;
                               //        UINT32 = 6;
                               //        INT32 = 7;
                               //        UINT64 = 8;
                               //        INT64 = 9;
                               //        HALF_FLOAT = 10;
                               //        FLOAT = 11;
                               //        DOUBLE = 12;
                               //        UTF8 = 13;
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

pub fn create_datafusion_plan(plan: &proto::LogicalPlanNode) -> Result<DFPlan> {
    if plan.file.is_some() {
        let file = plan.file.as_ref().unwrap();

        let columns = file
            .schema
            .as_ref()
            .unwrap()
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    &column.name.to_string(),
                    to_arrow_type(column.arrow_type),
                    true,
                )
            })
            .collect();

        Ok(DFPlan::TableScan {
            schema_name: "default".to_string(),
            table_name: file.filename.clone(),
            schema: Arc::new(Schema::new(columns)),
            projection: None,
        })
    } else if plan.projection.is_some() {
        if let Some(input) = &plan.input {
            let df_input = Arc::new(create_datafusion_plan(&input)?);
            let input_schema = df_input.schema();

            let projection_plan = plan.projection.as_ref().unwrap();

            let expr: Vec<Expr> = projection_plan
                .expr
                .iter()
                .map(|expr| map_expr(expr))
                .collect::<Result<Vec<Expr>>>()?;

            let schema = determine_schema(input_schema, &expr)?;

            Ok(DFPlan::Projection {
                expr,
                input: df_input.clone(),
                schema,
            })
        } else {
            Err(BallistaError::NotImplemented)
        }
    } else if plan.aggregate.is_some() {
        if let Some(input) = &plan.input {
            let df_input = Arc::new(create_datafusion_plan(&input)?);
            let input_schema = df_input.schema();

            let aggregate_plan = plan.aggregate.as_ref().unwrap();

            let group_expr: Vec<Expr> = aggregate_plan
                .group_expr
                .iter()
                .map(|expr| map_expr(expr))
                .collect::<Result<Vec<Expr>>>()?;

            let aggr_expr: Vec<Expr> = aggregate_plan
                .aggr_expr
                .iter()
                .map(|expr| map_expr(expr))
                .collect::<Result<Vec<Expr>>>()?;

            let mut combined = vec![];
            combined.extend_from_slice(&group_expr);
            combined.extend_from_slice(&aggr_expr);

            let schema = determine_schema(input_schema, &combined)?;

            Ok(DFPlan::Aggregate {
                group_expr,
                aggr_expr,
                input: df_input.clone(),
                schema,
            })
        } else {
            Err(BallistaError::NotImplemented)
        }
    } else {
        Err(BallistaError::NotImplemented)
    }
}

fn map_expr(expr: &proto::ExprNode) -> Result<Expr> {
    if expr.column_index.is_some() {
        Ok(Expr::Column(
            expr.column_index.as_ref().unwrap().index as usize,
        ))
    } else {
        Err(BallistaError::NotImplemented)
    }
}

//TODO should create DF logical plan instead and ask each relation for its schema - this is
// code duplication

fn determine_schema(schema: &Schema, projection: &Vec<Expr>) -> Result<Arc<Schema>> {
    let mut fields: Vec<Field> = Vec::with_capacity(projection.len());

    for expr in projection {
        match expr {
            Expr::Column(i) => {
                if *i < schema.fields().len() {
                    fields.push(schema.field(*i).clone());
                } else {
                    return Err(BallistaError::General(format!(
                        "Invalid column index {} in projection",
                        i
                    )));
                }
            }
            _ => unimplemented!(),
        }
    }

    Ok(Arc::new(Schema::new(fields)))
}

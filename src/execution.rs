use std::sync::Arc;

use crate::error::{BallistaError, Result};
use crate::proto;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::logicalplan::{Expr, LogicalPlan as DFPlan};

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
                Field::new(&column.name.to_string(), DataType::UInt32, true) //TODO translate datatype
            })
            .collect();

        Ok(DFPlan::TableScan {
            schema_name: "default".to_string(),
            table_name: file.filename.clone(),
            schema: Arc::new(Schema::new(columns)),
            projection: None,
        })
    } else if plan.projection.is_some() {
        let indices: Result<Vec<Expr>> = plan
            .projection
            .as_ref()
            .unwrap()
            .expr
            .iter()
            .map(|expr| {
                if expr.column_index.is_some() {
                    Ok(Expr::Column(
                        expr.column_index.as_ref().unwrap().index as usize,
                    ))
                } else {
                    Err(BallistaError::NotImplemented)
                }
            })
            .collect();

        if let Some(input) = &plan.input {
            Ok(DFPlan::Projection {
                expr: indices?,
                input: Arc::new(create_datafusion_plan(&input)?),
                schema: Arc::new(Schema::new(vec![])),
            })
        } else {
            Err(BallistaError::NotImplemented)
        }
    } else {
        Err(BallistaError::NotImplemented)
    }
}

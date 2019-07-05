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
        let indices: Vec<usize> = plan
            .projection
            .as_ref()
            .unwrap()
            .expr
            .iter()
            .map(|expr| {
                if expr.column_index.is_some() {
                    Ok(expr.column_index.as_ref().unwrap().index as usize)
                } else {
                    Err(BallistaError::NotImplemented)
                }
            })
            .collect::<Result<Vec<usize>>>()?;

        if let Some(input) = &plan.input {
            let df_input = Arc::new(create_datafusion_plan(&input)?);
            let input_schema = df_input.schema();

            Ok(DFPlan::Projection {
                expr: indices.iter().map(|i| Expr::Column(*i)).collect(),
                input: df_input.clone(),
                schema: projection(input_schema, &indices)?,
            })
        } else {
            Err(BallistaError::NotImplemented)
        }
    } else {
        Err(BallistaError::NotImplemented)
    }
}

//TODO private function copied from DataFusion table_impl

/// Create a new schema by applying a projection to this schema's fields
fn projection(schema: &Schema, projection: &Vec<usize>) -> Result<Arc<Schema>> {
    let mut fields: Vec<Field> = Vec::with_capacity(projection.len());
    for i in projection {
        if *i < schema.fields().len() {
            fields.push(schema.field(*i).clone());
        } else {
            return Err(BallistaError::General(format!(
                "Invalid column index {} in projection",
                i
            )));
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

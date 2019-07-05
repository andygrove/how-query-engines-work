use std::sync::Arc;

use crate::ballista_proto;

use arrow::datatypes::Schema;
use datafusion::logicalplan::LogicalPlan as DFPlan;

pub fn create_datafusion_plan(plan: &ballista_proto::LogicalPlanNode) -> DFPlan {
    if plan.file.is_some() {
        DFPlan::TableScan {
            schema_name: "".to_string(),
            table_name: "".to_string(),
            schema: Arc::new(Schema::new(vec![])),
            projection: None,
        }
    } else if plan.projection.is_some() {
        if let Some(input) = &plan.input {
            DFPlan::Projection {
                expr: vec![],
                input: Arc::new(create_datafusion_plan(&input)),
                schema: Arc::new(Schema::new(vec![])),
            }
        } else {
            unimplemented!()
        }
    } else {
        unimplemented!()
    }
}

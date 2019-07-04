use crate::ballista_proto::LogicalPlanNode;
use crate::ballista_proto::File;

pub mod ballista_proto {
    include!(concat!(env!("OUT_DIR"), "/ballista.rs"));
}


pub struct Expr {

}

pub struct LogicalPlan {
    plan: Box<LogicalPlanNode>
}

impl LogicalPlan {

    /// Get a reference to the internal protobuf representation of the plan
    pub fn to_proto(&self) -> LogicalPlanNode {
        self.plan.as_ref().clone()
    }

}

impl LogicalPlan {



}


pub fn read_file(filename: &str) -> LogicalPlan {
    LogicalPlan {
        plan: Box::new(LogicalPlanNode {
            file: Some(File { filename: filename.to_string() }),
            input: None,
            projection: None,
            selection: None,
            limit: None
        })
    }
}
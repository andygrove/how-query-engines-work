use crate::hello_world::LogicalPlanNode;
use crate::hello_world::File;

pub mod hello_world {
    include!(concat!(env!("OUT_DIR"), "/ballista.rs"));
}


pub struct Expr {

}

pub struct LogicalPlan {
    plan: Box<LogicalPlanNode>

}

impl LogicalPlan {



}


fn read_file(filename: &str) -> LogicalPlan {
    LogicalPlan {
        plan: Box::new(LogicalPlanNode {
            file: File { filename: filename.to_string() },
            input: None,
            projection: None,
            selection: None,
            limit: None
        })
    }
}
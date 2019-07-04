use crate::hello_world::LogicalPlanNode;
use crate::hello_world::File;

pub mod hello_world {
    include!(concat!(env!("OUT_DIR"), "/ballista.rs"));
}


struct Expr {

}

struct LogicalPlan {
    plan: Box<LogicalPlanNode>

}

impl LogicalPlan {

}
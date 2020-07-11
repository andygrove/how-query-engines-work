extern crate ballista;

use std::sync::Arc;
use std::thread;

use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::dataframe::max;
use ballista::datafusion::logicalplan::col_index;
use ballista::distributed::executor::ExecutorContext;
use ballista::distributed::localmode::LocalMode;
use ballista::execution::operators::HashAggregateExec;
use ballista::execution::operators::InMemoryTableScanExec;
use ballista::execution::physical_plan::{AggregateMode, ColumnarBatchStream, PhysicalPlan};
use ballista::utils::datagen::DataGen;
use std::time::Duration;

#[test]
fn local_mode() -> std::io::Result<()> {
    // smol::run(async {
    //     let cluster = LocalMode::new(2);
    //     thread::sleep(Duration::from_secs(5));
    //     Ok(())
    // })
}

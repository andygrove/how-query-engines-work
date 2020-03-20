use std::thread;

use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;

use ballista::client;
use ballista::cluster;
use ballista::error::BallistaError;

use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let schema = Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::UInt32, true),
        Field::new("trip_distance", DataType::Utf8, true),
        Field::new("RatecodeID", DataType::Utf8, true),
        Field::new("store_and_fwd_flag", DataType::Utf8, true),
        Field::new("PULocationID", DataType::Utf8, true),
        Field::new("DOLocationID", DataType::Utf8, true),
        Field::new("payment_type", DataType::Utf8, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("extra", DataType::Float64, true),
        Field::new("mta_tax", DataType::Float64, true),
        Field::new("tip_amount", DataType::Float64, true),
        Field::new("tolls_amount", DataType::Float64, true),
        Field::new("improvement_surcharge", DataType::Float64, true),
        Field::new("total_amount", DataType::Float64, true),
    ]);

    let executors = cluster::get_executors("nyctaxi", "default").unwrap();
    let mut executor_index = 0;

    let threads: Vec<thread::JoinHandle<Result<Vec<RecordBatch>, BallistaError>>> = vec![];

    let num_months: usize = 12;
    for month in 0..num_months {
        let filename = format!(
            "/mnt/data/nyc_taxis/csv/yellow_tripdata_2019-{:02}.csv",
            month + 1
        );

        // SELECT passenger_count, MAX(fare_amount) FROM <filename> GROUP BY passenger_count
        let plan = LogicalPlanBuilder::scan("default", &filename, &schema, None)
            .and_then(|plan| plan.aggregate(vec![col(3)], vec![Expr::AggregateFunction {
                name: "MAX".to_owned(), args: vec![col(10)], return_type: DataType::Float64 }]))
            .and_then(|plan| plan.build())
            //.map_err(|e| Err(format!("{:?}", e)))
            .unwrap(); //TODO

        // send the plan to a ballista server
        let executor = &executors[executor_index];
        executor_index += 1;
        if executor_index == executors.len() {
            executor_index = 0;
        }

        let host = executor.host.clone();
        let port = executor.port;

        tokio::spawn(async move {
            println!("Executing query against executor at {}:{}", host, port);
            client::execute_query(&host, port, plan).await
                .map_err(|e| BallistaError::General(format!("{:?}", e)))
        });
    }

    let mut batches: Vec<RecordBatch> = vec![];
    for handle in threads {
        let response: Vec<RecordBatch> = handle.join().expect("thread panicked").unwrap();
        for batch in response {
            batches.push(batch);
        }
    }

    println!("Received {} batches", batches.len());

    // perform secondary aggregate query on the results collected from the executors
    let schema = Schema::new(vec![
        Field::new("passenger_count", DataType::UInt32, true),
        Field::new("fare_amount", DataType::Float64, true),
    ]);

    let plan = LogicalPlanBuilder::scan("default", "tbd", &schema, None)
        .and_then(|plan| plan.aggregate(vec![col(0)], vec![Expr::AggregateFunction {
            name: "MAX".to_owned(), args: vec![col(1)], return_type: DataType::Float64 }]))
        .and_then(|plan| plan.build())
        //.map_err(|e| Err(format!("{:?}", e)))
        .unwrap(); //TODO

    // create local execution context
    let mut ctx = ExecutionContext::new();

    //TODO register in-memory table containing the batches collected from the executors

    let results = ctx
        .collect_plan(&plan, 1024*1024)
        .unwrap(); // TODO
    //    .map_err(|e| to_tonic_err(&e))?;


    Ok(())
}

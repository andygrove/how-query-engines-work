use std::process;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

extern crate ballista;

use ballista::cluster;
use ballista::error::BallistaError;
use ballista::plan::{Action, TableMeta};
use ballista::utils;
use ballista::{client, BALLISTA_VERSION};

use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::*;

use tokio::task;

#[tokio::main]
async fn main() -> Result<(), BallistaError> {
    println!(
        "Ballista v{} Parallel Aggregate Query Example",
        BALLISTA_VERSION
    );

    //TODO use command-line args
    let nyc_taxi_path = "/mnt/nyctaxi";
    let cluster_name = "ballista";
    let namespace = "default";

    // get a list of ballista executors from kubernetes
    let mut executor_index = 0;
    let executors = cluster::get_executors(cluster_name, namespace)?;
    if executors.is_empty() {
        println!("No executors found");
        process::exit(1);
    }
    println!("Found {} executors", executors.len());

    // execute aggregate query in parallel across all files
    let num_months: usize = 12;
    let mut batches: Vec<RecordBatch> = vec![];
    let mut tasks: Vec<task::JoinHandle<Result<Vec<RecordBatch>, BallistaError>>> = vec![];
    for month in 0..num_months {
        // round robin across the executors
        let executor = &executors[executor_index];
        executor_index += 1;
        if executor_index == executors.len() {
            executor_index = 0;
        }

        let host = executor.host.clone();
        let port = executor.port;

        // execute the query against the executor
        tasks.push(tokio::spawn(async move {
            println!("Executing query against executor at {}:{}", host, port);

            let filename = format!(
                "{}/csv/yellow/2019/yellow_tripdata_2019-{:02}.csv",
                nyc_taxi_path,
                month + 1
            );
            let schema = nyctaxi_schema();

            // SELECT passenger_count, MAX(fare_amount) FROM <filename> GROUP BY passenger_count
            let plan = LogicalPlanBuilder::scan("default", "tripdata", &schema, None)?
                .aggregate(vec![col(3)], vec![max(col(10))])?
                .build()?;

            let action = Action::RemoteQuery {
                plan: plan.clone(),
                tables: vec![TableMeta::Csv {
                    table_name: "tripdata".to_owned(),
                    has_header: true,
                    path: filename,
                    schema: schema.clone(),
                }],
            };

            execute_remote_query(host, port, action).await
        }));
    }

    // collect the results
    for handle in tasks {
        let response: Vec<RecordBatch> = handle.await.expect("thread panicked")?;
        for batch in response {
            batches.push(batch);
        }
    }

    if batches.is_empty() {
        println!("No data returned from executors!");
        process::exit(1);
    }
    println!("Received {} batches from executors", batches.len());

    // perform secondary aggregate query on the results collected from the executors
    let mut ctx = ExecutionContext::new();
    let schema = (&batches[0]).schema();
    let provider = MemTable::new(Arc::new(schema.as_ref().clone()), batches.clone())?;
    ctx.register_table("tripdata", Box::new(provider));
    let plan = LogicalPlanBuilder::scan("default", "tripdata", &schema, None)?
        .aggregate(vec![col(0)], vec![max(col(1))])?
        .build()?;
    let results = ctx.collect_plan(&plan, 1024 * 1024)?;

    // print the results
    utils::result_str(&results)
        .iter()
        .for_each(|str| println!("{}", str));

    Ok(())
}

async fn execute_remote_query(
    host: String,
    port: usize,
    action: Action,
) -> Result<Vec<RecordBatch>, BallistaError> {
    println!("Sending plan to {}:{}", host, port);

    let response = client::execute_action(&host, port, action)
        .await
        .map_err(|e| BallistaError::General(format!("{:?}", e)))?;

    println!("Received {} batches from {}:{}", response.len(), host, port);

    Ok(response)
}

fn nyctaxi_schema() -> Schema {
    Schema::new(vec![
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
    ])
}

fn max(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        name: "MAX".to_owned(),
        args: vec![expr],
        return_type: DataType::Float64,
    }
}

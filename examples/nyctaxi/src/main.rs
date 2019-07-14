use std::thread;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::ExecutionContext;
use ballista::client::Client;
use ballista::cluster;
use ballista::proto;
use ballista::logical_plan;

#[macro_use]
extern crate log;

pub fn main() {

    let _ = ::env_logger::init();

    // discover available executors
    let executors = cluster::get_executors("NYCTAXI").unwrap();

    // schema for nyxtaxi csv files
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

    let mut threads: Vec<thread::JoinHandle<_>> = vec![];

    // manually create one plan for each partition (month)
    let mut executor_index = 0;
    let num_months = 12;
    for month in 0..num_months {
        let filename = format!(
            "/mnt/ssd/nyc_taxis/csv/yellow_tripdata_2018-{:02}.csv",
            month + 1
        );

        // create DataFusion query plan to execute on each partition
        let mut ctx = ExecutionContext::new();
        ctx.register_csv("tripdata", &filename, &schema, true);
        let logical_plan = ctx.create_logical_plan(
            "SELECT trip_distance, MIN(fare_amount), MAX(fare_amount) \
            FROM tripdata GROUP BY trip_distance").unwrap();

        info!("Logical plan: {:?}", logical_plan);

        // convert DataFusion plan to Ballista protobuf
        let table_meta = vec![proto::TableMeta {
            table_name: "tripdata".to_string(),
            filename,
            file_type: "csv".to_string(),
            schema: Some(logical_plan::create_ballista_schema(&schema).unwrap())
        }];
        let plan = logical_plan::convert_to_ballista_plan(&logical_plan).unwrap();

        // send the plan to a ballista server
        let executor = &executors[executor_index];

        let host = executor.host.clone();
        let port = executor.port;

        threads.push(thread::spawn(move || {
            println!("Executing query against executor at {}:{}", host, port);
            let client = Client::new(&host, port);
            client.send(plan, table_meta);
        }));

        executor_index += 1;
        if executor_index == executors.len() {
            executor_index = 0;
        }
    }

    // wait for threads to complete
    for handle in threads {
        handle.join().expect("thread panicked");
    }

    println!("Finished");
}

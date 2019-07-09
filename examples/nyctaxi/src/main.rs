use std::env;
use std::thread;

use arrow::datatypes::{DataType, Field, Schema};
use ballista::client::Client;
use ballista::logical_plan::read_file;

pub fn main() {
    //    for (key, value) in env::vars() {
    //        println!("{}: {}", key, value);
    //    }

    // discover available executors
    let cluster_name = "NYCTAXI"; //TODO should come from env var populated by ballista
    let mut executors: Vec<Executor> = vec![];
    let mut instance = 1;
    loop {
        let host_env = format!("BALLISTA_{}_{}_SERVICE_HOST", cluster_name, instance);
        let port_env = format!("BALLISTA_{}_{}_SERVICE_PORT_GRPC", cluster_name, instance);
        match (env::var(&host_env), env::var(&port_env)) {
            (Ok(host), Ok(port)) => executors.push(Executor {
                host,
                port: port.parse::<usize>().unwrap(),
            }),
            _ => break,
        }
        instance += 1;
    }

    let num_months = 12;

    // build simple logical plan to apply a projection to a CSV file
    let schema = Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::Utf8, true),
        Field::new("trip_distance", DataType::Utf8, true),
        Field::new("RatecodeID", DataType::Utf8, true),
        Field::new("store_and_fwd_flag", DataType::Utf8, true),
        Field::new("PULocationID", DataType::Utf8, true),
        Field::new("DOLocationID", DataType::Utf8, true),
        Field::new("payment_type", DataType::Utf8, true),
        Field::new("fare_amount", DataType::Utf8, true),
        Field::new("extra", DataType::Utf8, true),
        Field::new("mta_tax", DataType::Utf8, true),
        Field::new("tip_amount", DataType::Utf8, true),
        Field::new("tolls_amount", DataType::Utf8, true),
        Field::new("improvement_surcharge", DataType::Utf8, true),
        Field::new("total_amount", DataType::Utf8, true),
    ]);

    let mut threads: Vec<thread::JoinHandle<_>> = vec![];

    // manually create one plan for each partition (month)
    let mut executor_index = 0;
    for month in 0..num_months {
        let filename = format!(
            "/mnt/ssd/nyc_taxis/csv/yellow_tripdata_2018-{:02}.csv",
            month + 1
        );
        let file = read_file(&filename, &schema);
        let plan = file.projection(vec![0, 1, 2]);

        // send the plan to a ballista server
        let executor = &executors[executor_index];

        let host = executor.host.clone();
        let port = executor.port;

        threads.push(thread::spawn(move || {
            println!("Executing query against executor at {}:{}", host, port);
            let client = Client::new(&host, port);
            client.send(plan);
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

struct Executor {
    host: String,
    port: usize,
}

#![deny(warnings, rust_2018_idioms)]

use arrow::datatypes::{DataType, Field, Schema};
use ballista::client::Client;
use ballista::logical_plan::read_file;

pub fn main() {
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
    let file = read_file("/mnt/ssd/nyc_taxis/yellow_tripdata_2017-01.csv", &schema);
    let plan = file.projection(vec![0, 1, 2]);

    // send the plan to a ballista server
    let client = Client::new("[::1]".to_string(), 50051);
    client.send(plan);
}

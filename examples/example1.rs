#![deny(warnings, rust_2018_idioms)]

use arrow::datatypes::{DataType, Field, Schema};
use ballista::client::Client;
use ballista::logical_plan::read_file;

pub fn main() {
    // build simple logical plan to apply a projection to a CSV file
    let schema = Schema::new(vec![
        Field::new("c1", DataType::UInt32, true),
        Field::new("c2", DataType::UInt32, true),
        Field::new("c3", DataType::UInt32, true),
        Field::new("c4", DataType::UInt32, true),
    ]);
    let file = read_file("/path/to/some/file.csv", &schema);
    let plan = file.projection(vec![0, 1, 2]);

    // send the plan to a ballista server
    let client = Client::new("[::1]".to_string(), 50051);
    client.send(plan);
}

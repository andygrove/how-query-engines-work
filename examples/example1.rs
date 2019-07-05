#![deny(warnings, rust_2018_idioms)]

use ballista::client::Client;
use ballista::logical_plan::read_file;

pub fn main() {
    // build simple logical plan to apply a projection to a CSV file
    let file = read_file("/path/to/some/file.csv");
    let plan = file.projection(vec![0, 1, 2]);

    // send the plan to a ballista server
    let client = Client {};
    client.send(plan);
}

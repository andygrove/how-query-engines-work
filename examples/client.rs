#![deny(warnings, rust_2018_idioms)]

use futures::Future;
use hyper::client::connect::{Destination, HttpConnector};
use tower_grpc::Request;
use tower_hyper::{client, util};
use tower_util::MakeService;

use ballista::ballista_proto::client::Executor;
use ballista::ballista_proto::ExecuteRequest;

use ballista::{LogicalPlan, read_file};

pub fn main() {
    let _ = ::env_logger::init();

    let uri: http::Uri = format!("http://[::1]:50051").parse().unwrap();

    let dst = Destination::try_from_uri(uri.clone()).unwrap();
    let connector = util::Connector::new(HttpConnector::new(4));
    let settings = client::Builder::new().http2_only(true).clone();
    let mut make_client = client::Connect::with_builder(connector, settings);

    let execute = make_client
        .make_service(dst)
        .map_err(|e| panic!("connect error: {:?}", e))
        .and_then(move |conn| {

            let conn = tower_request_modifier::Builder::new()
                .set_origin(uri)
                .build(conn)
                .unwrap();

            // Wait until the client is ready...
            Executor::new(conn).ready()
        })
        .and_then(|mut client| {

            let plan: LogicalPlan = read_file("/path/to/some/file.csv");

            client.execute(Request::new(ExecuteRequest {
                plan: Some(plan.to_proto())
            }))
        })
        .and_then(|response| {
            println!("RESPONSE = {:?}", response);
            Ok(())
        })
        .map_err(|e| {
            println!("ERR = {:?}", e);
        });

    tokio::run(execute);
}

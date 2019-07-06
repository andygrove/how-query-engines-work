#![deny(warnings, rust_2018_idioms)]

use futures::Future;
use hyper::client::connect::{Destination, HttpConnector};
use tower_grpc::Request;
use tower_hyper::{client, util};
use tower_util::MakeService;

use crate::logical_plan::LogicalPlan;
use crate::proto;
use crate::proto::client::Executor;

pub struct Client {
    host: String,
    port: usize,
}

impl Client {
    pub fn new(host: String, port: usize) -> Self {
        Self { host, port }
    }

    pub fn send(&self, plan: LogicalPlan) {

        // send the query to the server
        let uri: http::Uri = format!("http://{}:{}", self.host, self.port)
            .parse()
            .unwrap();
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
            .and_then(move |mut client| {
                client.execute(Request::new(proto::ExecuteRequest {
                    plan: Some(plan.to_proto()),
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
}

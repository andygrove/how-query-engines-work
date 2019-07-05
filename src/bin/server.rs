#![deny(warnings, rust_2018_idioms)]

use crate::ballista_proto::{server, ExecuteRequest, ExecuteResponse};

use futures::{future, Future, Stream};
use log::error;
use tokio::net::TcpListener;
use tower_grpc::{Request, Response};
use tower_hyper::server::{Http, Server};

use ballista::ballista_proto;
use ballista::execution::create_datafusion_plan;

#[derive(Clone, Debug)]
struct Greet;

impl server::Executor for Greet {
    type ExecuteFuture = future::FutureResult<Response<ExecuteResponse>, tower_grpc::Status>;

    fn execute(&mut self, request: Request<ExecuteRequest>) -> Self::ExecuteFuture {
        println!("REQUEST = {:?}", request);
        match &request.get_ref().plan {
            Some(plan) => {
                let df_plan = create_datafusion_plan(plan);

                println!("DataFusion plan: {:?}", df_plan);
            }
            _ => {}
        }

        let response = Response::new(ExecuteResponse {
            message: "Zomg, it works!".to_string(),
        });

        future::ok(response)
    }
}

pub fn main() {
    let _ = ::env_logger::init();

    let new_service = server::ExecutorServer::new(Greet);

    let mut server = Server::new(new_service);

    let http = Http::new().http2_only(true).clone();

    let addr = "[::1]:50051".parse().unwrap();
    let bind = TcpListener::bind(&addr).expect("bind");

    let serve = bind
        .incoming()
        .for_each(move |sock| {
            if let Err(e) = sock.set_nodelay(true) {
                return Err(e);
            }

            let serve = server.serve_with(sock, http.clone());
            tokio::spawn(serve.map_err(|e| error!("hyper error: {:?}", e)));

            Ok(())
        })
        .map_err(|e| eprintln!("accept error: {}", e));

    tokio::run(serve)
}

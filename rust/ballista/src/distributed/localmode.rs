use std::sync::Arc;
use std::thread;

use crate::distributed::executor::{BallistaExecutor, Executor};
use crate::distributed::flight_service::FlightServiceImpl;
use crate::error::Result;
use crate::flight::flight_service_server::FlightServiceServer;
use crate::BALLISTA_VERSION;

use std::thread::JoinHandle;
use tonic::transport::Server;

pub struct LocalMode {}

impl LocalMode {
    pub fn new(num_executors: usize) {
        for i in 0..num_executors {
            let port = 50050 + i;
            let _: JoinHandle<Result<()>> = thread::spawn(move || {
                smol::run(async {
                    let addr = format!("0.0.0.0:{}", port);
                    let addr = addr.as_str().parse().unwrap();
                    let executor: Arc<dyn Executor> = Arc::new(BallistaExecutor::default());
                    let service = FlightServiceImpl::new(executor);
                    let server = FlightServiceServer::new(service);
                    println!(
                        "Ballista v{} Rust Executor listening on {:?}",
                        BALLISTA_VERSION, addr
                    );
                    Server::builder()
                        .add_service(server)
                        .serve(addr)
                        .await
                        .unwrap();
                    Ok(())
                })
            });
        }
    }
}

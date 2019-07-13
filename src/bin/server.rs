#[macro_use]
extern crate log;

use crate::proto::{server, ExecuteRequest, ExecuteResponse, TableMeta};

use futures::{future, Future, Stream};
use log::error;
use tokio::net::TcpListener;
use tower_grpc::{Request, Response};
use tower_hyper::server::{Http, Server};

use ballista::error::Result;
use ballista::execution;
use ballista::proto;
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::LogicalPlan;

#[derive(Clone, Debug)]
struct BallistaService;

impl server::Executor for BallistaService {
    type ExecuteFuture = future::FutureResult<Response<ExecuteResponse>, tower_grpc::Status>;

    fn execute(&mut self, request: Request<ExecuteRequest>) -> Self::ExecuteFuture {
        info!("REQUEST = {:?}", request);
        let request = request.get_ref();

        let response = match &request.plan {
            Some(plan) => match execution::create_datafusion_plan(plan) {
                Ok(df_plan) => match execute_query(&request.table_meta, df_plan.as_ref().to_logical_plan().as_ref()) {
                    Ok(count) => Response::new(ExecuteResponse {
                        message: format!("Query retrieved {} rows", count),
                    }),
                    Err(e) => Response::new(ExecuteResponse {
                        message: format!("Error executing plan: {:?}", e),
                    }),
                },
                Err(e) => Response::new(ExecuteResponse {
                    message: format!("Error converting plan: {:?}", e),
                }),
            },
            _ => Response::new(ExecuteResponse {
                message: "empty request".to_string(),
            }),
        };

        future::ok(response)
    }
}

fn execute_query(table_meta: &Vec<TableMeta>, df_plan: &LogicalPlan) -> Result<usize> {
    let mut context = ExecutionContext::new();
    table_meta.iter().for_each(|table| {
        let schema = execution::create_arrow_schema(table.schema.as_ref().unwrap()).unwrap();
        info!("Registering table {} as filename {}", table.table_name, table.filename);
        context.register_csv(&table.table_name, &table.filename, &schema, true); //TODO has_header should not be hard-coded

    });

    let optimized_plan = context.optimize(&df_plan)?;
    info!("Optimized plan: {:?}", optimized_plan);


    let relation = context.execute(&optimized_plan, 1024)?;

    let mut x = relation.borrow_mut();

    let mut count = 0;
    while let Some(batch) = x.next()? {
        count += batch.num_rows();
    }

    Ok(count)
}


pub fn main() {
    let _ = ::env_logger::init();

    let new_service = server::ExecutorServer::new(BallistaService);

    let mut server = Server::new(new_service);

    let http = Http::new().http2_only(true).clone();

    let addr = "0.0.0.0:9090".parse().unwrap();
    info!("Ballista server binding to {}", addr);
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
        .map_err(|e| error!("accept error: {}", e));

    info!("Ballista running");
    tokio::run(serve)
}

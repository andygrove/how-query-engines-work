#![deny(warnings, rust_2018_idioms)]

use crate::proto::{server, ExecuteRequest, ExecuteResponse};

use futures::{future, Future, Stream};
use log::error;
use tokio::net::TcpListener;
use tower_grpc::{Request, Response};
use tower_hyper::server::{Http, Server};

use ballista::error::Result;
use ballista::execution::create_datafusion_plan;
use ballista::proto;
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::LogicalPlan;

#[derive(Clone, Debug)]
struct BallistaService;

impl server::Executor for BallistaService {
    type ExecuteFuture = future::FutureResult<Response<ExecuteResponse>, tower_grpc::Status>;

    fn execute(&mut self, request: Request<ExecuteRequest>) -> Self::ExecuteFuture {
        //println!("REQUEST = {:?}", request);

        let response = match &request.get_ref().plan {
            Some(plan) => match create_datafusion_plan(plan) {
                Ok(df_plan) => {
                    println!("DataFusion plan: {:?}", df_plan);

                    match execute_query(&df_plan) {
                        Ok(count) => Response::new(ExecuteResponse {
                            message: format!("Query retrieved {} rows", count),
                        }),
                        Err(e) => Response::new(ExecuteResponse {
                            message: format!("Error executing plan: {:?}", e),
                        }),
                    }
                }
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

fn execute_query(df_plan: &LogicalPlan) -> Result<usize> {
    let mut context = ExecutionContext::new();

    let optimized_plan = context.optimize(&df_plan)?;
    println!("Optimized plan: {:?}", optimized_plan);

    register_tables(&mut context, &optimized_plan);

    let relation = context.execute(&optimized_plan, 1024)?;

    let mut x = relation.borrow_mut();

    let mut count = 0;
    while let Some(batch) = x.next()? {
        count += batch.num_rows();
    }

    Ok(count)
}

//TODO this is a temporary hack to walk the plan and register tables with the context ... this isn't how it will work long term
fn register_tables(ctx: &mut ExecutionContext, plan: &LogicalPlan) {
    match plan {
        LogicalPlan::TableScan {
            table_name, schema, ..
        } => {
            ctx.register_csv(&table_name, &table_name, schema, true);
        }
        LogicalPlan::Projection { input, .. } => {
            register_tables(ctx, input);
        }
        _ => unimplemented!(),
    }
}

pub fn main() {
    let _ = ::env_logger::init();

    let new_service = server::ExecutorServer::new(BallistaService);

    let mut server = Server::new(new_service);

    let http = Http::new().http2_only(true).clone();

    let addr = "0.0.0.0:50051".parse().unwrap();
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

use log::info;
use gin::executor::service::GinExecutor;
use gin::executor::proto::gin_executor_service_server::GinExecutorServiceServer;
use tonic::transport::Server;
use std::env;

use std::net::ToSocketAddrs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args: Vec<String> = env::args().collect();

    let executor = &args[1].to_owned();
    let executor_socket = executor.to_socket_addrs().unwrap().next().unwrap();
    let scheduler = &args[2].to_owned();
    let scheduler_socket = scheduler.to_socket_addrs().unwrap().next().unwrap();
    info!("Executor started");
    let _executor = GinExecutor::new(
        executor_socket,
        scheduler_socket
    );

    let svc = GinExecutorServiceServer::new(_executor);
    Server::builder().add_service(svc).serve(executor_socket).await?;
    Ok(())
}

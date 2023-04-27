use log::info;
use gin::executor::service::GinExecutor;
use gin::executor::proto::gin_executor_service_server::GinExecutorServiceServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    info!("Executor started");
    let _executor = GinExecutor::new(
        "1".to_owned(),
        "127.0.0.1".to_owned(),
        50052,
        "http://127.0.0.1:50051".to_owned(),
    );
    let addr = "127.0.0.1:50052".parse()?;
    let svc = GinExecutorServiceServer::new(_executor);
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}

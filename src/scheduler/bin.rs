
use gin::scheduler::service::Scheduler;
use gin::scheduler::proto::gin_scheduler_service_server::GinSchedulerServiceServer;
use tonic::transport::Server;
use log::info;
// Define the main function
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    // Create a new Scheduler instance
    let scheduler = Scheduler::new();

    // Create a new tonic server
    let addr = "127.0.0.1:50051".parse()?;
    let server = Server::builder()
        .add_service(GinSchedulerServiceServer::new(scheduler))
        .serve(addr);

    // Start the server
    info!("Scheduler listening on {}", addr);
    server.await?;

    Ok(())
}
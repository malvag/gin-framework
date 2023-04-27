// mod executor;
// mod scheduler;
// use executor_proto;
use futures::executor::block_on;
use gin::executor::proto::gin_executor_service_client::GinExecutorServiceClient;
use gin::scheduler::proto::gin_scheduler_service_client::GinSchedulerServiceClient;
use gin::scheduler::proto::CheckExecutorsRequest;
// use scheduler::proto::Stage;
use gin::Job;

use log::error;
pub struct GinContext {
    scheduler: GinSchedulerServiceClient<tonic::transport::Channel>,
    executors: Vec<GinExecutorServiceClient<tonic::transport::Channel>>,
    application: Vec<Job>,
}

impl GinContext {
    //singleton instance
    fn get_instance() -> &'static mut Self {
        static mut INSTANCE: *mut GinContext = std::ptr::null_mut();
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| unsafe {
            let sched_result =
                block_on(GinSchedulerServiceClient::connect("http://127.0.0.1:50051"));
            match sched_result {
                Ok(scheduler) => {
                    INSTANCE = Box::into_raw(Box::new(GinContext {
                        scheduler,
                        executors: vec![],
                        application: vec![],
                    }));
                }
                Err(_) => {
                    error!("Could not connect to scheduler");
                }
            }
        });
        unsafe { &mut *INSTANCE }
    }
}

// pub fn create_job() {
//     let mut job_info = JobInfo::new();
//     // fill in job_info fields as needed

//     // assign job_info to the job field
//     ctx.job = Some(&mut job_info);
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let mut client = GinSchedulerServiceClient::connect("http://127.0.0.1:50051").await?;
    let gtx = GinContext::get_instance();
    let response = gtx
        .scheduler
        .check_executors(CheckExecutorsRequest {})
        .await?;

    println!("Response: {:?}", response.get_ref().executor_status);
    Ok(())
}

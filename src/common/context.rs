
// mod executor;
// mod scheduler;
// use executor_proto;
use futures::executor::block_on;
// use dataframe::DataFrame;
// use gin::executor::proto::gin_executor_service_client::GinExecutorServiceClient;
use crate::scheduler::proto::gin_scheduler_service_client::GinSchedulerServiceClient;
// use crate::scheduler::proto::CheckExecutorsRequest;
// use scheduler::proto::Stage;
// use gin::Job;
// use dataframe::{Row, DataFrame,read_from_csv};


use log::error;
pub struct GinContext {
    pub scheduler: GinSchedulerServiceClient<tonic::transport::Channel>,
    // application: Vec<Job>,
}

impl GinContext {
    //singleton instance
    pub fn get_instance() -> &'static mut Self {
        static mut INSTANCE: *mut GinContext = std::ptr::null_mut();
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| unsafe {
            let sched_result =
                block_on(GinSchedulerServiceClient::connect("http://127.0.0.1:50051"));
            match sched_result {
                Ok(scheduler) => {
                    INSTANCE = Box::into_raw(Box::new(GinContext {
                        scheduler,
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

use futures::executor::block_on;
use crate::scheduler::proto::gin_scheduler_service_client::GinSchedulerServiceClient;

use crate::common::common::S3Configuration;
pub struct GinContext {
    pub scheduler: GinSchedulerServiceClient<tonic::transport::Channel>,
    s3_config: Option<S3Configuration>,
    // application: Vec<Job>,
}

impl GinContext {
    //singleton instance
    pub fn get_context(end_point: &str) -> &'static mut Self {
        static mut INSTANCE: *mut GinContext = std::ptr::null_mut();
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(move || unsafe {
            let sched_result =
                block_on(GinSchedulerServiceClient::connect(end_point.to_owned()));
            match sched_result {
                Ok(scheduler) => {
                    INSTANCE = Box::into_raw(Box::new(GinContext {
                        scheduler,
                        s3_config: None,
                    }));
                }
                Err(_) => {
                    panic!("Could not connect to scheduler");
                }
            }
        });
        unsafe { &mut *INSTANCE }
    }

    pub fn with_s3(&'static mut self, s3_config: S3Configuration) -> &'static mut Self {
        self.s3_config = Some(s3_config);
        self
    }

    pub fn get_s3_config(&'static self) -> S3Configuration {
        self.s3_config.clone().unwrap()
    }
}

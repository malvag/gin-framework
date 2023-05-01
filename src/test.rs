use std::any::Any;

// mod executor;
// mod scheduler;
// use executor_proto;
use futures::executor::block_on;
mod dataframe;
// use dataframe::DataFrame;
// use gin::executor::proto::gin_executor_service_client::GinExecutorServiceClient;
use gin::scheduler::proto::gin_scheduler_service_client::GinSchedulerServiceClient;
use gin::scheduler::proto::CheckExecutorsRequest;
// use scheduler::proto::Stage;
// use gin::Job;
use dataframe::{Row, DataFrame,read_from_csv};


use log::error;
pub struct GinContext {
    scheduler: GinSchedulerServiceClient<tonic::transport::Channel>,
    // application: Vec<Job>,
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


    // let mut test = DataFrame::new();    

    println!("Response: {:?}", response.get_ref().executor_status);
    
    // test.select(vec!["column1".to_owned()]).count();
    let mut df: DataFrame<i32> = read_from_csv::<i32>("./mydata.csv");
    // let mut df = DataFrame::new(vec![vec![1, 2], vec![3, 4], vec![5, 6]]);
    // println!("Initial df: {:?}", df);
    let filtered_df = df.filter(|row: &Row<i32>| row.cols[0] < 3).count();
    // println!("Return value of select: {:?}", df.select(["col3".to_owned(), "col4".to_owned()].to_vec()));
    // df.select(["col1".to_owned(),"col2".to_owned()].to_vec());
    // println!("Plan after two select calls: {:?}", df.plan);
    
    // df.sum();
    // println!("Plan after sum call: {:?}", df.plan);

    // df.filter(|row| row.cols[0] > 2);
    // println!("Plan after filter call: {:?}", df.plan);

    // df.count();
    // println!("Plan after count call: {:?}", df.plan);

    // df.select(["col1".to_owned()].to_vec());
    // println!("Plan after select call: {:?}", df.plan);
    
    // df.collect();
    // println!("Plan after collect call: {:?}", df.plan);
    
    // df.filter(|row| row.cols[0] > 3);

    // println!("Plan after filter call: {:?}", df.plan);

    Ok(())
}

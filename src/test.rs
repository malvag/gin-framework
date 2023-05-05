// mod executor;
// mod scheduler;
// use executor_proto;

// use dataframe::DataFrame;
// use gin::executor::proto::gin_executor_service_client::GinExecutorServiceClient;
use gin::common::context::GinContext;
// use scheduler::proto::Stage;
// use gin::Job;

use gin::scheduler::proto::CheckExecutorsRequest;




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

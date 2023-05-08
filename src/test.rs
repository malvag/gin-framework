// mod executor;
// mod scheduler;
// use executor_proto;

// use dataframe::DataFrame;
// use gin::executor::proto::gin_executor_service_client::GinExecutorServiceClient;
use gin::common::context::GinContext;
// use scheduler::proto::Stage;
// use gin::Job;

use gin::common::dataframe::DataFrame;
use gin::scheduler::proto::CheckExecutorsRequest;




#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gtx = GinContext::get_instance();
    let response = gtx
        .scheduler
        .check_executors(CheckExecutorsRequest {})
        .await?;


    let mut df:DataFrame<f64> = DataFrame::new();    

    println!("Response: {:?}", response.get_ref().executor_status);
    
    // println!("Return value of select: {:?}", df.select(["col3".to_owned(), "col4".to_owned()].to_vec()));
    // df.select(["col1".to_owned(),"col2".to_owned()].to_vec());
    // println!("Plan after two select calls: {:?}", df.plan);
    
    // df.sum();
    // println!("Plan after sum call: {:?}", df.plan);

    // let result = df.filter("x > 2").select(["col1".to_string()].to_vec()).count();
    let result = df.count();
    println!("{}",result);
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

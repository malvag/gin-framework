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

use gin::common::common::S3Configuration;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let s3_conf = S3Configuration {
        region: "".to_string(),
        endpoint: "http://localhost:9000".to_string(),
        access_key: "1JeWf0EmWzZboFDa".to_string(),
        secret_key: "zjRu7hw9YkWucT9jyxzXgIYWC906euFs".to_string(),
    };
    let gtx = GinContext::get_instance().with_s3(s3_conf);
    let response = gtx
        .scheduler
        .check_executors(CheckExecutorsRequest {})
        .await?;


    let mut df:DataFrame<f64> = DataFrame::new();

    df.uri = "s3://test-rust-s3/yellow_tripdata_2023-01.parquet".to_owned();  

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

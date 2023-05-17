use gin::common::context::GinContext;
use gin::common::dataframe::DataFrame;
use gin::scheduler::proto::CheckExecutorsRequest;
use gin::common::common::S3Configuration;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let s3_conf = S3Configuration {
        region: "".to_string(),
        endpoint: "http://localhost:9000".to_string(),
        access_key: "test-access-key".to_string(),
        secret_key: "test-secret-key".to_string(),
    };
    let gtx = GinContext::get_instance().with_s3(s3_conf);
    let response = gtx
        .scheduler
        .check_executors(CheckExecutorsRequest {})
        .await?;

    let mut df:DataFrame<f64> = DataFrame::new();

    df.uri = "s3://test-rust-s3/new.parquet".to_owned();  

    println!("Response: {:?}", response.get_ref().executor_status);

    println!("Gin Calculating: df.width()");
    
    let result = df.width();

    println!("{} (should be 19)", result);


    println!("Gin Calculating: df.select('VendorID', 'trip_distance', 'tip_amount').width()");

    let result = df.select(&["VendorID", "trip_distance", "tip_amount"]).width();

    println!("{} (should be 3)", result);


    println!("Gin Calculating: df.filter('VendorID == 0').count()");
    
    let result = df.filter("VendorID == 0").count();

    println!("{} (should be 0)",result);


    println!("Gin Calculating: df.filter('VendorID == 1').count()");
    
    let result = df.filter("VendorID == 1").count();

    println!("{} (should be 827367)",result);


    println!("Gin Calculating: df.filter('VendorID == 2').count()");
    
    let result = df.filter("VendorID == 2").count();

    println!("{} (should be 2239397)",result);


    println!("Gin Calculating: df.filter('VendorID == 2').sum('VendorID')");
    
    let result = df.filter("VendorID == 2").sum("VendorID");

    println!("{} (should be 4478794)",result);

    Ok(())
}

use gin::common::context::GinContext;
use gin::common::dataframe::DataFrame;
use gin::common::common::S3Configuration;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    println!("\n********************************************************");
    println!(  "*                  GIN FRAMEWORK DEMO                  *");
    println!(  "*                ----------------------                *");
    println!(  "* Alexandridi         Maliaroudakis         Melidonis  *");
    println!(  "********************************************************\n");

    // ******************************************************
    //  INITIALIZE GIN CONTEXT
    // ******************************************************


    let s3_conf = S3Configuration {
        region: "".to_string(),
        endpoint: "http://localhost:9000".to_string(),
        access_key: "test-access-key".to_string(),
        secret_key: "test-secret-key".to_string(),
    };

    GinContext::get_context("http://127.0.0.1:50051").with_s3(s3_conf);


    // ******************************************************
    //  INITIALIZE A DATAFRAME
    // ******************************************************


    let mut df: DataFrame<f64> = DataFrame::read_from("s3://test-rust-s3/demo-yellow-taxi.parquet");


    // ******************************************************
    //  SELECT COLUMNS AND GET THE WIDTH OF THE DATAFRAME
    // ******************************************************


    println!("1. 'Select' and 'Width'....\n");


    println!("Gin Calculating: df.width()");
    
    let result = df.width();

    println!("{} (should be 19)\n", result);

    // -------

    println!("Gin Calculating: df.select('VendorID', 'trip_distance', 'tip_amount').width()");

    let result = df
        .select(&["VendorID", "trip_distance", "tip_amount"])
        .width();

    println!("{} (should be 3)\n", result);


    // ******************************************************
    //  FILTER AND COUNT THE ROWS OF THE DATAFRAME
    // ******************************************************


    println!("2. 'Filter' and 'Count'....\n");


    println!("Gin Calculating: df.filter('VendorID == 2').count()");
    
    let result = df
        .filter("VendorID == 2")
        .count();

    println!("{} (should be 2239397)\n",result);


    // ******************************************************
    //  FILTER THE ROWS THE DATAFRAME AND SUM A COLUMN
    // ******************************************************


    println!("3. 'Filter' and 'Sum'....\n");

    println!("Gin Calculating: df.filter('VendorID == 2').sum('VendorID')");
    
    let result = df
        .filter("VendorID == 2")
        .sum("VendorID");

    println!("{} (should be 4478794)", result);


    println!("\n********************************************************");
    println!(  "*                      THANK YOU!                      *");
    println!(  "*                ----------------------                *");
    println!(  "* Alexandridi         Maliaroudakis         Melidonis  *");
    println!(  "********************************************************\n");

    Ok(())
}

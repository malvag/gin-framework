
mod interface;
use interface::{Row, DataFrame,read_from_csv};
fn main() {

    let mut df: DataFrame<i32> = read_from_csv::<i32>("./mydata.csv");
    println!("Initial df: {:?}", df);

    println!("Return value of select: {:?}", df.select(&["col3", "col4"]));
    df.select(&["col1","col2"]);
    println!("Plan after two select calls: {:?}", df.plan);
    
    df.sum(&["col1","col2"]);
    println!("Plan after sum call: {:?}", df.plan);

    df.filter(|row| row.cols[0] > 2);
    println!("Plan after filter call: {:?}", df.plan);

    df.count();
    println!("Plan after count call: {:?}", df.plan);

    df.select(&["col1"]);
    println!("Plan after select call: {:?}", df.plan);
    
    df.collect();
    println!("Plan after collect call: {:?}", df.plan);
    
    df.filter(|row| row.cols[0] > 3);

    println!("Plan after filter call: {:?}", df.plan);

}



use arrow2::datatypes::{ Field, Schema };
use arrow2::io::parquet::write::FileMetaData;
use arrow2::datatypes::SchemaRef;
use arrow2::chunk::Chunk;
use eval::eval;
use eval::Value;
use std::collections::HashSet;
use arrow2::array::PrimitiveArray;
use arrow2::datatypes::DataType;
use arrow2::bitmap::Bitmap;
use arrow2::array::{ Array, Int64Array };


fn select(schema: &Schema, field_names: &[&str]) -> Schema {
    // TODO: use filter from Schema implementation

    let fields: Vec<Field> = schema.fields
        .iter()
        .filter(|field| field_names.contains(&&*field.name))
        .cloned()
        .collect();
    return Schema {
        fields,
        metadata: schema.metadata.clone(),
    };
}
// chunks: read::RowGroupDeserializer,
fn sum(
    chunk: &Chunk<Box<dyn Array>>,
    schema: &Schema,
    field_name: &str
) -> Result<i64, Box<dyn Error>> {
    // let chunk = chunks.into_iter().next().unwrap()?;
    let index = schema.fields
        .iter()
        .position(|field| field.name == field_name)
        .ok_or("error")?;
    // println!("Index of field '{}' is {}", field_name, index);
    let array = chunk.columns()[index].as_any().downcast_ref::<Int64Array>().ok_or("error")?;
    let sum: i64 = array.iter().flatten().sum();
    Ok(sum)
}

fn count(chunk: &Chunk<Box<dyn Array>>, schema: &Schema) -> Result<usize, Box<dyn Error>> {
    let arrays = chunk.columns();
    if arrays.len() != 0 {
        Ok(arrays[0].len())
    } else {
        Ok(0)
    }
}

fn evaluate_filter(closure_string: &str, chunk_value: i64) -> Result<bool, Box<dyn std::error::Error>> {
    // println!("eval: {:?}", eval(closure_string).unwrap());
    let tokens: Vec<&str> = closure_string.split_whitespace().collect();
    let result: bool = match eval(&format!("{} {} {}", chunk_value, tokens[1], tokens[2])).unwrap() {
        Value::Bool(num) => num.to_owned(),
        _ => todo!(),
    };
    Ok(result)
}

fn filter(
    chunk: &Chunk<Box<dyn Array>>,
    closure_string: &str
) -> Result<Chunk<Box<dyn Array>>, Box<dyn Error>> {
    let clone = chunk.clone();
    let mut indexes = vec![];
    let x = 3;
    // println!("from filter={:?}", evaluate_filter(&format!("{} == {}", value,_x),3).unwrap());
    for column in chunk.columns().iter() {
        let int_array = column.as_any().downcast_ref::<Int64Array>().unwrap();
        // println!("int_array={:#?}", int_array);

        for i in 0..int_array.len() {
            let value = int_array.value(i);
            let x = value;
            if !(evaluate_filter(closure_string,value).unwrap()) {
                indexes.push(i);
            }
        }
    }
    let unique_vec: Vec<_> = indexes.into_iter().collect::<HashSet<_>>().into_iter().collect();
    // println!("indexes={:?}", unique_vec);

    // let columns = chunk.columns();
    let mut arrays = vec![];
    for column in chunk.columns().iter() {
        let int_array = column.as_any().downcast_ref::<Int64Array>().unwrap();
        let filtered = int_array
            .iter()
            .enumerate()
            .filter(|(i, _)| !unique_vec.contains(&i))
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        let new: PrimitiveArray<i64> = filtered.into_iter().map(|x| x.cloned()).collect::<Vec<_>>().into();
        let new2 = Box::new(new.clone()) as Box<dyn arrow2::array::Array>;
        arrays.push(new2);
    }
    
    Ok(Chunk::new(arrays))
}

// works. Initializing a chunk manually
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn Error>> {
//     let values = vec![1, 2, 3, 4, 5];

//     let array = Int64Array::from_slice(&values);
//     let column: Box<dyn Array> = Box::new(array);

//     let num_rows = 3;
//     let mut chunk = Chunk::new(vec![column; num_rows]);
//     println!("{:#?}", chunk);

//     let res3 = filter(&chunk, "x < 4");
//     println!("Result from filter={:#?}", res3.unwrap());
//     Ok(())
// }
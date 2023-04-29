use std::fmt::Debug;

#[derive(Debug)]
pub struct DataFrame <T> {
    pub data: Vec<Row<T>>,
    pub plan: Vec<Methods<T>>
}

#[derive(Debug)]
pub enum Methods<T> {
    Select(Vec<String>),
    Count,
    Collect,
    Sum(Vec<String>),
    Filter(fn(Row<T>) -> bool)
}


#[derive(Debug,Clone)]
pub struct Row <T> {
    pub cols: Vec<T>,
}


impl <T:Debug+Clone> DataFrame <T> {

    //Action
    pub fn count(&mut self) -> &mut DataFrame <T>{
        self.plan.push(Methods::Count);

        // call gRPC service

        self.plan.clear();
        self
    }

    //Action
    pub fn sum(&mut self, columns:&[&str]) -> &mut DataFrame <T> {
        let mut tokens: Vec<String> = vec![];
        for column in columns{
            tokens.push(column.to_string())
        }
        self.plan.push(Methods::Sum(tokens));
        
        // call gRPC service

        self.plan.clear();
        self
    }

    //Action
    pub fn collect(&mut self) -> &mut DataFrame <T>{
        self.plan.push(Methods::Collect);

        // call gRPC service

        self.plan.clear();
        self
    }

    //Transformation
    pub fn select(&mut self, columns:&[& str]) -> DataFrame <T>{
        let mut tokens: Vec<String> = vec![];
        for column in columns{
            tokens.push(column.to_string())
        }
        self.plan.push(Methods::Select(tokens));
        let new_df = DataFrame {
            data: self.data.clone(),
            plan: vec![],
        };
        new_df  
    }

    //Transformation
    pub fn filter(&mut self,condition: fn(Row<T>)-> bool) -> DataFrame <T> {
        // self.plan.push(Methods::Where(condition));
        let action: Methods <T> = Methods::Filter(condition);
        
        self.data.retain(|row| {
            let result = match action {
                Methods::Filter(f) => f(row.clone()),
                _ => {
                    println!("Other variant....");
                    false
                }
            };
            println!("Result: {}", result);
            result
        });

        self.plan.push(action);
        let new_df = DataFrame {
            data: self.data.clone(),
            plan: vec![],
        };
        new_df
    }

    // pub unsafe extern "C" fn func(mut args: ...) {
    //     // implementation
    // }

}

pub fn read_from_csv<T: Debug>(url: &str) -> DataFrame<i32> {
    println!("Reading from path: {}",url);
    let row1 = Row {
        cols: vec![1, 2, 3],
    };
    let row2 = Row {
        cols: vec![4, 5, 6],
    };
    let row3 = Row {
        cols: vec![7, 8, 9],
    };
    let data = vec![row1, row2, row3];
    let plan = vec![]; 
    DataFrame {
         data,
         plan
    }
}


// fn add(x: i32, y: i32) -> i32 {
//     x + y
// }

// fn add(x: f32, y: f32) -> f32 {
//     x + y
// }


use crate::scheduler::proto::SubmitJobRequest;
use crate::common::common::{ActionType, Filter, Select, Stage};
use log::{debug, error};
use crate::common::common::stage::StageType;

use std::fmt::Debug;
use std::mem;
use futures::executor::block_on;

pub struct DataFrame<T> {
    pub uri: String,
    data: Vec<Row<T>>,
    plan: Vec<Methods<T>>,
}

#[derive(Clone)]
pub enum Methods<T> {
    Select(Vec<String>),
    Count,
    Collect,
    Sum,
    Filter(fn(&Row<T>) -> bool),
}

#[derive(Clone)]
pub struct Row<T> {
    pub cols: Vec<T>,
}

fn serialize_filter<T, F>(filter: &F) -> Vec<u8>
where
    F: Fn(&Row<T>) -> bool,
{
    let filter_ptr = filter as *const F as *const u8;
    let filter_size = mem::size_of::<F>();
    let mut buffer = Vec::with_capacity(filter_size);
    unsafe {
        buffer.set_len(filter_size);
        std::ptr::copy_nonoverlapping(filter_ptr, buffer.as_mut_ptr(), filter_size);
    }
    buffer
}

fn deserialize_filter<T, F>(bytes: &[u8]) -> &'static F
where
    F: Fn(&Row<T>) -> bool + 'static,
{
    let filter_ptr = bytes.as_ptr() as *const F;
    unsafe { &*filter_ptr }
}

impl<T: Debug + Clone> DataFrame<T> {
    pub fn new() -> Self {
        Self {
            uri: "".to_owned(),
            data: Vec::new(),
            plan: Vec::new(),
        }
    }

    //Action
    pub fn count(&mut self) -> f64 {
        self.plan.push(Methods::Count);

        // call gRPC service
        self.sync_send_execution_graph()
    }

    //Action
    pub fn sum(&mut self) -> f64 {
        self.plan.push(Methods::Sum);

        // call gRPC service
        self.sync_send_execution_graph()
    }

    //Action
    pub fn collect(&mut self) -> &mut DataFrame<T> {
        self.plan.push(Methods::Collect);

        // call gRPC service
        self.sync_send_execution_graph();

        self
    }

    //Transformation
    pub fn select(&mut self, columns: Vec<String>) -> DataFrame<T> {
        self.plan.push(Methods::Select(columns));

        DataFrame {
            uri: self.uri.to_owned(),
            data: self.data.clone(),
            plan: self.plan.clone(),
        }
    }

    //Transformation
    pub fn filter(&mut self, condition: fn(&Row<T>) -> bool) -> DataFrame<T> {

        let action: Methods<T> = Methods::Filter(condition);
        self.plan.push(action);
        
        DataFrame {
            uri: self.uri.to_owned(),
            data: self.data.clone(),
            plan: self.plan.clone(),
        }
    }

    fn sync_send_execution_graph(&mut self) -> f64{
        let mut stage_vec: Vec<Stage> = Vec::new();
        let mut index = 0;
        for node in self.plan.iter() {
            let stage: Stage;
            match node {
                Methods::Collect => {
                    debug!("Methods:Collect");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Action(
                            ActionType::Collect.into(),
                        )),
                    };
                }
                Methods::Count => {
                    debug!("Methods:Count");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Action(
                            ActionType::Count.into(),
                        )),
                    };
                }
                Methods::Sum => {
                    debug!("Methods:Sum");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Action(
                            ActionType::Sum.into(),
                        )),
                    };
                }
                Methods::Filter(func) => {
                    debug!("Methods:Filter");
                    let closure_bytes = serialize_filter(func);
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Filter(Filter {
                            predicate: closure_bytes.to_vec(),
                        })),
                    };
                }
                Methods::Select(select_vec) => {
                    debug!("Methods:Select");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Select(Select {
                            columns: select_vec.clone(),
                        })),
                    };
                }
            }
            stage_vec.push(stage);
            index += 1;
        }
        let stage_vec_clone = stage_vec.clone();
        //send execution graph to scheduler
        let request = SubmitJobRequest {
            dataset_uri: self.uri.to_owned(),
            plan: stage_vec,
        };
        match block_on(crate::common::context::GinContext::get_instance().scheduler.submit_job(request)) {
            Ok(response) => {
                debug!("Success");
                let action_stage = match stage_vec_clone.last() {
                    Some(stage) => stage,
                    None => {
                        error!("Submit Job failed: Could not get action stage from plan");
                        return -1.0;
                    },
                };
                match action_stage.stage_type.as_ref().unwrap() {
                    StageType::Action(action_type) => {
                        let response_stage_type = match action_type {
                            0 => ActionType::Sum,
                            1 => ActionType::Count,
                            2 => ActionType::Collect,
                            _ => {
                                error!("Submit Job failed: Invalid action type");
                                return -1.0;
                            }
                        };
                        match response_stage_type {
                            ActionType::Sum | ActionType::Count=>{
                                let arr: [u8; 8] = response.into_inner().result.try_into().unwrap();
                                let result: f64 = f64::from_le_bytes(arr); 
                                // [TODO]
                                // maybe have multiple returnable types?
                                return result;
                            },
                            ActionType::Collect =>{
                                // [TODO]
                                // maybe discuss the format of this?
                                todo!()
                            }
                        }
                    },
                    StageType::Filter(_) => error!("Submit Job failed: Invalid action type"),
                    StageType::Select(_) => error!("Submit Job failed: Invalid action type"),
                }
                let status = -1.0;
                status
            },
            Err(_status) =>{ 
                panic!("Submit job failed: {}", _status);
            }
        }
    }
}

pub fn read_from_csv<T: Debug>(url: &str) -> DataFrame<i32> {
    println!("Reading from path: {}", url);
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
        uri: url.to_owned(),
        data,
        plan,
    }
}
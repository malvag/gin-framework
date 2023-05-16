use crate::scheduler::proto::SubmitJobRequest;
use crate::common::common::{ActionType, Select, Stage, Filter};
use crate::common::context::GinContext;
use log::{debug, error};
use crate::common::common::stage::StageType;

use std::fmt::Debug;
use futures::executor::block_on;

pub struct DataFrame<T> {
    pub uri: String,
    data: Vec<Row<T>>,
    plan: Vec<Methods>,
}

#[derive(Clone)]
pub enum Methods {
    Select(Vec<String>),
    Count,
    Collect,
    Sum,
    Filter(String),
}

#[derive(Clone)]
pub struct Row<T> {
    pub cols: Vec<T>,
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
        // call gRPC service
        self.sync_send_execution_graph(Methods::Count)
    }

    //Action
    pub fn sum(&mut self) -> f64 {
        // call gRPC service
        self.sync_send_execution_graph(Methods::Sum)
    }

    //Action
    pub fn collect(&mut self) -> &mut DataFrame<T> {
        // call gRPC service
        self.sync_send_execution_graph(Methods::Collect);

        self
    }

    //Transformation
    pub fn select(&mut self, columns: Vec<String>) -> DataFrame<T> {
        let action = Methods::Select(columns);
        let mut plan = self.plan.clone();
        plan.push(action);
        
        DataFrame {
            uri: self.uri.to_owned(),
            data: self.data.clone(),
            plan,
        }
    }

    //Transformation
    pub fn filter(&mut self, condition: &str) -> DataFrame<T> {

        let action: Methods = Methods::Filter(condition.to_string());
        let mut plan = self.plan.clone();
        plan.push(action);
        
        DataFrame {
            uri: self.uri.to_owned(),
            data: self.data.clone(),
            plan,
        }
    }

    fn sync_send_execution_graph(&mut self, action: Methods) -> f64{
        let mut plan = self.plan.clone();
        plan.push(action);

        let mut stage_vec: Vec<Stage> = Vec::new();
        let mut index = 0;
        for node in plan.iter() {
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
                Methods::Filter(_func) => {
                    debug!("Methods:Filter");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Filter(Filter {
                            predicate: _func.to_owned(),
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
        let gtx = GinContext::get_instance();
        let stage_vec_clone = stage_vec.clone();
        //send execution graph to scheduler
        let request = SubmitJobRequest {
            dataset_uri: self.uri.to_owned(),
            plan: stage_vec,
            s3_conf: Some(gtx.get_s3_config()),
        };
        match block_on(GinContext::get_instance().scheduler.submit_job(request)) {
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
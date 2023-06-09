use crate::scheduler::proto::SubmitJobRequest;
use crate::common::common::{ActionType, Select, Stage, Filter, SumCol};
use crate::common::context::GinContext;
use log::{debug, error};
use crate::common::common::stage::{ StageType, ActionField };

use std::fmt::Debug;
use futures::executor::block_on;

pub struct DataFrame<T> {
    pub uri: String,
    data: Vec<Row<T>>,
    plan: Vec<Methods>,
}

#[derive(Clone)]
pub enum Methods {
    // Transformations
    Filter(String),
    Select(Vec<String>),
    // Actions
    Count,
    Collect,
    Sum(String),
    Width,
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

    pub fn read_from(uri: &str) -> Self {
        Self {
            uri: uri.to_owned(),
            data: Vec::new(),
            plan: Vec::new(),
        }
    }

    //Action: Count the number of rows.
    pub fn count(&mut self) -> f64 {
        // call gRPC service
        self.sync_send_execution_graph(Methods::Count)
    }

    //Action: Sum of the values of the specified column.
    pub fn sum(&mut self, sum_col: &str) -> f64 {
        // call gRPC service
        self.sync_send_execution_graph(Methods::Sum(sum_col.to_string()))
    }

    //Action: todo
    pub fn collect(&mut self) -> &mut DataFrame<T> {
        // call gRPC service
        self.sync_send_execution_graph(Methods::Collect);

        self
    }

    //Action: Get the number of columns
    pub fn width(&mut self) -> usize {
        // call gRPC service
        self.sync_send_execution_graph(Methods::Width) as usize
    }

    //Transformation: Select columns
    pub fn select(&mut self, columns: &[&str]) -> DataFrame<T> {
        let col_vect = columns.into_iter().map(|&s| String::from(s)).collect();
        let action = Methods::Select(col_vect);
        let mut plan = self.plan.clone();
        plan.push(action);

        DataFrame {
            uri: self.uri.to_owned(),
            data: self.data.clone(),
            plan,
        }
    }

    //Transformation: Filter the rows based on a condition.
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

    fn sync_send_execution_graph(&mut self, action: Methods) -> f64 {
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
                        action_field: None,
                    };
                }
                Methods::Count => {
                    debug!("Methods:Count");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Action(
                            ActionType::Count.into(),
                        )),
                        action_field: None,
                    };
                }
                Methods::Sum(sum_col) => {
                    debug!("Methods:Sum");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Action(
                            ActionType::Sum.into(),
                        )),
                        action_field: Some(ActionField::SumCol(SumCol {
                            field_name: sum_col.to_string() 
                        })),
                    };
                }
                Methods::Width => {
                    debug!("Methods:Width");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Action(
                            ActionType::Width.into()
                        )),
                        action_field: None,
                    }

                }
                Methods::Filter(_func) => {
                    debug!("Methods:Filter");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Filter(Filter {
                            predicate: _func.to_owned(),
                        })),
                        action_field: None,
                    };
                }
                Methods::Select(select_vec) => {
                    debug!("Methods:Select");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(StageType::Select(Select {
                            columns: select_vec.clone(),
                        })),
                        action_field: None,
                    };
                }
            }
            stage_vec.push(stage);
            index += 1;
        }
        let gtx = GinContext::get_context("http://127.0.0.1:50051");
        let stage_vec_clone = stage_vec.clone();
        //send execution graph to scheduler
        let request = SubmitJobRequest {
            dataset_uri: self.uri.to_owned(),
            plan: stage_vec,
            s3_conf: Some(gtx.get_s3_config()),
        };
        match block_on(GinContext::get_context("http://127.0.0.1:50051").scheduler.submit_job(request)) {
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
                            3 => ActionType::Width,
                            _ => {
                                error!("Submit Job failed: Invalid action type");
                                return -1.0;
                            }
                        };
                        match response_stage_type {
                            ActionType::Sum | ActionType::Count | ActionType::Width => {
                                let arr: [u8; 8] = response.into_inner().result.try_into().unwrap();
                                let result: f64 = f64::from_le_bytes(arr); 
                                // [TODO]
                                // maybe have multiple returnable types?
                                return result;
                            },
                            ActionType::Collect => {
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

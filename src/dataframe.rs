use crate::GinContext;
use futures::future::poll_fn;
use gin::scheduler::proto::{
    gin_scheduler_service_client::GinSchedulerServiceClient, SubmitJobRequest,SubmitJobResponse
};
use gin::scheduler::proto::{ActionType, Filter, Select, Stage};
use log::debug;
use log::error;
use prost::encoding::bytes;
use std::fmt::Debug;
use std::mem;
use futures::executor::block_on;

pub struct DataFrame<T> {
    pub uri: String,
    pub data: Vec<Row<T>>,
    pub plan: Vec<Methods<T>>,
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
            let mut stage: Stage;
            match node {
                Methods::Collect => {
                    debug!("Methods:Collect");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(gin::scheduler::proto::stage::StageType::Action(
                            ActionType::Collect.into(),
                        )),
                    };
                }
                Methods::Count => {
                    debug!("Methods:Count");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(gin::scheduler::proto::stage::StageType::Action(
                            ActionType::Count.into(),
                        )),
                    };
                }
                Methods::Sum => {
                    debug!("Methods:Sum");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(gin::scheduler::proto::stage::StageType::Action(
                            ActionType::Sum.into(),
                        )),
                    };
                }
                Methods::Filter(func) => {
                    debug!("Methods:Filter");
                    let closure_bytes = serialize_filter(func);
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(gin::scheduler::proto::stage::StageType::Filter(Filter {
                            predicate: closure_bytes.to_vec(),
                        })),
                    };
                }
                Methods::Select(select_vec) => {
                    debug!("Methods:Select");
                    stage = Stage {
                        id: index.to_owned().to_string(),
                        stage_type: Some(gin::scheduler::proto::stage::StageType::Select(Select {
                            columns: select_vec.clone(),
                        })),
                    };
                }
            }
            stage_vec.push(stage);
            index += 1;
        }
        //send execution graph to scheduler
        let request = SubmitJobRequest {
            dataset_uri: self.uri.to_owned(),
            plan: stage_vec,
        };
        match block_on(GinContext::get_instance().scheduler.submit_job(request)) {
            Ok(response) => {
                debug!("Success");
                let result: f64 = serde_cbor::from_slice(&response.into_inner().result).unwrap();
                result
            },
            Err(status) =>{ 
                panic!("Submit job failed");
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
use crate::common::common::stage::StageType;
use crate::executor::proto::{
    gin_executor_service_server::GinExecutorService, Empty, LaunchTaskRequest, LaunchTaskResponse,
};
use crate::scheduler::proto::gin_scheduler_service_client::GinSchedulerServiceClient;
use crate::scheduler::proto::{RegisterExecutorRequest, UnregisterExecutorRequest};
use futures::executor::block_on;

use log::{info,debug};

use tonic::{Request, Response, Status};
use log::error;
use rhai::{Engine, Locked};
use std::cell::RefCell;
use std::net::SocketAddr;
use eval::eval;
use eval::Value;

#[derive(Debug)]
pub struct GinExecutor {
    id: i32,
    address: SocketAddr,
    scheduler_address: SocketAddr,
}

impl GinExecutor {
    pub fn new(id: i32, addr: SocketAddr, scheduler: SocketAddr,) -> Self {
        let ob = Self {
            id: id,
            address: addr,
            scheduler_address: scheduler
        };
        ob._attach_to_scheduler();
        ob
    }

    fn filter(&self,closure_string: &str, x: f64) -> Result<bool, Box<dyn std::error::Error>> {

        let result: bool = match eval(closure_string).unwrap() {
            Value::Bool(num) => num.to_owned(),
            _ => todo!()
        };
        Ok(result)
    }
    
    pub fn get_uri(&self) -> String{
        format!("http://{}:{}",self.address.ip(),self.address.port())
    }
    pub fn get_scheduler_uri(&self) -> String{
        format!("http://{}:{}",self.scheduler_address.ip(),self.scheduler_address.port())
    }


    pub fn _detach_from_scheduler(&self){
        let sched_client_try = block_on(GinSchedulerServiceClient::connect(self.get_scheduler_uri()));
        let sched_client = match sched_client_try {
            Ok(client) => client,
            Err(error) => panic!("Problem connecting to scheduler: {:?}", error),
        };
        debug!("Connected to scheduler");
        let request = UnregisterExecutorRequest { executor_uri: self.get_uri() };
        let res = block_on(sched_client.to_owned().unregister_executor(request));
        let _ = match res {
            Ok(_) => {}
            Err(error) => panic!("Problem unregistering scheduler: {:?}", error),
        };

        info!("Unregistered from scheduler");
    }

    pub fn _attach_to_scheduler(&self) {
        let sched_client_try = block_on(GinSchedulerServiceClient::connect(self.get_scheduler_uri()));
        let request = RegisterExecutorRequest { executor_uri: self.get_uri() };
        let sched_client = match sched_client_try {
            Ok(client) => client,
            Err(error) => panic!("Problem connecting to scheduler: {:?}", error),
        };
        debug!("Connected to scheduler");
        let res = block_on(sched_client.to_owned().register_executor(request));
        let _ = match res {
            Ok(_) => {}
            Err(error) => panic!("Problem registering scheduler: {:?}", error),
        };

        info!("Registered to scheduler");
    }
}

#[tonic::async_trait]
impl GinExecutorService for GinExecutor {
    async fn heartbeat(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        debug!("Bah dup!");
        Ok(Response::new(Empty {}))
    }

    async fn launch_task(
        &self,
        _request: Request<LaunchTaskRequest>,
    ) -> Result<Response<LaunchTaskResponse>, Status> {
        // self._launch_task(request).await

        info!("Task launched");
        // let execution = _request.get_ref().clone();
        // for plan_step in execution.plan {
        //     let step = match plan_step.stage_type {
        //         Some(stype) => stype,
        //         None => {
        //             return Err(Status::aborted(
        //                 "Failed launching task on executor. Corrupted state?",
        //             ));
        //         }
        //     };
        //     match step {
        //         StageType::Filter(_filter) => {
        //             todo!();
        //         }

        //         StageType::Select(_columns) => {
        //             todo!();
        //         }

        //         StageType::Action(_action) => {
        //             todo!();
        //         }
        //     }
        // }
        // test

        let x = 6.0;
        let closure_string = "x < 2";
        let result = self.filter(closure_string,x).unwrap();

        assert_eq!(result, false);

        let demo_result: f64 = 10.0;
        let demo_response = LaunchTaskResponse {
            executor_id: 0,
            success: true,
            result: serde_cbor::to_vec(&demo_result).unwrap(),
        };
        Ok(Response::new(demo_response))
        // Ok(Response::new(Empty {}))
    }
}

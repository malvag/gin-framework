use crate::common::common::stage::StageType;
use crate::executor::proto::{
    gin_executor_service_server::GinExecutorService, Empty, LaunchTaskRequest, LaunchTaskResponse,
};
use crate::scheduler::proto::gin_scheduler_service_client::GinSchedulerServiceClient;
use crate::scheduler::proto::RegisterExecutorRequest;
use futures::executor::block_on;

use log::info;

use tonic::{Request, Response, Status};
struct TranformationEngine {
    engine: rhai::Engine,
}
use log::error;
use rhai::{Engine};

impl TranformationEngine {
    pub fn new() -> Self {
        let engine = Engine::new();
        TranformationEngine { engine }
    }
    pub fn run_filter(&self, filter_fn: &str, element: f64) -> bool {
        let mut scope = rhai::Scope::new();
        let prescript = "_internal = ";
        let script = prescript.to_owned() + filter_fn; // " x < 5"

        let ast = match self.engine.compile(&script) {
            Ok(ast) => ast,
            Err(e) => {
                error!("Wrong filter function {}", e);
                return false;
            }
        };
        scope.push("_internal", false);
        scope.push("x", element);

        // Evaluate it and panic otherwise
        match self.engine.run_ast_with_scope(&mut scope, &ast) {
            Ok(_) => {},
            Err(e) => {
                error!("Error processing element through the filter function: {}", e);
                return false;
            }
        }


        // debug!("{}", scope.get_value::<bool>("_internal").unwrap());


        scope.get_value::<bool>("_internal").unwrap()
    }
}

#[derive(Debug)]
pub struct GinExecutor {
    id: String,
    hostname: String,
    scheduler_uri: String,
    port: i32,
    connected: bool,
}

impl GinExecutor {
    pub fn new(id: String, hostname: String, port: i32, scheduler_uri: String) -> Self {
        let ob = Self {
            id,
            hostname,
            scheduler_uri,
            port,
            connected: true,
        };
        ob._attach_to_scheduler();
        ob
    }
    pub fn _attach_to_scheduler(&self) {
        let scheduler_uri = self.scheduler_uri.clone();
        let uri = format!("http://{}:{}", self.hostname.clone(), self.port);
        let sched_client_try = block_on(GinSchedulerServiceClient::connect(scheduler_uri));
        let request = RegisterExecutorRequest { executor_uri: uri };
        let sched_client = match sched_client_try {
            Ok(client) => client,
            Err(error) => panic!("Problem connecting to scheduler: {:?}", error),
        };
        info!("Connected to scheduler");
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
        info!("Bah dup!");
        Ok(Response::new(Empty {}))
    }

    async fn launch_task(
        &self,
        _request: Request<LaunchTaskRequest>,
    ) -> Result<Response<LaunchTaskResponse>, Status> {
        // self._launch_task(request).await

        info!("Task launched");
        let execution = _request.get_ref().clone();
        for plan_step in execution.plan {
            let step = match plan_step.stage_type {
                Some(stype) => stype,
                None => {
                    return Err(Status::aborted(
                        "Failed launching task on executor. Corrupted state?",
                    ));
                }
            };
            match step {
                StageType::Filter(_filter) => {
                    todo!();
                }

                StageType::Select(_columns) => {
                    todo!();
                }

                StageType::Action(_action) => {
                    todo!();
                }
            }
        }
        // test
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

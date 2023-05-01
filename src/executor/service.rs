use crate::executor::proto::{
    gin_executor_service_server::GinExecutorService, Empty, ExecutorInfo, HeartbeatResponse,
    KillTaskRequest, LaunchTaskRequest,
};
use crate::scheduler::proto::gin_scheduler_service_client::GinSchedulerServiceClient;
use crate::scheduler::proto::RegisterExecutorRequest;
use futures::executor::block_on;


use log::info;




use tonic::{Request, Response, Status};
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

    pub fn _get_info(&self) -> ExecutorInfo {
        ExecutorInfo {
            executor_id: self.id.clone(),
            hostname: self.hostname.clone(),
            port: self.port,
            connected: self.connected,
        }
    }

    pub async fn _heartbeat(&self) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse {
            executor_id: self.id.clone(),
        }))
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

    pub async fn _launch_task(
        &mut self,
        request: Request<LaunchTaskRequest>,
    ) -> Result<Response<Empty>, Status> {
        let _task_info = request.into_inner().task_info;
        // Implement launching of task
        Ok(Response::new(Empty {}))
    }

    pub async fn _kill_task(
        &mut self,
        request: Request<KillTaskRequest>,
    ) -> Result<Response<Empty>, Status> {
        let _task_id = request.into_inner().task_id;
        // Implement killing of task
        Ok(Response::new(Empty {}))
    }
}

#[tonic::async_trait]
impl GinExecutorService for GinExecutor {
    async fn heartbeat(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        info!("Bah dup!");
        self.to_owned()._heartbeat().await
    }

    async fn launch_task(
        &self,
        _request: Request<LaunchTaskRequest>,
    ) -> Result<Response<Empty>, Status> {
        // self._launch_task(request).await
        info!("Task launched");
        Ok(Response::new(Empty {}))
    }

    async fn kill_task(
        &self,
        _request: Request<KillTaskRequest>,
    ) -> Result<Response<Empty>, Status> {
        // self._kill_task(request).await
        info!("Task removed");
        Ok(Response::new(Empty {}))
    }
}


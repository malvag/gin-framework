use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::common::common::ActionType;
use crate::executor::proto::gin_executor_service_client::GinExecutorServiceClient;
use crate::executor::proto::{Empty, LaunchTaskRequest, LaunchTaskResponse};
use crate::scheduler::proto::gin_scheduler_service_server::GinSchedulerService;
use crate::scheduler::proto::CheckExecutorsRequest;
use crate::scheduler::proto::CheckExecutorsResponse;
use log::debug;
use log::error;
use log::info;
use std::sync::mpsc;
use std::thread;
use tokio::runtime::Runtime;
use tonic::{Request, Response, Status};

use crate::scheduler::proto::{
    RegisterExecutorRequest, RegisterExecutorResponse, SubmitJobRequest, SubmitJobResponse,
    UnregisterExecutorRequest, UnregisterExecutorResponse,
};

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common::common::stage::StageType;

use crate::common::parquet_reader::ParquetReader;
pub struct IdGenerator {
    next_id: AtomicUsize,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self {
            next_id: AtomicUsize::new(0),
        }
    }

    pub fn generate_id(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}

pub struct Scheduler {
    executors: Arc<Mutex<HashMap<String, bool>>>,
    // id_generator: Arc<IdGenerator>,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            executors: Arc::new(Mutex::new(HashMap::new())),
            // id_generator: Arc::new(IdGenerator::new()),
        }
    }

    fn aggregate_results(&self, bytes_vec: Vec<Vec<u8>>) -> Result<f64, Box<dyn std::error::Error>> {
        let mut sum = 0.0;
        for bytes in bytes_vec {
            let result: f64 = serde_cbor::from_slice(&bytes).unwrap();
            sum += result;
        }
        Ok(sum)
    }
    

    async fn sync_send_launch(
        &self,
        _request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let executors_copy: Arc<Vec<String>> = Arc::new(Vec::from_iter({
            let executors_guard = self.executors.lock().unwrap();
            (*executors_guard)
                .clone()
                .keys()
                .map(|k| k.to_owned())
                .collect::<Vec<String>>()
        }));
        // Create a channel with a buffer size of the number of workers
        let (tx, rx) = mpsc::channel::<()>();
        // Create a channel for results with a buffer size of the number of workers
        let (result_tx, result_rx) = mpsc::channel::<Result<LaunchTaskResponse, String>>(); //executors_copy.len()

        let s3_config = _request.get_ref().to_owned().s3_conf.unwrap();

        let parquet_reader = ParquetReader::new(&s3_config, &_request.get_ref().to_owned().dataset_uri).await.unwrap();
        let metadata = match parquet_reader.read_metadata().await {
            Ok(meta) => meta,
            Err(_) => return Err(Status::aborted("Read metadata failed.")),
        };

        info!("Read Parquet: {{ version: {}, rows: {} }}", metadata.version, metadata.num_rows);

        // Number of worker threads
        for i in 0..executors_copy.clone().len() {
            let tx = tx.clone();
            let result_tx = result_tx.clone();
            let thread_id = i + 1;
            let _request_copy = _request.get_ref().to_owned();
            let executor = executors_copy[i].clone();
            // Spawn a new worker thread
            thread::spawn(move || {
                debug!("Scheduler's thread {} started", thread_id);

                let rt = Runtime::new().unwrap();

                let mut client: GinExecutorServiceClient<tonic::transport::Channel> =
                    match rt.block_on(GinExecutorServiceClient::connect(executor.clone())) {
                        Ok(client) => client,
                        Err(e) => {
                            // Executor is not reachable
                            error!("Could not connect to executor {}: {}", executor.clone(), e);

                            result_tx
                                .send(Err(format!(
                                    "Could not connect to executor {}",
                                    executor.clone()
                                )))
                                .unwrap();
                            // Signal completion to the main thread
                            tx.send(()).unwrap();
                            return;
                        }
                    };

                let submit_task = LaunchTaskRequest {
                    executor_id: i32::abs(i.try_into().unwrap()),
                    plan: _request_copy.plan.clone(),
                    dataset_uri: _request_copy.dataset_uri.to_owned(),
                    s3_conf: _request_copy.s3_conf.clone(),
                    partition_index: i as u32, // TODO: cahnge to row_group read from metadata.
                };

                let _response = match rt.block_on(client.launch_task(submit_task)) {
                    Ok(response) => response,
                    Err(e) => {
                        // Executor is not reachable

                        error!(
                            "Could not get response from executor {}: {}",
                            executor.clone(),
                            e
                        );

                        result_tx
                            .send(Err(format!(
                                "Could not get response from executor {}",
                                executor.clone()
                            )))
                            .unwrap();
                        // Signal completion to the main thread
                        tx.send(()).unwrap();
                        return;
                    }
                };

                // Send the result back to the main thread
                result_tx.send(Ok(_response.into_inner())).unwrap();

                // thread::sleep(std::time::Duration::from_secs(1));
                debug!("Scheduler's thread {} finished", thread_id);

                // Signal completion to the main thread
                tx.send(()).unwrap();
            });
        }
        // Wait for messages from all the worker threads
        for _ in 0..executors_copy.len() {
            rx.recv().unwrap();
        }

        // Collect the results from all the worker threads
        let mut results = Vec::with_capacity(executors_copy.len());
        for _ in 0..executors_copy.len() {
            match result_rx.recv().unwrap() {
                Ok(response) => results.push(Ok(response)),
                Err(err) => results.push(Err(err)),
            }
        }
        let action_stage = match _request.get_ref().plan.last() {
            Some(stage) => stage,
            None => {
                return Err(Status::aborted("Launch Job failed: Could not get last stage from plan. Corrupted state?"));
            },
        };

        match action_stage.stage_type.as_ref().unwrap() {
            StageType::Action(action_type) => {
                let req_to_client_stage_type = match action_type {
                    0 => ActionType::Sum,
                    1 => ActionType::Count,
                    2 => ActionType::Collect,
                    3 => ActionType::Width,
                    _ => {
                        return Err(Status::aborted("Could not evaluate action stage after execution. Corrupted state?"));
                    }
                };
                match req_to_client_stage_type {
                    ActionType::Sum | ActionType::Count | ActionType::Width => {
                        // deserialize the result from every thread
                        let mut aggregated: Vec<Vec<u8>> = Vec::new();
                        for thread_result in results {
                            match thread_result {
                            Ok(result) =>{
                                aggregated.push(result.result);
                            },
                            Err(_) => {return Err(Status::aborted("Could not evaluate result from thread. Corrupted state?"));}
                        }
                        }
                        let unserialized_result = self.aggregate_results(aggregated);
                        // serialize it for the client
                        let serialized = match unserialized_result{
                            Ok(unserialized) => {
                                unserialized.to_le_bytes()
                            },
                            Err(_) => {return Err(Status::aborted("Could not evaluate result from thread. Corrupted state?"));}
                        };
                        // [TODO]
                        // maybe have multiple returnable types?
                        let response = SubmitJobResponse {
                            success: true,
                            result: serialized.to_vec()
                        };
                        return Ok(Response::new(response));
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
        // All worker threads have completed the task successfully
        // debug!("All workers completed the task successfully");

        return Err(Status::aborted("Failed launching task. Corrupted state?"));
    }
}
// Implement the service methods
#[tonic::async_trait]
impl GinSchedulerService for Scheduler {
    async fn register_executor(
        &self,
        _request: Request<RegisterExecutorRequest>,
    ) -> Result<Response<RegisterExecutorResponse>, Status> {
        let uri = _request.get_ref().clone();

        // Add the executor to the list of connected executors
        let mut executors = self.executors.lock().unwrap();
        executors.insert(uri.executor_uri.to_owned(), true);

        let response = RegisterExecutorResponse { success: true };
        info!(
            "Executor {} connected!",
            _request.get_ref().executor_uri.clone()
        );
        Ok(Response::new(response))
    }

    async fn unregister_executor(
        &self,
        _request: Request<UnregisterExecutorRequest>,
    ) -> Result<Response<UnregisterExecutorResponse>, Status> {
        let uri = _request.get_ref().clone();

        // Add the executor to the list of connected executors
        let mut executors = self.executors.lock().unwrap();
        executors.remove(&uri.executor_uri.to_owned());

        let response = UnregisterExecutorResponse { success: true };
        info!(
            "Executor {} disconnected!",
            _request.get_ref().executor_uri.clone()
        );
        Ok(Response::new(response))
    }

    async fn submit_job(
        &self,
        _request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let graph = &_request.get_ref().plan;

        for stage in graph.iter() {
            match &stage.stage_type {
                Some(StageType::Action(method_type)) => {
                    debug!("action {}", method_type);
                }
                Some(StageType::Filter(_method_type)) => {
                    debug!("filter");
                }
                Some(StageType::Select(_method_type)) => {
                    debug!("select");
                }
                None => debug!("No valid method"),
            }
        }

        self.sync_send_launch(_request).await
    }

    async fn check_executors(
        &self,
        _request: Request<CheckExecutorsRequest>,
    ) -> Result<Response<CheckExecutorsResponse>, Status> {
        debug!("Checking executors!");

        let mut executor_stats = HashMap::<String, bool>::new();
        let executors_copy: HashMap<String, bool> = {
            let executors_guard = self.executors.lock().unwrap();
            (*executors_guard).clone()
        };
        for (uri, executor) in executors_copy.iter() {
            debug!("{} {}", uri.clone(), executor.clone());
            let mut client = match GinExecutorServiceClient::connect(uri.clone()).await {
                Ok(client) => client,
                Err(_) => {
                    // Executor is not reachable
                    continue;
                }
            };
            match client.heartbeat(Empty {}).await {
                Ok(_) => {
                    // Executor is still connected
                    *executor_stats.entry(uri.clone()).or_insert(true) = true;
                }
                Err(_) => {
                    // Executor is not reachable
                    *executor_stats.entry(uri.clone()).or_insert(false) = false;
                }
            };
        }
        {
            let mut executors_guard = self.executors.lock().unwrap();
            (*executors_guard) = executor_stats.clone();
        }
        let response = CheckExecutorsResponse {
            executor_status: executor_stats,
        };
        Ok(Response::new(response))
    }
}

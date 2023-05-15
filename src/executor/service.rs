use crate::common::common::ActionType;
use crate::common::parquet_reader::ParquetReader;
use crate::executor::proto::{
    gin_executor_service_server::GinExecutorService, Empty, LaunchTaskRequest, LaunchTaskResponse,
};
use crate::scheduler::proto::gin_scheduler_service_client::GinSchedulerServiceClient;
use crate::scheduler::proto::{RegisterExecutorRequest, UnregisterExecutorRequest};
use arrow2::io::parquet::read::{RowGroupDeserializer, RowGroupReader};
use futures::executor::block_on;

use log::{debug, info, error};

use tonic::{Request, Response, Status};

use crate::common::common::stage::StageType;
use arrow2::array::PrimitiveArray;
use arrow2::array::{Array, Int64Array};
use arrow2::bitmap::Bitmap;
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;
use arrow2::datatypes::SchemaRef;
use arrow2::datatypes::{Field, Schema};
use arrow2::io::parquet::write::FileMetaData;
use eval::eval;
use eval::Value;
use std::collections::HashSet;
use std::net::SocketAddr;
use arrow2::io::parquet::read;
use std::time::{Duration, Instant};
#[derive(Debug)]
pub struct GinExecutor {
    id: i32,
    address: SocketAddr,
    scheduler_address: SocketAddr,
}

impl GinExecutor {
    pub fn new(id: i32, addr: SocketAddr, scheduler: SocketAddr) -> Self {
        let ob = Self {
            id: id,
            address: addr,
            scheduler_address: scheduler,
        };
        ob._attach_to_scheduler();
        ob
    }

    // fn select(reader , field_names: &[&str]) -> Schema {
    //     // TODO: use filter from Schema implementation

    //     let fields: Vec<Field> = schema
    //         .fields
    //         .iter()
    //         .filter(|field| field_names.contains(&&*field.name))
    //         .cloned()
    //         .collect();
    //     return Schema {
    //         fields,
    //         metadata: schema.metadata.clone(),
    //     };
    // }
    // chunks: read::RowGroupDeserializer,
    fn sum(
        chunk: &Chunk<Box<dyn Array>>,
        schema: &Schema,
        field_name: &str,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        // let chunk = chunks.into_iter().next().unwrap()?;
        let index = schema
            .fields
            .iter()
            .position(|field| field.name == field_name)
            .ok_or("error")?;
        // println!("Index of field '{}' is {}", field_name, index);
        let array = chunk.columns()[index]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("error")?;
        let sum: i64 = array.iter().flatten().sum();
        Ok(sum)
    }

    fn count(
        chunk: &Chunk<Box<dyn Array>>,
        schema: &Schema,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let arrays = chunk.columns();
        if arrays.len() != 0 {
            Ok(arrays[0].len())
        } else {
            Ok(0)
        }
    }

    fn evaluate_filter(
        closure_string: &str,
        chunk_value: i64,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        // println!("eval: {:?}", eval(closure_string).unwrap());
        let tokens: Vec<&str> = closure_string.split_whitespace().collect();
        let result: bool =
            match eval(&format!("{} {} {}", chunk_value, tokens[1], tokens[2])).unwrap() {
                Value::Bool(num) => num.to_owned(),
                _ => todo!(),
            };
        Ok(result)
    }

    fn filter(
        chunk: &Chunk<Box<dyn Array>>,
        closure_string: &str,
    ) -> Result<Chunk<Box<dyn Array>>, Box<dyn std::error::Error>> {
        // let clone = chunk.clone();
        let mut indexes = vec![];

        // chunk.columns()

        for column in chunk.columns().iter() {
            let int_array = match column.as_any().downcast_ref::<Int64Array>() {
                Some(arr) => arr,
                None => continue,
            };
            debug!("New column with len {}", int_array.len());
            // println!("int_array={:#?}", int_array);

            for i in 0..int_array.len() {
                if i % 100000 == 0{
                    debug!("New entry {}", i);
                }
                let value = int_array.value(i);
                let x = value;
                if !(GinExecutor::evaluate_filter(closure_string, value).unwrap()) {
                    indexes.push(i);
                }
            }
        }
        debug!("1");
        let unique_vec: Vec<_> = indexes
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        // println!("indexes={:?}", unique_vec);

        // let columns = chunk.columns();
        debug!("2");
        let mut arrays = vec![];
        for column in chunk.columns().iter() {
            let int_array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            debug!("2.1");
            let filtered = int_array
                .iter()
                .enumerate()
                .filter(|(i, _)| !unique_vec.contains(&i))
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            debug!("2.2");
            let new: PrimitiveArray<i64> = filtered
                .into_iter()
                .map(|x| x.cloned())
                .collect::<Vec<_>>()
                .into();
            let new2 = Box::new(new.clone()) as Box<dyn arrow2::array::Array>;
            debug!("2.3");
            arrays.push(new2);
        }
        debug!("3");

        Ok(Chunk::new(arrays))
    }

    pub fn get_uri(&self) -> String {
        format!("http://{}:{}", self.address.ip(), self.address.port())
    }
    pub fn get_scheduler_uri(&self) -> String {
        format!(
            "http://{}:{}",
            self.scheduler_address.ip(),
            self.scheduler_address.port()
        )
    }

    pub fn _detach_from_scheduler(&self) {
        let sched_client_try =
            block_on(GinSchedulerServiceClient::connect(self.get_scheduler_uri()));
        let sched_client = match sched_client_try {
            Ok(client) => client,
            Err(error) => panic!("Problem connecting to scheduler: {:?}", error),
        };
        debug!("Connected to scheduler");
        let request = UnregisterExecutorRequest {
            executor_uri: self.get_uri(),
        };
        let res = block_on(sched_client.to_owned().unregister_executor(request));
        let _ = match res {
            Ok(_) => {}
            Err(error) => panic!("Problem unregistering scheduler: {:?}", error),
        };

        info!("Unregistered from scheduler");
    }

    pub fn _attach_to_scheduler(&self) {
        let sched_client_try =
            block_on(GinSchedulerServiceClient::connect(self.get_scheduler_uri()));
        let request = RegisterExecutorRequest {
            executor_uri: self.get_uri(),
        };
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
        let mut result:f64 = -1.0;
        info!("Task launched");
        let request = _request.get_ref().clone();
        let s3_conf = request.s3_conf.unwrap();
        let dataset_uri = request.dataset_uri;
        let index = request.partition_index;
        let parquet_reader = ParquetReader::new(&s3_conf, &dataset_uri).await.unwrap();
        let _initial_data = parquet_reader
            .read_row_group_deser(index as usize, None)
            .await
            .unwrap();
        let metadata = match parquet_reader.read_metadata().await {
            Ok(meta) => meta,
            _ => panic!(),
        };
        // for field in metadata.schema().fields() {
        //     debug!("{}", field.name());
        // }

        // let chunk = _initial_data.into_iter().next().unwrap().expect("Failed to load Chunk.");
        // debug!("[executor:{}] Read chunk: [{}] with {} rows.", self.id, index, chunk.len());

        let execution = _request.get_ref().clone();
        let mut step_input = {
            let mut input = Vec::new();
            for elem in _initial_data {
                input.push(elem.unwrap())
            }
            input
        };

        for plan_step in execution.plan {
            info!("Started processing step {}", plan_step.id);
            let step = match plan_step.stage_type {
                Some(stype) => stype,
                None => {
                    return Err(Status::aborted(
                        "Failed launching task on executor. Corrupted state?",
                    ));
                }
            };
            let mut stats_plan_processing_started = Instant::now();
            let mut stats_chunks_processed = 0;
            let mut stats_plan_processing_elapsed: std::time::Duration = Duration::default();
            match step {
                StageType::Filter(_filter) => {
                    let mut _intermediate = Vec::new();
                    for chunk in step_input.into_iter() {
                        debug!("Processing new chunk");
                        match GinExecutor::filter(&chunk, &_filter.predicate) {
                            Ok(res) => {
                                _intermediate.push(res);
                            }
                            Err(_) => continue,
                        }
                        stats_chunks_processed = stats_chunks_processed + 1;
                    }
                    step_input = _intermediate.clone();
                    stats_plan_processing_elapsed = stats_plan_processing_started.elapsed();
                }

                StageType::Select(_columns) => {
                    let mut field_list = Vec::new();
                    for f in _columns.columns {
                        field_list = metadata
                            .schema()
                            .fields()
                            .iter()
                            .filter(|item| item.name() == f)
                            .map(|item| ParquetReader::convert_parquet_to_arrow(item))
                            .collect();
                    }
                    let tmp_data = parquet_reader
                        .read_row_group_deser(index as usize, Some(field_list))
                        .await
                        .unwrap();

                    step_input = {
                        let mut input = Vec::new();
                        
                        for elem in tmp_data {
                            let chunk = elem.unwrap();
                            // let field_len = &chunk.clone().columns().len();
                            // debug!("selected ... and now the input has {} columns",field_len );
                            input.push(chunk)
                        }
                        input
                    };
                }

                StageType::Action(_action) => {
                        let response_stage_type = match _action {
                            0 => ActionType::Sum,
                            1 => ActionType::Count,
                            2 => ActionType::Collect,
                            _ => {
                                error!("Execute task failed: Invalid action type");
                                continue;
                            }
                        };
                        match response_stage_type {
                            ActionType::Sum => {
                                todo!();
                            },
                            ActionType::Count=>{
                                let mut cross_chunk_result_vec = Vec::new();
                                for chunk in step_input.clone().into_iter() {
                                    cross_chunk_result_vec.push(GinExecutor::count(&chunk, &read::infer_schema(&metadata).unwrap()).unwrap());
                                }
                                let mut sum: usize = 0;
                                for elem in cross_chunk_result_vec {
                                    sum += elem;
                                }
                                result = sum as f64;
                                // [TODO]
                                // maybe have multiple returnable types?
                            },
                            ActionType::Collect =>{
                                // [TODO]
                                // maybe discuss the format of this?
                                todo!()
                            }
                        }
                }
                
            }
            // debug!(
            //     "{} chunks processed in {:.2?} seconds",
            //     stats_chunks_processed, stats_plan_processing_elapsed
            // );
        }

        // demo answer
        let demo_result: f64 = result;
        let demo_response = LaunchTaskResponse {
            executor_id: 0,
            success: true,
            result: serde_cbor::to_vec(&demo_result).unwrap(),
        };
        Ok(Response::new(demo_response))
        // Ok(Response::new(Empty {}))
    }
}

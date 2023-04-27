pub mod executor;
pub mod scheduler;

use scheduler::proto::Stage;

use std::collections::{HashMap, VecDeque};

pub struct Application {
    jobs: Vec<Job>,
}

pub struct Job {
    stages: Vec<Stage>,
}

// Define a struct to represent a task
struct Task {
    id: String,
    input_data: Vec<i64>,
}



// struct JobExecutor {
//     job: Job,
//     completed: HashMap<String, ()>,
//     ready: VecDeque<String>,
// }

// impl JobExecutor {
//     fn new(job: Job) -> Self {
//         let mut ready = VecDeque::new();
//         let mut completed = HashMap::new();
//         for stage in &job.stages {
//             if stage.dependencies.is_empty() {
//                 ready.push_back(stage.name.clone());
//             }
//         }
//         Self {
//             job,
//             completed,
//             ready,
//         }
//     }

//     async fn execute(&mut self) {
//         while let Some(name) = self.ready.pop_front() {
//             let stage = self.job.stages.iter().find(|s| s.name == name).unwrap();
//             let dependencies_completed = stage
//                 .dependencies
//                 .iter()
//                 .all(|name| self.completed.contains_key(name));
//             if dependencies_completed {
//                 (stage.task)();
//                 self.completed.insert(name.clone(), ());
//                 for stage in &self.job.stages {
//                     if !self.completed.contains_key(&stage.name)
//                         && stage
//                             .dependencies
//                             .iter()
//                             .all(|name| self.completed.contains_key(name))
//                     {
//                         self.ready.push_back(stage.name.clone());
//                     }
//                 }
//             } else {
//                 self.ready.push_back(name);
//             }
//         }
//     }
// }
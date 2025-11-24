use crate::executor::DedicatedExecutor;
use crate::io::register_io_runtime;
use log::info;
use std::sync::Arc;
use datafusion::error::DataFusionError;
use tokio::runtime::{Builder, Runtime};

pub struct RuntimeManager {
    pub io_runtime: Arc<Runtime>,
    cpu_executor: DedicatedExecutor,
}

impl RuntimeManager {
    pub fn new(cpu_threads: usize) -> Self {
        println!("Creating RuntimeManager with {} CPU threads", cpu_threads);

        // IO Runtime
        let io_runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(cpu_threads)
                .thread_name("datafusion-io")
                .enable_all()
                .build()
                .expect("Failed to create IO runtime"),
        );

        // Register IO runtime for current thread
        register_io_runtime(Some(io_runtime.handle().clone()));

        // CPU Executor with its own runtime
        let mut cpu_runtime_builder = Builder::new_multi_thread();
        let io_handle = io_runtime.handle().clone();

        cpu_runtime_builder
            .worker_threads(cpu_threads) // TODO : Decide on CPU threads vs 1.5 * CPU threads
            //.worker_threads(((cpu_threads as f64) * 1.5 + 1.0) as usize)
            .thread_name("datafusion-cpu")
            .enable_time()
            .on_thread_start(move || {
                // Register IO runtime for each CPU thread
                register_io_runtime(Some(io_handle.clone()));
            });

        let cpu_executor = DedicatedExecutor::new("datafusion-cpu", cpu_runtime_builder);

        Self {
            io_runtime,
            cpu_executor,
        }
    }

    pub fn cpu_executor(&self) -> DedicatedExecutor {
        self.cpu_executor.clone()
    }
    pub async fn run<Fut, T>(&self, fut: Fut) -> Result<T, DataFusionError>
    where
        Fut: std::future::Future<Output = Result<T, DataFusionError>> + Send + 'static,
        T: Send + 'static,
    {
        Self::run_inner(self.cpu_executor.clone(), fut).await
    }

    async fn run_inner<Fut, T>(exec: DedicatedExecutor, fut: Fut) -> Result<T, DataFusionError>
    where
        Fut: std::future::Future<Output = Result<T, DataFusionError>> + Send + 'static,
        T: Send + 'static,
    {
        exec.spawn(fut).await.unwrap_or_else(|e| {
            Err(DataFusionError::Context(
                format!("Join Error: {:?}", e),
                Box::new(DataFusionError::Internal("Task execution failed".to_string())),
            ))
        })
    }
}

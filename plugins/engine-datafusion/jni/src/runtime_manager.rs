use crate::executor::DedicatedExecutor;
use crate::io::register_io_runtime;
use opensearch_vectorized_spi::rust_log_info;
use log::info;
use std::sync::Arc;
use datafusion::error::DataFusionError;
use tokio::runtime::{Builder, Runtime};

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub cpu_threads: usize,
    pub io_threads: usize,
    pub cpu_thread_multiplier: Option<f64>,
    pub io_thread_multiplier: Option<f64>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let cpu_count = num_cpus::get();
        Self {
            cpu_threads: cpu_count,
            io_threads: cpu_count,
            cpu_thread_multiplier: None,
            io_thread_multiplier: None,
        }
    }
}

impl RuntimeConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cpu_threads(mut self, threads: usize) -> Self {
        self.cpu_threads = threads;
        self
    }

    pub fn with_io_threads(mut self, threads: usize) -> Self {
        self.io_threads = threads;
        self
    }

    /// Use multiplier for CPU threads (e.g., 1.5x for CPU-bound work)
    pub fn with_cpu_multiplier(mut self, multiplier: f64) -> Self {
        self.cpu_thread_multiplier = Some(multiplier);
        self
    }

    /// Use multiplier for IO threads (e.g., 0.5x for IO-bound work)
    pub fn with_io_multiplier(mut self, multiplier: f64) -> Self {
        self.io_thread_multiplier = Some(multiplier);
        self
    }

    fn effective_cpu_threads(&self) -> usize {
        if let Some(multiplier) = self.cpu_thread_multiplier {
            ((self.cpu_threads as f64 * multiplier) + 1.0) as usize
        } else {
            self.cpu_threads
        }
    }

    fn effective_io_threads(&self) -> usize {
        if let Some(multiplier) = self.io_thread_multiplier {
            ((self.io_threads as f64 * multiplier) + 1.0) as usize
        } else {
            self.io_threads
        }
    }
}

pub struct RuntimeManager {
    pub io_runtime: Arc<Runtime>,
    pub(crate) cpu_executor: DedicatedExecutor,
}

impl RuntimeManager {
    pub fn new(cpu_threads: usize) -> Self {
        Self::with_config(RuntimeConfig::new()
            .with_cpu_threads(cpu_threads)
            .with_io_threads(cpu_threads)
            .with_cpu_multiplier(1.0)
            .with_io_multiplier(2.0)
        )
    }

    pub fn with_config(config: RuntimeConfig) -> Self {
        rust_log_info!("Creating RuntimeManager with config: {:?}", config);

        // IO Runtime
        let io_runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(config.effective_io_threads())
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
            .worker_threads(config.effective_cpu_threads())
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

    pub fn shutdown(&self) {
        info!("Shutting down RuntimeManager");
        self.cpu_executor.join_blocking();
         // TODO: io_runtime spawned threads seem to have issue and are leaking

    }
}

impl Drop for RuntimeManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

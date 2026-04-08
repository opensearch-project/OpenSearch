use crate::executor::DedicatedExecutor;
use crate::io::register_io_runtime;
use vectorized_exec_spi::log_info;
use crate::metrics_collector::MetricsCollector;
use log::info;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use datafusion::error::DataFusionError;
use tokio::runtime::{Builder, Handle, Runtime};

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
    /// Cloneable handle to the IO runtime, used by all JNI entry points to submit
    /// tasks without taking ownership of the runtime.
    pub io_runtime: Handle,
    /// Owned runtime kept behind a Mutex so shutdown() can take() it and call
    /// shutdown_timeout(), which blocks until all IO threads have fully stopped.
    io_runtime_owned: Mutex<Option<Runtime>>,
    pub(crate) cpu_executor: DedicatedExecutor,
    pub io_metrics: Arc<MetricsCollector>,
    pub cpu_metrics: Option<Arc<MetricsCollector>>,
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
        log_info!("Creating RuntimeManager with config: {:?}", config);

        // IO Runtime — build first so we can extract a Handle for sharing
        let io_runtime_rt = Builder::new_multi_thread()
            .worker_threads(config.effective_io_threads())
            .thread_name("datafusion-io")
            .enable_all()
            .build()
            .expect("Failed to create IO runtime");

        let io_handle = io_runtime_rt.handle().clone();

        // Register IO runtime for the calling (JNI) thread.
        register_io_runtime(Some(io_handle.clone()));

        // CPU Executor with its own runtime
        let mut cpu_runtime_builder = Builder::new_multi_thread();
        let io_handle_for_cpu = io_handle.clone();

        cpu_runtime_builder
            .worker_threads(config.effective_cpu_threads())
            .thread_name("datafusion-cpu")
            .enable_all()
            .on_thread_start(move || {
                // Register IO runtime for each CPU thread
                register_io_runtime(Some(io_handle_for_cpu.clone()));
            });

        let cpu_executor = DedicatedExecutor::new("datafusion-cpu", cpu_runtime_builder);

        // Create MetricsCollector for IO runtime
        let io_metrics = Arc::new(MetricsCollector::new(io_runtime_rt.handle()));

        // Create MetricsCollector for CPU runtime (if handle is available)
        let cpu_metrics = cpu_executor
            .handle()
            .map(|handle| Arc::new(MetricsCollector::new(&handle)));

        Self {
            io_runtime: io_handle,
            io_runtime_owned: Mutex::new(Some(io_runtime_rt)),
            cpu_executor,
            io_metrics,
            cpu_metrics,
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
        // Shut down CPU executor first — waits for all in-flight CPU tasks to finish.
        self.cpu_executor.join_blocking();
        // Take the owned IO runtime out of the Option and call shutdown_timeout, which
        // blocks until every datafusion-io-* thread has fully terminated.
        if let Some(rt) = self.io_runtime_owned.lock().unwrap().take() {
            rt.shutdown_timeout(Duration::from_secs(10));
        }
        info!("RuntimeManager shut down complete");
    }
}

impl Drop for RuntimeManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 */

use tokio::runtime::Runtime;
use std::future::Future;

/// Manages the Tokio runtime for async operations
pub struct RuntimeManager {
    runtime: Runtime,
}

impl RuntimeManager {
    pub fn new() -> Self {
        // Placeholder

        let runtime = Runtime::new().expect("Failed to create Tokio runtime");
        Self { runtime }
    }
    
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.runtime.block_on(future)
    }
}

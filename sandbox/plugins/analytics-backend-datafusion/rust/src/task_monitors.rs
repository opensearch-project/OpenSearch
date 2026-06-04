/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-operation task monitors.
//!
//! Each FFM operation type gets its own [`TaskMonitor`] for timing metrics:
//! - `coordinator_reduce`: coordinator-side local plan execution
//! - `query_execution`: shard-level query execution
//! - `stream_next`: streaming pagination
//! - `plan_setup`: session context creation + plan preparation
//!
//! Monitors are initialized lazily on first access via `once_cell::sync::Lazy`.

use once_cell::sync::Lazy;
use tokio_metrics::TaskMonitor;

static COORDINATOR_REDUCE_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static QUERY_EXECUTION_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static STREAM_NEXT_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static PLAN_SETUP_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);

pub fn coordinator_reduce_monitor() -> &'static TaskMonitor { &COORDINATOR_REDUCE_MONITOR }
pub fn query_execution_monitor() -> &'static TaskMonitor { &QUERY_EXECUTION_MONITOR }
pub fn stream_next_monitor() -> &'static TaskMonitor { &STREAM_NEXT_MONITOR }
pub fn plan_setup_monitor() -> &'static TaskMonitor { &PLAN_SETUP_MONITOR }

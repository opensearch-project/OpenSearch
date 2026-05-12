/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-operation task monitors.
//!
//! Each FFM operation type (`query_execution`, `stream_next`, `fetch_phase`)
//! gets its own [`TaskMonitor`] for timing metrics.
//!
//! Monitors are initialized lazily on first access via `once_cell::sync::Lazy`.

use once_cell::sync::Lazy;
use tokio_metrics::TaskMonitor;

static QUERY_EXECUTION_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static STREAM_NEXT_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static FETCH_PHASE_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static CREATE_CONTEXT_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static PREPARE_PARTIAL_PLAN_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static PREPARE_FINAL_PLAN_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static SQL_TO_SUBSTRAIT_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);

pub fn query_execution_monitor() -> &'static TaskMonitor { &QUERY_EXECUTION_MONITOR }
pub fn stream_next_monitor() -> &'static TaskMonitor { &STREAM_NEXT_MONITOR }
pub fn fetch_phase_monitor() -> &'static TaskMonitor { &FETCH_PHASE_MONITOR }
pub fn create_context_monitor() -> &'static TaskMonitor { &CREATE_CONTEXT_MONITOR }
pub fn prepare_partial_plan_monitor() -> &'static TaskMonitor { &PREPARE_PARTIAL_PLAN_MONITOR }
pub fn prepare_final_plan_monitor() -> &'static TaskMonitor { &PREPARE_FINAL_PLAN_MONITOR }
pub fn sql_to_substrait_monitor() -> &'static TaskMonitor { &SQL_TO_SUBSTRAIT_MONITOR }

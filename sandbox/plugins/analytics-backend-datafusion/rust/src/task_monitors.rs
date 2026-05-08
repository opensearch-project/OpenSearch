/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-operation task monitors.
//!
//! Each FFM operation type (`query_execution`, `stream_next`, `fetch_phase`,
//! `segment_stats`) gets its own [`TaskMonitor`] for timing metrics.
//!
//! Monitors are initialized lazily on first access via `once_cell::sync::Lazy`.

use once_cell::sync::Lazy;
use tokio_metrics::TaskMonitor;

static QUERY_EXECUTION_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static STREAM_NEXT_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static FETCH_PHASE_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);
static SEGMENT_STATS_MONITOR: Lazy<TaskMonitor> = Lazy::new(TaskMonitor::new);

pub fn query_execution_monitor() -> &'static TaskMonitor { &QUERY_EXECUTION_MONITOR }
pub fn stream_next_monitor() -> &'static TaskMonitor { &STREAM_NEXT_MONITOR }
pub fn fetch_phase_monitor() -> &'static TaskMonitor { &FETCH_PHASE_MONITOR }
pub fn segment_stats_monitor() -> &'static TaskMonitor { &SEGMENT_STATS_MONITOR }

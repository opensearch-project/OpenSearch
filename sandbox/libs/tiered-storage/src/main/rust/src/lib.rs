/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared tiered storage infrastructure for OpenSearch sandbox.
//!
//! This crate provides format-agnostic components for routing file I/O between
//! local and remote object stores. Any data format (parquet, CSV, Lucene) can
//! use the same [`TieredStorageRegistry`] and remote store backends.
//!
//! # Thread Safety
//!
//! All public types are `Send + Sync`. Concurrent access is handled via
//! [`DashMap`](dashmap::DashMap) sharded locking and atomic operations —
//! no external synchronization is required.
//!
//! # Crate Architecture
//!
//! - [`types`] — `TieredFileEntry` (with acquire/release), `FileLocation`, `ReadGuard`, errors
//! - [`registry`] — `FileRegistry` trait + `TieredStorageRegistry` concrete impl
//! - [`tiered_object_store`] — `TieredObjectStore` routes reads between local and remote stores
//! - [`ffm`] — `extern "C"` FFM bridge for Java ↔ Rust interop
//!
//! Cloud backends (S3, GCS, Azure, FS) live in separate plugin crates
//! under `sandbox/plugins/repository-*/`.

pub mod ffm;
pub mod registry;
pub mod tiered_object_store;
pub mod types;

pub use native_bridge_common::{log_debug, log_error, log_info};

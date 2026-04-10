/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Remote object store backends for OpenSearch sandbox.
//!
//! Provides factory-created [`ObjectStore`](object_store::ObjectStore) backends
//! for S3, GCS, Azure, and local filesystem. Each backend is wrapped in
//! [`RemoteObjectStore`] for logging and error normalisation.
//!
//! # Architecture
//!
//! Mirrors the Java `BlobStore` pattern:
//! - [`remote_object_store`] — logging/error-normalizing wrapper
//! - [`fs`] — local filesystem backend config + builder
//! - [`s3`] — Amazon S3 backend config + builder
//! - [`gcs`] — Google Cloud Storage backend config + builder
//! - [`azure`] — Azure Blob Storage backend config + builder
//! - [`factory`] — creates the appropriate backend from type + config JSON
//!
//! # Usage
//!
//! ```ignore
//! use opensearch_remote_store::StoreFactory;
//!
//! // Simple:
//! let store = StoreFactory::new("s3", config_json, "repo-1").build()?;
//!
//! // With custom S3 credentials:
//! let store = StoreFactory::new("s3", config_json, "repo-1")
//!     .with_s3_credentials(provider)
//!     .build()?;
//! ```

pub mod backends;
pub mod factory;
pub mod remote_object_store;

pub use factory::create;
pub use factory::StoreFactory;
pub use factory::StoreFactoryError;
pub use remote_object_store::RemoteObjectStore;

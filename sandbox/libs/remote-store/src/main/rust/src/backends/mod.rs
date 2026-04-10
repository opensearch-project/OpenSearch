/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Remote object store backend implementations.
//!
//! Each backend provides a `build(config_json) -> Result<Arc<dyn ObjectStore>>`
//! function. To add a new backend, create a new module here and add a match
//! arm in [`StoreFactory::build`](crate::factory::StoreFactory::build).

pub mod azure;
pub mod fs;
pub mod gcs;
pub mod s3;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Core file registry trait and concrete tiered implementation.

pub mod tiered_registry;
pub mod traits;

pub use tiered_registry::TieredStorageRegistry;
pub use traits::FileRegistry;

// Re-export error type from crate::types for convenience.
pub use crate::types::FileRegistryError;

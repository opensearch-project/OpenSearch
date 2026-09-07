// SPDX-License-Identifier: Apache-2.0
// Vendored in-memory subset of liquid-cache core. See ../../README.md for provenance.

//! In-memory liquid cache: cache storage (index, budget, eviction policies,
//! transcode) plus the numeric LiquidArray encodings.

pub mod cache;
pub mod liquid_array;
mod sync;
pub mod utils;

pub use cache::cache_policies;

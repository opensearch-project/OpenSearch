/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

// ═══════════════════════════════════════════════════════════════════════════════
// Single cdylib for JDK FFM (Foreign Function & Memory API).
//
// This crate:
//   1. Sets the global jemalloc allocator (shared across all plugin rlibs)
//   2. Pulls in plugin rlibs via extern crate (forces linker to include symbols)
//   3. All #[no_mangle] extern "C" functions from the plugin crates are
//      automatically available for dlsym/SymbolLookup
// ═══════════════════════════════════════════════════════════════════════════════

/// jemalloc tuning: reduce dirty page decay (30s), muzzy decay (30s), and tcache max (64KB).
#[export_name = "malloc_conf"]
pub static MALLOC_CONF: &[u8] = b"dirty_decay_ms:30000,muzzy_decay_ms:30000,lg_tcache_max:16\0";

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Pull in plugin rlibs — forces linker to include all #[no_mangle] symbols.
extern crate native_bridge_common;
extern crate opensearch_datafusion;
extern crate opensearch_parquet_format;
extern crate opensearch_repository_s3;
extern crate opensearch_repository_gcs;
extern crate opensearch_repository_azure;
extern crate opensearch_repository_fs;
extern crate opensearch_tiered_storage;

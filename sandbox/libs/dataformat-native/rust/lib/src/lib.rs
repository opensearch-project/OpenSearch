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
// Unlike the JNI approach (RegisterNatives, classloader workarounds), FFM calls
// extern "C" functions directly via SymbolLookup + Linker.downcallHandle().
// No JNIEnv, no JClass, no classloader binding — just plain C ABI.
//
// This crate:
//   1. Sets the global mimalloc allocator (shared across all plugin rlibs)
//   2. Pulls in plugin rlibs via extern crate (forces linker to include symbols)
//   3. All #[no_mangle] extern "C" functions from the plugin crates are
//      automatically available for dlsym/SymbolLookup
// ═══════════════════════════════════════════════════════════════════════════════

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Pull in plugin rlibs — forces linker to include all #[no_mangle] symbols.
extern crate native_bridge_common;
extern crate opensearch_datafusion;
extern crate opensearch_parquet_format;
extern crate opensearch_repository_s3;
extern crate opensearch_repository_gcs;
extern crate opensearch_repository_azure;
extern crate opensearch_repository_fs;
extern crate opensearch_tiered_storage;

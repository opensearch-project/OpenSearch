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
//   1. Sets the global allocator to HeapRoutingAllocator (routes allocations
//      to the active plugin heap via thread-local, without mi_heap_set_default)
//   2. Pulls in plugin rlibs via extern crate (forces linker to include symbols)
//   3. All #[no_mangle] extern "C" functions from the plugin crates are
//      automatically available for dlsym/SymbolLookup
// ═══════════════════════════════════════════════════════════════════════════════

#[global_allocator]
static GLOBAL: native_bridge_common::heap_allocator::HeapRoutingAllocator =
    native_bridge_common::heap_allocator::HeapRoutingAllocator;

// On Linux, when this cdylib is loaded via dlopen (Java FFM), mimalloc's
// internal state (_mi_subproc) may not be initialized before the first
// allocation. Call mi_process_init() in a library constructor to ensure
// mimalloc is ready before Rust's runtime triggers any allocations.
#[cfg(target_os = "linux")]
#[unsafe(link_section = ".init_array")]
static INIT: unsafe extern "C" fn() = {
    unsafe extern "C" fn init() {
        extern "C" {
            fn mi_process_init();
        }
        unsafe { mi_process_init() };
    }
    init
};

// Pull in plugin rlibs — forces linker to include all #[no_mangle] symbols.
extern crate native_bridge_common;
extern crate opensearch_datafusion;
extern crate opensearch_parquet_format;
extern crate opensearch_repository_s3;
extern crate opensearch_repository_gcs;
extern crate opensearch_repository_azure;
extern crate opensearch_repository_fs;
extern crate opensearch_tiered_storage;

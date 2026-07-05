/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.analytics.spi.QueryExecutionMetrics;
import org.opensearch.be.datafusion.NativeErrorConverter;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.core.action.ActionListener;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;
import org.opensearch.plugin.stats.AnalyticsBackendTaskCancellationStats;
import org.opensearch.plugins.NativeStoreHandle;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * FFM bridge to native DataFusion library.
 *
 * <h2>Pointer lifecycle (no Arena needed)</h2>
 * <p>Native pointers returned by {@code createGlobalRuntime}, {@code createDatafusionReader},
 * and {@code executeQueryAsync} are opaque {@code long} values — Rust heap addresses cast to
 * {@code i64}. They are <b>not</b> {@code MemorySegment}s and do not require an Arena. They
 * live until explicitly freed by the corresponding close method.</p>
 *
 * <h2>Arena usage</h2>
 * <p>{@link NativeCall} creates a confined Arena for short-lived allocations (strings, byte
 * arrays) that are only needed for the duration of the FFM call. The Arena is closed
 * immediately after the call returns, freeing all temp memory.</p>
 *
 * <h2>Error convention</h2>
 * <p>Functions return {@code i64}: {@code >= 0} is success, {@code < 0} is a negated pointer
 * to a heap-allocated error string. {@link NativeCall#invoke} reads and frees the error,
 * then throws.</p>
 */
public final class NativeBridge {

    private static final Logger logger = LogManager.getLogger(NativeBridge.class);

    /**
     * Converts a Throwable from the FFM boundary into an appropriate Exception, applying
     * {@link NativeErrorConverter} to translate known native error patterns into OpenSearch
     * exception types (e.g. CircuitBreakingException, OpenSearchStatusException).
     */
    private static Exception convertNativeError(Throwable t) {
        Exception ex = t instanceof Exception e ? e : new RuntimeException(t);
        return NativeErrorConverter.convert(ex);
    }

    /**
     * For synchronous FFM methods: converts and rethrows as RuntimeException.
     * NativeErrorConverter only returns RuntimeException subtypes when it matches,
     * so this preserves the unchecked throw semantics of the original call site.
     */
    private static RuntimeException rethrowConverted(RuntimeException e) {
        Exception converted = NativeErrorConverter.convert(e);
        if (converted instanceof RuntimeException rte) {
            return rte;
        }
        return e;
    }

    private static final MethodHandle INIT_RUNTIME_MANAGER;
    private static final MethodHandle SHUTDOWN_RUNTIME_MANAGER;
    private static final MethodHandle CREATE_GLOBAL_RUNTIME;
    private static final MethodHandle CLOSE_GLOBAL_RUNTIME;
    private static final MethodHandle GET_MEMORY_POOL_USAGE;
    private static final MethodHandle GET_MEMORY_POOL_LIMIT;
    private static final MethodHandle GET_MEMORY_POOL_STATS;
    private static final MethodHandle SET_MEMORY_POOL_LIMIT;
    /**
     * Forward-compat probe for runtime spill-cap updates. Today the upstream
     * DataFusion {@code DiskManager.max_temp_directory_size} is a plain {@code u64}
     * that cannot be safely mutated through {@code Arc<DiskManager>} once the
     * runtime is in use, so we do not ship this symbol from our Rust crate.
     * When the upstream PR converting that field to {@code Arc<AtomicU64>}
     * lands, our crate exports {@code df_set_spill_limit} and this handle
     * becomes non-null automatically — the same Java JAR works against both
     * versions of the native library.
     */
    private static final MethodHandle SET_SPILL_LIMIT;
    private static final MethodHandle SET_MIN_TARGET_PARTITIONS;
    private static final MethodHandle SET_REDUCE_TARGET_PARTITIONS;
    private static final MethodHandle SET_SPILL_EXEMPT_CAP_BYTES;
    private static final MethodHandle SET_MEMORY_GUARD_THRESHOLDS;
    private static final MethodHandle CREATE_READER;
    private static final MethodHandle CLOSE_READER;
    private static final MethodHandle EXECUTE_QUERY;
    private static final MethodHandle STREAM_GET_SCHEMA;
    private static final MethodHandle STREAM_NEXT;
    private static final MethodHandle STREAM_CLOSE;
    private static final MethodHandle STREAM_GET_METRICS;
    private static final MethodHandle FREE_METRICS_BUF;
    private static final MethodHandle SQL_TO_SUBSTRAIT;
    private static final MethodHandle REGISTER_FILTER_TREE_CALLBACKS;
    private static final MethodHandle CREATE_LOCAL_SESSION;
    private static final MethodHandle CLOSE_LOCAL_SESSION;
    private static final MethodHandle REGISTER_PARTITION_STREAM;
    private static final MethodHandle EXECUTE_LOCAL_PLAN;
    private static final MethodHandle SENDER_SEND;
    private static final MethodHandle SENDER_CLOSE;
    private static final MethodHandle REGISTER_MEMTABLE;
    private static final MethodHandle CREATE_CUSTOM_CACHE_MANAGER;
    private static final MethodHandle DESTROY_CUSTOM_CACHE_MANAGER;
    private static final MethodHandle CREATE_CACHE;
    private static final MethodHandle CACHE_MANAGER_ADD_FILES;
    private static final MethodHandle CACHE_MANAGER_ADD_FILES_WITH_STORE;
    private static final MethodHandle CACHE_MANAGER_REMOVE_FILES;
    private static final MethodHandle CACHE_MANAGER_CLEAR;
    private static final MethodHandle CACHE_MANAGER_CLEAR_BY_TYPE;
    private static final MethodHandle CACHE_MANAGER_GET_MEMORY_BY_TYPE;
    private static final MethodHandle CACHE_MANAGER_GET_TOTAL_MEMORY;
    private static final MethodHandle CACHE_MANAGER_CONTAINS_BY_TYPE;
    private static final MethodHandle CACHE_MANAGER_UPDATE_SIZE_LIMIT;
    private static final MethodHandle CREATE_SESSION_CONTEXT;
    private static final MethodHandle CREATE_SESSION_CONTEXT_INDEXED;
    private static final MethodHandle CLOSE_SESSION_CONTEXT;
    private static final MethodHandle EXECUTE_WITH_CONTEXT;
    private static final MethodHandle SET_COLUMN_INDEX_CACHE_LIMIT;
    private static final MethodHandle SET_OFFSET_INDEX_CACHE_LIMIT;
    private static final MethodHandle CLEAR_SCOPED_PAGE_INDEX_CACHE;
    private static final MethodHandle SET_SCOPED_PAGE_INDEX_ENABLED;
    private static final MethodHandle CANCEL_QUERY;
    private static final MethodHandle SET_CANCEL_STATS_THRESHOLD_MS;
    private static final MethodHandle STATS;
    private static final MethodHandle QUERY_REGISTRY_TOP_N_BY_CURRENT;
    private static final MethodHandle DF_NATIVE_NODE_STATS;
    private static final MethodHandle PREPARE_PARTIAL_PLAN;
    private static final MethodHandle PREPARE_FINAL_PLAN;
    private static final MethodHandle EXECUTE_LOCAL_PREPARED_PLAN;
    private static final MethodHandle FETCH_BY_ROW_IDS;
    private static final MethodHandle UPDATE_CONCURRENCY_GATE;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        INIT_RUNTIME_MANAGER = linker.downcallHandle(
            lib.find("df_init_runtime_manager").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT, ValueLayout.JAVA_DOUBLE, ValueLayout.JAVA_DOUBLE)
        );

        SHUTDOWN_RUNTIME_MANAGER = linker.downcallHandle(
            lib.find("df_shutdown_runtime_manager").orElseThrow(),
            FunctionDescriptor.ofVoid()
        );

        CREATE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_create_global_runtime").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        CLOSE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_close_global_runtime").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        GET_MEMORY_POOL_USAGE = linker.downcallHandle(
            lib.find("df_get_memory_pool_usage").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        GET_MEMORY_POOL_LIMIT = linker.downcallHandle(
            lib.find("df_get_memory_pool_limit").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        GET_MEMORY_POOL_STATS = linker.downcallHandle(
            lib.find("df_get_memory_pool_stats").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS)
        );

        SET_MEMORY_POOL_LIMIT = linker.downcallHandle(
            lib.find("df_set_memory_pool_limit").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        // Optional spill-limit setter. Not yet shipped by our Rust crate (upstream
        // DataFusion 53.1.0 does not support runtime spill resize through the public
        // Arc<DiskManager> API). Bind only if the symbol is present so the same
        // Java JAR is forward-compatible with a future native library that ships it.
        SET_SPILL_LIMIT = lib.find("df_set_spill_limit")
            .map(
                addr -> linker.downcallHandle(
                    addr,
                    FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
                )
            )
            .orElse(null);

        SET_MIN_TARGET_PARTITIONS = linker.downcallHandle(
            lib.find("df_set_min_target_partitions").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        SET_REDUCE_TARGET_PARTITIONS = linker.downcallHandle(
            lib.find("df_set_reduce_target_partitions").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        SET_SPILL_EXEMPT_CAP_BYTES = linker.downcallHandle(
            lib.find("df_set_spill_exempt_cap_bytes").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        SET_MEMORY_GUARD_THRESHOLDS = linker.downcallHandle(
            lib.find("df_set_memory_guard_thresholds").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        CREATE_READER = linker.downcallHandle(
            lib.find("df_create_reader").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,    // table_path_ptr
                ValueLayout.JAVA_LONG,  // table_path_len
                ValueLayout.ADDRESS,    // files_ptr
                ValueLayout.ADDRESS,    // files_len_ptr
                ValueLayout.ADDRESS,    // writer_generations_ptr
                ValueLayout.JAVA_LONG,  // count (applies to filenames + writer_generations)
                ValueLayout.JAVA_LONG,  // object store ptr
                ValueLayout.ADDRESS,    // sort_fields_ptr (parallel String[] for index.sort.field)
                ValueLayout.ADDRESS,    // sort_fields_len_ptr
                ValueLayout.ADDRESS,    // sort_orders_ptr (parallel String[] for index.sort.order: "asc"|"desc")
                ValueLayout.ADDRESS,    // sort_orders_len_ptr
                ValueLayout.JAVA_LONG   // sort_count (0 when index has no sort)
            )
        );

        CLOSE_READER = linker.downcallHandle(lib.find("df_close_reader").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

        EXECUTE_QUERY = linker.downcallHandle(
            lib.find("df_execute_query").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,   // returns stream_ptr
                ValueLayout.JAVA_LONG,   // shard_view_ptr
                ValueLayout.ADDRESS,     // table_name_ptr
                ValueLayout.JAVA_LONG,   // table_name_len
                ValueLayout.ADDRESS,     // plan_ptr
                ValueLayout.JAVA_LONG,   // plan_len
                ValueLayout.JAVA_LONG,   // runtime_ptr
                ValueLayout.JAVA_LONG,   // context_id
                ValueLayout.JAVA_LONG,   // query_config_ptr
                ValueLayout.JAVA_LONG,   // internal_search_mode
                ValueLayout.JAVA_LONG    // internal_search_bound
            )
        );

        STREAM_GET_SCHEMA = linker.downcallHandle(
            lib.find("df_stream_get_schema").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        STREAM_NEXT = linker.downcallHandle(
            lib.find("df_stream_next").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        STREAM_CLOSE = linker.downcallHandle(lib.find("df_stream_close").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

        STREAM_GET_METRICS = linker.downcallHandle(
            lib.find("df_stream_get_metrics").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
        );

        FREE_METRICS_BUF = linker.downcallHandle(
            lib.find("df_free_metrics_buf").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        // i64 df_sql_to_substrait(shard_ptr, table_ptr, table_len, sql_ptr, sql_len, runtime_ptr, out_ptr, out_cap, out_len)
        SQL_TO_SUBSTRAIT = linker.downcallHandle(
            lib.find("df_sql_to_substrait").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );

        // ── Coordinator-reduce bindings ──
        // i64 df_create_local_session(runtime_ptr)
        CREATE_LOCAL_SESSION = linker.downcallHandle(
            lib.find("df_create_local_session").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        // void df_close_local_session(session_ptr)
        CLOSE_LOCAL_SESSION = linker.downcallHandle(
            lib.find("df_close_local_session").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        // i64 df_register_partition_stream(session_ptr, input_id_ptr, input_id_len,
        // partial_plan_ptr, partial_plan_len,
        // out_ptr, out_cap, out_len)
        REGISTER_PARTITION_STREAM = linker.downcallHandle(
            lib.find("df_register_partition_stream").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );

        // i64 df_execute_local_plan(session_ptr, substrait_ptr, substrait_len, context_id)
        EXECUTE_LOCAL_PLAN = linker.downcallHandle(
            lib.find("df_execute_local_plan").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        // i64 df_sender_send(sender_ptr, array_ptr, schema_ptr)
        SENDER_SEND = linker.downcallHandle(
            lib.find("df_sender_send").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        // void df_sender_close(sender_ptr)
        SENDER_CLOSE = linker.downcallHandle(lib.find("df_sender_close").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

        // i64 df_register_memtable(session_ptr, input_id_ptr, input_id_len,
        // partial_plan_ptr, partial_plan_len,
        // array_ptrs, schema_ptrs, n_batches,
        // out_ptr, out_cap, out_len)
        REGISTER_MEMTABLE = linker.downcallHandle(
            lib.find("df_register_memtable").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );

        // void df_register_filter_tree_callbacks(createCollector, collectDocs, releaseCollector)
        REGISTER_FILTER_TREE_CALLBACKS = linker.downcallHandle(
            lib.find("df_register_filter_tree_callbacks").orElseThrow(),
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS
            )
        );

        CREATE_CUSTOM_CACHE_MANAGER = linker.downcallHandle(
            lib.find("df_create_custom_cache_manager").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG)
        );

        DESTROY_CUSTOM_CACHE_MANAGER = linker.downcallHandle(
            lib.find("df_destroy_custom_cache_manager").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        // i64 df_create_cache(mgr_ptr, type_ptr, type_len, size_limit, eviction_ptr, eviction_len)
        CREATE_CACHE = linker.downcallHandle(
            lib.find("df_create_cache").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );

        // ── SessionContext decomposition bindings ──
        CREATE_SESSION_CONTEXT = linker.downcallHandle(
            lib.find("df_create_session_context").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_BYTE,   // hasPartialAggregate (0/1)
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );

        CREATE_SESSION_CONTEXT_INDEXED = linker.downcallHandle(
            lib.find("df_create_session_context_indexed").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_BYTE,   // requestsRowIds (0/1) — QTF query phase signal
                ValueLayout.JAVA_BYTE,   // hasPartialAggregate (0/1)
                ValueLayout.JAVA_LONG,   // queryConfigPtr
                ValueLayout.ADDRESS,     // planBytes (multi-index schema widening)
                ValueLayout.JAVA_LONG    // planLen
            )
        );

        // i64 df_cache_manager_add_files(runtime_ptr, files_ptr, files_len_ptr, files_count)
        CACHE_MANAGER_ADD_FILES = linker.downcallHandle(
            lib.find("df_cache_manager_add_files").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );

        CACHE_MANAGER_ADD_FILES_WITH_STORE = linker.downcallHandle(
            lib.find("df_cache_manager_add_files_with_store").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,  // runtime_ptr
                ValueLayout.JAVA_LONG,  // store_ptr
                ValueLayout.ADDRESS,    // files_ptr
                ValueLayout.ADDRESS,    // files_len_ptr
                ValueLayout.JAVA_LONG   // files_count
            )
        );

        CACHE_MANAGER_REMOVE_FILES = linker.downcallHandle(
            lib.find("df_cache_manager_remove_files").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );

        CACHE_MANAGER_CLEAR = linker.downcallHandle(
            lib.find("df_cache_manager_clear").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        // i64 df_cache_manager_clear_by_type(runtime_ptr, type_ptr, type_len)
        CACHE_MANAGER_CLEAR_BY_TYPE = linker.downcallHandle(
            lib.find("df_cache_manager_clear_by_type").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        CACHE_MANAGER_GET_MEMORY_BY_TYPE = linker.downcallHandle(
            lib.find("df_cache_manager_get_memory_by_type").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        CACHE_MANAGER_GET_TOTAL_MEMORY = linker.downcallHandle(
            lib.find("df_cache_manager_get_total_memory").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        // i64 df_cache_manager_contains_by_type(runtime_ptr, type_ptr, type_len, file_ptr, file_len)
        CACHE_MANAGER_CONTAINS_BY_TYPE = linker.downcallHandle(
            lib.find("df_cache_manager_contains_by_type").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );

        CACHE_MANAGER_UPDATE_SIZE_LIMIT = linker.downcallHandle(
            lib.find("df_cache_manager_update_size_limit").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        SET_COLUMN_INDEX_CACHE_LIMIT = linker.downcallHandle(
            lib.find("df_set_column_index_cache_limit").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        SET_OFFSET_INDEX_CACHE_LIMIT = linker.downcallHandle(
            lib.find("df_set_offset_index_cache_limit").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        CLEAR_SCOPED_PAGE_INDEX_CACHE = linker.downcallHandle(
            lib.find("df_clear_scoped_page_index_cache").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG)
        );
        SET_SCOPED_PAGE_INDEX_ENABLED = linker.downcallHandle(
            lib.find("df_set_scoped_page_index_enabled").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        CANCEL_QUERY = linker.downcallHandle(lib.find("df_cancel_query").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

        SET_CANCEL_STATS_THRESHOLD_MS = linker.downcallHandle(
            lib.find("df_set_cancel_stats_threshold_ms").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        // Hand the five filter-tree upcall stubs to Rust now. No explicit
        // caller step required — as soon as this class is loaded, callbacks
        // are installed and `df_execute_indexed_query` can dispatch into Java.
        installFilterTreeCallbacks(linker);

        CLOSE_SESSION_CONTEXT = linker.downcallHandle(
            lib.find("df_close_session_context").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        EXECUTE_WITH_CONTEXT = linker.downcallHandle(
            lib.find("df_execute_with_context").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        // i64 df_stats(runtime_ptr, out_ptr, out_cap)
        STATS = linker.downcallHandle(
            lib.find("df_stats").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        // i64 df_query_registry_top_n_by_current(out_ptr, cap_entries)
        QUERY_REGISTRY_TOP_N_BY_CURRENT = linker.downcallHandle(
            lib.find("df_query_registry_top_n_by_current").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        // i64 df_native_node_stats(out_ptr, out_cap)
        DF_NATIVE_NODE_STATS = linker.downcallHandle(
            lib.find("df_native_node_stats").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        // ── Distributed aggregate: prepare partial/final plans ──
        // i64 df_prepare_partial_plan(handle_ptr, bytes_ptr, bytes_len)
        PREPARE_PARTIAL_PLAN = linker.downcallHandle(
            lib.find("df_prepare_partial_plan").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        // i64 df_prepare_final_plan(session_ptr, bytes_ptr, bytes_len)
        PREPARE_FINAL_PLAN = linker.downcallHandle(
            lib.find("df_prepare_final_plan").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        // i64 df_execute_local_prepared_plan(session_ptr, context_id)
        EXECUTE_LOCAL_PREPARED_PLAN = linker.downcallHandle(
            lib.find("df_execute_local_prepared_plan").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        // i64 df_fetch_by_row_ids(shard_view_ptr, row_ids_buf_ptr, row_ids_count,
        // col_names_ptr, col_names_len_ptr, col_names_count, runtime_ptr, context_id)
        FETCH_BY_ROW_IDS = linker.downcallHandle(
            lib.find("df_fetch_by_row_ids").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        // i64 df_update_concurrency_gate(gate_name_ptr, gate_name_len, new_max_permits)
        UPDATE_CONCURRENCY_GATE = linker.downcallHandle(
            lib.find("df_update_concurrency_gate").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT)
        );
    }

    private NativeBridge() {}

    private static void installFilterTreeCallbacks(Linker linker) {
        try {
            java.lang.foreign.Arena arena = java.lang.foreign.Arena.global();
            Class<?> cb = org.opensearch.be.datafusion.indexfilter.FilterTreeCallbacks.class;
            var lookup = java.lang.invoke.MethodHandles.lookup();

            // All five callbacks now receive contextId (long) as their first parameter.
            // Rust passes it through every FFM upcall; Java uses it to look up the
            // correct per-query FilterDelegationHandle in FilterTreeCallbacks.BINDINGS.
            MethodHandle createProvider = lookup.findStatic(
                cb,
                "createProvider",
                java.lang.invoke.MethodType.methodType(int.class, long.class, int.class)
            );
            MethodHandle releaseProvider = lookup.findStatic(
                cb,
                "releaseProvider",
                java.lang.invoke.MethodType.methodType(void.class, long.class, int.class)
            );
            MethodHandle createCollector = lookup.findStatic(
                cb,
                "createCollector",
                java.lang.invoke.MethodType.methodType(int.class, long.class, int.class, long.class, int.class, int.class)
            );
            MethodHandle collectDocs = lookup.findStatic(
                cb,
                "collectDocs",
                java.lang.invoke.MethodType.methodType(
                    long.class,
                    long.class,
                    int.class,
                    int.class,
                    int.class,
                    java.lang.foreign.MemorySegment.class,
                    long.class
                )
            );
            MethodHandle releaseCollector = lookup.findStatic(
                cb,
                "releaseCollector",
                java.lang.invoke.MethodType.methodType(void.class, long.class, int.class)
            );

            java.lang.foreign.MemorySegment createProviderStub = linker.upcallStub(
                createProvider,
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT),
                arena
            );
            java.lang.foreign.MemorySegment releaseProviderStub = linker.upcallStub(
                releaseProvider,
                FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT),
                arena
            );
            java.lang.foreign.MemorySegment createCollectorStub = linker.upcallStub(
                createCollector,
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT
                ),
                arena
            );
            java.lang.foreign.MemorySegment collectDocsStub = linker.upcallStub(
                collectDocs,
                FunctionDescriptor.of(
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG
                ),
                arena
            );
            java.lang.foreign.MemorySegment releaseCollectorStub = linker.upcallStub(
                releaseCollector,
                FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT),
                arena
            );
            NativeCall.invokeVoid(
                REGISTER_FILTER_TREE_CALLBACKS,
                createProviderStub,
                releaseProviderStub,
                createCollectorStub,
                collectDocsStub,
                releaseCollectorStub
            );
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }
    }

    // ---- Tokio runtime management (no Arena needed — no string/buffer args) ----

    public static void initTokioRuntimeManager(int cpuThreads, double datanodeMultiplier, double coordinatorMultiplier) {
        NativeCall.invokeVoid(INIT_RUNTIME_MANAGER, cpuThreads, datanodeMultiplier, coordinatorMultiplier);
    }

    /** Convenience overload with default 1.5x multipliers for both gates. */
    public static void initTokioRuntimeManager(int cpuThreads) {
        initTokioRuntimeManager(cpuThreads, 1.5, 1.5);
    }

    public static void shutdownTokioRuntimeManager() {
        NativeCall.invokeVoid(SHUTDOWN_RUNTIME_MANAGER);
    }

    /**
     * Updates the effective permit count of a concurrency gate at runtime.
     * Gate names: "fragment_executor" or "reduce".
     */
    public static void updateConcurrencyGate(String gateName, int newMaxPermits) {
        try (var call = new NativeCall()) {
            var name = call.str(gateName);
            call.invoke(UPDATE_CONCURRENCY_GATE, name.segment(), name.len(), newMaxPermits);
        }
    }

    // ---- DataFusion runtime (confined Arena for spillDir string only) ----

    /**
     * Creates a global DataFusion runtime. Returns an opaque native pointer ({@code long}).
     * This pointer is <b>not</b> a MemorySegment — it's a Rust heap address that lives
     * until {@link #closeGlobalRuntime} is called.
     */
    public static long createGlobalRuntime(long memoryLimit, long cacheManagerPtr, String spillDir, long spillLimit) {
        try (var call = new NativeCall()) {
            var dir = call.str(spillDir);
            return call.invoke(CREATE_GLOBAL_RUNTIME, memoryLimit, cacheManagerPtr, dir.segment(), dir.len(), spillLimit);
        }
    }

    /** Frees the native runtime. Safe to call once. */
    public static void closeGlobalRuntime(long ptr) {
        NativeCall.invokeVoid(CLOSE_GLOBAL_RUNTIME, ptr);
    }

    // ---- Memory pool observability and dynamic limit ----

    /** Returns current memory pool usage in bytes. */
    public static long getMemoryPoolUsage(long runtimePtr) {
        try (var call = new NativeCall()) {
            return call.invoke(GET_MEMORY_POOL_USAGE, runtimePtr);
        }
    }

    /** Returns current memory pool limit in bytes. */
    public static long getMemoryPoolLimit(long runtimePtr) {
        try (var call = new NativeCall()) {
            return call.invoke(GET_MEMORY_POOL_LIMIT, runtimePtr);
        }
    }

    /** Returns memory pool stats: [usage_bytes, tripped_count]. Single FFM call. */
    public static long[] getMemoryPoolStats(long runtimePtr) {
        try (var arena = Arena.ofConfined()) {
            var buf = arena.allocate(ValueLayout.JAVA_LONG, 2);
            GET_MEMORY_POOL_STATS.invokeExact(runtimePtr, buf);
            return new long[] { buf.getAtIndex(ValueLayout.JAVA_LONG, 0), buf.getAtIndex(ValueLayout.JAVA_LONG, 1) };
        } catch (Throwable t) {
            logger.debug("Failed to read native memory pool stats", t);
            return new long[] { 0, 0 };
        }
    }

    /** Sets the memory pool limit at runtime. Takes effect for new allocations only. */
    public static void setMemoryPoolLimit(long runtimePtr, long newLimitBytes) {
        try (var call = new NativeCall()) {
            call.invoke(SET_MEMORY_POOL_LIMIT, runtimePtr, newLimitBytes);
        }
    }

    /**
     * Returns true if the loaded native library exports {@code df_set_spill_limit}.
     * When false, spill-cap updates require a node restart — the public
     * {@link org.opensearch.be.datafusion.DataFusionPlugin#DATAFUSION_SPILL_MEMORY_LIMIT}
     * setting stays {@code NodeScope}-only.
     */
    public static boolean isSpillLimitDynamic() {
        return SET_SPILL_LIMIT != null;
    }

    /**
     * Updates the spill-temp-directory cap at runtime. Throws
     * {@link UnsupportedOperationException} when the loaded native library does not
     * export {@code df_set_spill_limit} — callers should gate on
     * {@link #isSpillLimitDynamic()} before invoking.
     */
    public static void setSpillLimit(long runtimePtr, long newLimitBytes) {
        if (SET_SPILL_LIMIT == null) {
            throw new UnsupportedOperationException(
                "df_set_spill_limit not available in the loaded native library; spill cap is fixed at startup"
            );
        }
        try (var call = new NativeCall()) {
            call.invoke(SET_SPILL_LIMIT, runtimePtr, newLimitBytes);
        }
    }

    /** Sets the minimum target_partitions floor for the adaptive budget system. */
    public static void setMinTargetPartitions(int value) {
        try {
            SET_MIN_TARGET_PARTITIONS.invokeExact((long) value);
        } catch (Throwable t) {
            logger.debug("Failed to set min target partitions", t);
        }
    }

    /** Sets the initial target_partitions for coordinator-reduce sessions. */
    public static void setReduceTargetPartitions(int value) {
        try {
            SET_REDUCE_TARGET_PARTITIONS.invokeExact((long) value);
        } catch (Throwable t) {
            logger.debug("Failed to set reduce target partitions", t);
        }
    }

    /**
     * Sets the spill-exemption cap in bytes — the total in-flight allocation allowed through the
     * 85% spill gate by spillable consumers so they can finish spilling. Live-tunable; takes effect
     * on the next try_grow.
     */
    public static void setSpillExemptCapBytes(long bytes) {
        try {
            SET_SPILL_EXEMPT_CAP_BYTES.invokeExact(bytes);
        } catch (Throwable t) {
            logger.debug("Failed to set spill exempt cap bytes", t);
        }
    }

    /** Sets the memory guard thresholds (0.0–1.0): admission throttle, admission reject, execution spill, execution critical. */
    public static void setMemoryGuardThresholds(
        double admissionThrottle,
        double admissionReject,
        double executionSpill,
        double executionCritical
    ) {
        try {
            SET_MEMORY_GUARD_THRESHOLDS.invokeExact(
                (long) (admissionThrottle * 1000),
                (long) (admissionReject * 1000),
                (long) (executionSpill * 1000),
                (long) (executionCritical * 1000)
            );
        } catch (Throwable t) {
            logger.debug("Failed to set memory guard thresholds", t);
        }
    }

    // ---- Reader management (confined Arena for path + file strings) ----

    /**
     * Creates a native reader. Returns an opaque native pointer.
     * Freed by {@link #closeDatafusionReader}.
     *
     * @param path     shard data directory
     * @param segments per-segment metadata — each carries a single filename and writer generation
     * @param dataformatAwareStoreHandle per-format native store handle (null = local, live = use store pointer)
     * @param sortFields index.sort.field values, or empty list if the index has no sort configured.
     *                   Parallel to {@code sortOrders}. Two consumers Rust-side: vanilla path's
     *                   {@code ListingOptions.with_file_sort_order(...)} so the parquet scan advertises
     *                   {@code output_ordering} to the optimizer, and indexed path's segment-iteration
     *                   reversal when the query's leading ORDER BY runs counter to catalog direction.
     * @param sortOrders index.sort.order values ("asc" or "desc"), parallel to {@code sortFields}.
     */
    public static long createDatafusionReader(
        String path,
        List<org.opensearch.index.engine.exec.MonoFileWriterSet> segments,
        NativeStoreHandle dataformatAwareStoreHandle,
        List<String> sortFields,
        List<String> sortOrders
    ) {
        long storePtr = 0L;
        if (dataformatAwareStoreHandle != null) {
            try {
                storePtr = dataformatAwareStoreHandle.getPointer();
            } catch (IllegalStateException e) {
                // Handle closed between check and extraction — use default (local)
                storePtr = 0L;
            }
        }
        if (sortFields == null) sortFields = List.of();
        if (sortOrders == null) sortOrders = List.of();
        if (sortFields.size() != sortOrders.size()) {
            throw new IllegalArgumentException(
                "createDatafusionReader: sortFields ("
                    + sortFields.size()
                    + ") and sortOrders ("
                    + sortOrders.size()
                    + ") must have the same length"
            );
        }
        try (var call = new NativeCall()) {
            var p = call.str(path);
            var f = call.strArray(segments.stream().map(org.opensearch.index.engine.exec.MonoFileWriterSet::file).toArray(String[]::new));
            var gens = call.longs(
                segments.stream().mapToLong(org.opensearch.index.engine.exec.MonoFileWriterSet::writerGeneration).toArray()
            );
            var sf = call.strArray(sortFields.toArray(String[]::new));
            var so = call.strArray(sortOrders.toArray(String[]::new));
            return call.invoke(
                CREATE_READER,
                p.segment(),
                p.len(),
                f.ptrs(),
                f.lens(),
                gens,
                f.count(),
                storePtr,
                sf.ptrs(),
                sf.lens(),
                so.ptrs(),
                so.lens(),
                sf.count()
            );
        }
    }

    public static void closeDatafusionReader(long ptr) {
        NativeCall.invokeVoid(CLOSE_READER, ptr);
    }

    // ---- Query execution (confined Arena for tableName + plan bytes) ----

    /** {@code internal_search_mode}: normal query — decode {@code substraitPlan} as Substrait. */
    public static final long INTERNAL_SEARCH_OFF = 0L;
    /** {@code internal_search_mode}: get-by-row-id — native plan filters {@code __row_id__ = bound}, {@code substraitPlan} ignored. */
    public static final long INTERNAL_SEARCH_BY_ROW_ID = 1L;
    /** {@code internal_search_mode}: seq-no scan — native plan filters {@code _seq_no > bound}, {@code substraitPlan} ignored. */
    public static final long INTERNAL_SEARCH_SEQ_NO_ABOVE = 2L;

    public static void executeQueryAsync(
        long readerPtr,
        String tableName,
        byte[] substraitPlan,
        long runtimePtr,
        long contextId,
        long queryConfigPtr,
        ActionListener<Long> listener
    ) {
        executeQueryAsync(readerPtr, tableName, substraitPlan, runtimePtr, contextId, queryConfigPtr, INTERNAL_SEARCH_OFF, 0L, listener);
    }

    /**
     * Executes a query and returns an opaque stream pointer via {@code listener}.
     * <p>
     * When {@code internalSearchMode} is {@link #INTERNAL_SEARCH_OFF}, {@code substraitPlan} is
     * decoded as a Substrait plan (normal search). When it is {@link #INTERNAL_SEARCH_BY_ROW_ID}
     * or {@link #INTERNAL_SEARCH_SEQ_NO_ABOVE}, the native side ignores {@code substraitPlan} and
     * builds a single pushed-down filter plan via the DataFusion DataFrame API, using
     * {@code internalSearchBound} as the {@code __row_id__} value or the {@code _seq_no} floor.
     * The returned stream is drained identically in all modes.
     */
    public static void executeQueryAsync(
        long readerPtr,
        String tableName,
        byte[] substraitPlan,
        long runtimePtr,
        long contextId,
        long queryConfigPtr,
        long internalSearchMode,
        long internalSearchBound,
        ActionListener<Long> listener
    ) {
        try {
            NativeHandle.validatePointer(readerPtr, "reader");
            NativeHandle.validatePointer(runtimePtr, "runtime");
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try (var call = new NativeCall()) {
            var table = call.str(tableName);
            long result = call.invoke(
                EXECUTE_QUERY,
                readerPtr,
                table.segment(),
                table.len(),
                call.bytes(substraitPlan),
                (long) substraitPlan.length,
                runtimePtr,
                contextId,
                queryConfigPtr,
                internalSearchMode,
                internalSearchBound
            );
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(convertNativeError(t));
        }
    }

    // ---- Stream operations (no Arena needed — only long args) ----

    public static void streamGetSchema(long streamPtr, ActionListener<Long> listener) {
        try {
            NativeHandle.validatePointer(streamPtr, "stream");
            long result = NativeLibraryLoader.checkResult((long) STREAM_GET_SCHEMA.invokeExact(streamPtr));
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(convertNativeError(t));
        }
    }

    public static void streamNext(long runtimePtr, long streamPtr, ActionListener<Long> listener) {
        try {
            NativeHandle.validatePointer(streamPtr, "stream");
            long result = NativeLibraryLoader.checkResult((long) STREAM_NEXT.invokeExact(streamPtr));
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(convertNativeError(t));
        }
    }

    public static void streamClose(long streamPtr) {
        NativeCall.invokeVoid(STREAM_CLOSE, streamPtr);
    }

    /**
     * Extracts execution metrics from the stream's physical plan as JSON bytes.
     * Returns null if no metrics are available (e.g., plan didn't capture them).
     * Must be called BEFORE streamClose (the stream handle must still be alive).
     */
    public static byte[] streamGetMetrics(long streamPtr) {
        try (var arena = Arena.ofConfined()) {
            var outPtr = arena.allocate(ValueLayout.ADDRESS);
            var outLen = arena.allocate(ValueLayout.JAVA_LONG);
            long result = (long) STREAM_GET_METRICS.invokeExact(streamPtr, outPtr, outLen);
            if (result != 0) {
                return null; // no metrics available
            }
            long len = outLen.get(ValueLayout.JAVA_LONG, 0);
            MemorySegment dataPtr = outPtr.get(ValueLayout.ADDRESS, 0);
            byte[] bytes = dataPtr.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
            // Free the Rust-allocated buffer
            FREE_METRICS_BUF.invokeExact(dataPtr, len);
            return bytes;
        } catch (Throwable t) {
            logger.debug("Failed to read native stream metrics", t);
            return null;
        }
    }

    // ---- Cancellation ----

    /** Fires the cancellation token for the given context. No-op if already completed. */
    public static void cancelQuery(long contextId) {
        NativeCall.invokeVoid(CANCEL_QUERY, contextId);
    }

    /**
     * Sets the cancellation stats threshold in milliseconds.
     * Queries cancelled for less than this duration are not counted in stats.
     * Primarily for testing — production uses the default (10 000 ms).
     */
    public static void setCancelStatsThresholdMs(long millis) {
        NativeCall.invokeVoid(SET_CANCEL_STATS_THRESHOLD_MS, millis);
    }

    // ---- Stats collection ----

    /**
     * Collects all native executor metrics in a single FFM call.
     * Decodes directly from the MemorySegment — no intermediate long[].
     *
     * @return a fully constructed {@link DataFusionStats}
     * @throws IllegalStateException if the runtime manager is not initialized
     */
    public static DataFusionStats stats(long runtimePtr) {
        try (var call = new NativeCall()) {
            var seg = call.buf((int) StatsLayout.LAYOUT.byteSize());
            call.invoke(STATS, runtimePtr, seg, StatsLayout.LAYOUT.byteSize());

            // IO runtime (always present — zeroed if not yet initialized)
            var ioRuntime = StatsLayout.readRuntimeMetrics(seg, "io_runtime");

            // CPU runtime (always present — zeroed when absent)
            var cpuRuntime = StatsLayout.readRuntimeMetrics(seg, "cpu_runtime");

            // Task monitors
            var taskMonitors = new LinkedHashMap<String, TaskMonitorStats>();
            for (NativeExecutorsStats.OperationType op : NativeExecutorsStats.OperationType.values()) {
                taskMonitors.put(op.key(), StatsLayout.readTaskMonitor(seg, op.key()));
            }

            // Partition gates
            var fragmentExecutorGate = StatsLayout.readPartitionGate(seg, "fragment_executor_gate", "fragment_executor_gate");

            // Cache stats (zeroed in native when caches are disabled)
            var cacheStats = StatsLayout.readCacheStats(seg);

            // Search stats
            var searchStats = StatsLayout.readSearchStats(seg);

            return new DataFusionStats(
                new NativeExecutorsStats(ioRuntime, cpuRuntime, taskMonitors),
                fragmentExecutorGate,
                StatsLayout.readAdaptiveBudgetStats(seg),
                null,
                cacheStats,
                searchStats
            );
        }
    }

    // ---- Per-query registry top-N snapshot ----

    /**
     * Snapshot the {@code n} heaviest live queries by {@code current_bytes}.
     *
     * <p>One FFM call: Java allocates an {@code n × ENTRY_BYTES} segment, Rust
     * runs a bounded min-heap of size {@code n} over its registry and writes
     * the survivors as {@link QueryRegistryLayout} entries. Completed and
     * zero-byte trackers are filtered on the Rust side. Order within the
     * buffer is unspecified.
     *
     * <p>Mirrors the {@link #stats(long)} pattern: the layout class decodes
     * directly into the caller's final type ({@link QueryExecutionMetrics})
     * with no transport-only intermediate.
     *
     * @param n maximum entries to return; must be non-negative. Zero short-circuits
     *          without crossing the FFM boundary.
     * @return a non-null map of {@code contextId → metrics}, possibly empty,
     *         with at most {@code n} entries.
     * @throws IllegalArgumentException if {@code n} is negative or implies a
     *         buffer larger than {@link Integer#MAX_VALUE} bytes
     */
    public static Map<Long, QueryExecutionMetrics> getTopNQueriesByMemory(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("n must be non-negative: " + n);
        }
        if (n == 0) {
            return Collections.emptyMap();
        }
        long bytes = (long) n * QueryRegistryLayout.ENTRY_BYTES;
        if (bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("n too large for a single snapshot: " + n);
        }
        try (var call = new NativeCall()) {
            var seg = call.buf((int) bytes);
            long written = call.invoke(QUERY_REGISTRY_TOP_N_BY_CURRENT, seg, (long) n);
            if (written == 0L) {
                return Collections.emptyMap();
            }
            int rows = (int) Math.min(written, (long) n);
            Map<Long, QueryExecutionMetrics> out = new HashMap<>(rows);
            for (int i = 0; i < rows; i++) {
                long ctxId = QueryRegistryLayout.readContextId(seg, i);
                out.put(ctxId, QueryRegistryLayout.readMetrics(seg, i));
            }
            return Collections.unmodifiableMap(out);
        }
    }

    /**
     * Reads native task cancellation counters via {@code df_native_node_stats} FFM call.
     * Independent of {@link #stats(long)} which calls {@code df_stats} for plugin stats.
     *
     * @return a populated {@link AnalyticsBackendTaskCancellationStats}
     * @throws IllegalStateException if the FFM function returns a non-zero error code
     */
    public static AnalyticsBackendTaskCancellationStats nativeNodeStats() {
        try (var call = new NativeCall()) {
            var seg = call.buf(32);
            call.invoke(DF_NATIVE_NODE_STATS, seg, 32L);
            return NativeNodeStatsLayout.readNativeNodeStats(seg);
        }
    }

    // ---- Stubs ----

    public static byte[] sqlToSubstrait(long readerPtr, String tableName, String sql, long runtimePtr) {
        NativeHandle.validatePointer(readerPtr, "reader");
        NativeHandle.validatePointer(runtimePtr, "runtime");
        try (var call = new NativeCall()) {
            var table = call.str(tableName);
            var query = call.str(sql);
            var out = call.outBuffer(1024 * 1024);
            call.invoke(
                SQL_TO_SUBSTRAIT,
                readerPtr,
                table.segment(),
                table.len(),
                query.segment(),
                query.len(),
                runtimePtr,
                out.data(),
                (long) out.capacity(),
                out.lenOut()
            );
            return out.toByteArray();
        }
    }

    // ---- Coordinator-reduce exports ----

    /**
     * Pair returned from {@link #registerPartitionStream} / {@link #registerMemtable}: the
     * native sender pointer (or 0 for memtable) plus the Arrow IPC-encoded schema the native
     * session derived by lowering the producer-side substrait. The Java tripwire
     * ({@code typesMatch} in {@code DatafusionReduceSink}) validates fed batches against this
     * schema, and downstream callers decode it once into an Arrow {@link org.apache.arrow.vector.types.pojo.Schema}.
     */
    public record RegisteredInput(long pointer, byte[] schemaIpc) {
    }

    /**
     * Creates a local DataFusion session tied to the given global runtime. Returns an opaque
     * native pointer freed by {@link #closeLocalSession}.
     */
    public static long createLocalSession(long runtimePtr) {
        NativeHandle.validatePointer(runtimePtr, "runtime");
        try (var call = new NativeCall()) {
            return call.invoke(CREATE_LOCAL_SESSION, runtimePtr);
        }
    }

    /** Frees the native local session. Tolerates a zero pointer for idempotent close. */
    public static void closeLocalSession(long sessionPtr) {
        NativeCall.invokeVoid(CLOSE_LOCAL_SESSION, sessionPtr);
    }

    /**
     * Registers an input partition stream on the session under {@code inputId}, deriving the
     * input schema by lowering the producer-side {@code partialPlanBytes}. Returns the native
     * sender pointer (freed by {@link #senderClose}) and the Arrow IPC-encoded schema the
     * native session settled on after lowering.
     */
    public static RegisteredInput registerPartitionStream(long sessionPtr, String inputId, byte[] partialPlanBytes) {
        NativeHandle.validatePointer(sessionPtr, "session");
        try (var call = new NativeCall()) {
            var id = call.str(inputId);
            var out = call.outBuffer(64 * 1024);
            long ptr = call.invoke(
                REGISTER_PARTITION_STREAM,
                sessionPtr,
                id.segment(),
                id.len(),
                call.bytes(partialPlanBytes),
                (long) partialPlanBytes.length,
                out.data(),
                (long) out.capacity(),
                out.lenOut()
            );
            return new RegisteredInput(ptr, out.toByteArray());
        }
    }

    /**
     * Executes a Substrait plan on the session, returning an opaque stream pointer. The stream is
     * drained via {@link #streamNext} and freed by {@link #streamClose}.
     *
     * @param sessionPtr pointer returned by {@link #createLocalSession}
     * @param substrait  Substrait plan bytes
     * @param contextId  the parent {@code AnalyticsQueryTask.getId()}; registers the reduce in the
     *                   native {@code QUERY_REGISTRY} under this id so {@link #cancelQuery} fires
     *                   the attached cancellation token. Pass {@code 0} to disable tracking.
     */
    public static long executeLocalPlan(long sessionPtr, byte[] substrait, long contextId) {
        NativeHandle.validatePointer(sessionPtr, "session");
        try (var call = new NativeCall()) {
            return call.invoke(EXECUTE_LOCAL_PLAN, sessionPtr, call.bytes(substrait), (long) substrait.length, contextId);
        } catch (RuntimeException e) {
            throw rethrowConverted(e);
        }
    }

    /**
     * Positive sentinel returned by {@code df_sender_send} (via {@link #senderSend}) when the
     * send was skipped because the consumer dropped the receiver before this batch could be sent
     * — the benign "consumer finished first" case (e.g. a LimitExec satisfied its fetch). It
     * rides the success half of the native return contract ({@code >= 0} success, {@code < 0}
     * {@code -error_ptr}), so {@code checkResult} passes it through without throwing.
     * MUST match {@code SENDER_SEND_RECEIVER_DROPPED} in {@code ffm.rs}.
     */
    public static final long SENDER_SEND_RECEIVER_DROPPED = 1L;

    /**
     * Pushes one Arrow C Data-exported batch (array + schema addresses) into the sender. The
     * native side takes ownership of both FFI structs. Returns {@code 0} on a normal send or
     * {@link #SENDER_SEND_RECEIVER_DROPPED} if the consumer already dropped the receiver.
     */
    public static long senderSend(long senderPtr, long arrayPtr, long schemaPtr) {
        NativeHandle.validatePointer(senderPtr, "sender");
        // arrayPtr/schemaPtr come from Arrow Java's C Data export (ArrowArray.memoryAddress()),
        // NOT from our NativeHandle lifecycle — validate as non-zero rather than live-handle.
        if (arrayPtr == 0) {
            throw new IllegalArgumentException("arrayPtr must be non-zero");
        }
        if (schemaPtr == 0) {
            throw new IllegalArgumentException("schemaPtr must be non-zero");
        }
        try (var call = new NativeCall()) {
            return call.invoke(SENDER_SEND, senderPtr, arrayPtr, schemaPtr);
        }
    }

    /** Closes the sender, signalling end-of-input. Tolerates a zero pointer. */
    public static void senderClose(long senderPtr) {
        NativeCall.invokeVoid(SENDER_CLOSE, senderPtr);
    }

    /**
     * Memtable variant of {@link #registerPartitionStream}: hands across a list of
     * already-exported Arrow C Data batches in two parallel pointer arrays so the native side
     * can build a {@code MemTable} in one shot. Schema is derived by lowering the producer-side
     * {@code partialPlanBytes}; native takes ownership of all FFI structs on success.
     *
     * <p>Returns a {@link RegisteredInput} whose {@code pointer} field is always 0 (memtable
     * registration has no sender to return) — the {@code schemaIpc} field carries the schema
     * the native session settled on after lowering.
     */
    public static RegisteredInput registerMemtable(
        long sessionPtr,
        String inputId,
        byte[] partialPlanBytes,
        long[] arrayPtrs,
        long[] schemaPtrs
    ) {
        NativeHandle.validatePointer(sessionPtr, "session");
        if (arrayPtrs.length != schemaPtrs.length) {
            throw new IllegalArgumentException(
                "arrayPtrs.length (" + arrayPtrs.length + ") != schemaPtrs.length (" + schemaPtrs.length + ")"
            );
        }
        try (var call = new NativeCall()) {
            var id = call.str(inputId);
            var out = call.outBuffer(64 * 1024);
            long ptr = call.invoke(
                REGISTER_MEMTABLE,
                sessionPtr,
                id.segment(),
                id.len(),
                call.bytes(partialPlanBytes),
                (long) partialPlanBytes.length,
                call.longs(arrayPtrs),
                call.longs(schemaPtrs),
                (long) arrayPtrs.length,
                out.data(),
                (long) out.capacity(),
                out.lenOut()
            );
            return new RegisteredInput(ptr, out.toByteArray());
        }
    }

    public static long createCustomCacheManager() {
        try {
            return NativeLibraryLoader.checkResult((long) CREATE_CUSTOM_CACHE_MANAGER.invokeExact());
        } catch (Throwable t) {
            throw t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t);
        }
    }
    // ---- SessionContext decomposition ----

    /**
     * Creates a SessionContext with the default ListingTable registered.
     * Returns a tracked handle consumed by {@link #executeWithContextAsync}.
     *
     * @param tableName the logical table name (alias/pattern) to register the table under
     * @param queryConfigPtr pointer to a WireDatafusionQueryConfig struct, or 0 for fallback defaults
     * @param hasPartialAggregate whether the fragment contains a partial aggregate — signals Rust to
     *                            exclude the CombinePartialFinalAggregate optimizer rule
     * @param planBytes Substrait plan bytes — used to widen the registered schema for multi-index
     *                  queries (null-filling columns this shard omits). Empty = skip widening.
     */
    public static SessionContextHandle createSessionContext(
        long readerPtr,
        long runtimePtr,
        String tableName,
        long contextId,
        boolean hasPartialAggregate,
        long queryConfigPtr,
        byte[] planBytes
    ) {
        NativeHandle.validatePointer(readerPtr, "reader");
        NativeHandle.validatePointer(runtimePtr, "runtime");
        try (var call = new NativeCall()) {
            var table = call.str(tableName);
            boolean hasPlan = planBytes != null && planBytes.length > 0;
            MemorySegment planSegment = hasPlan ? call.bytes(planBytes) : MemorySegment.NULL;
            long planLen = hasPlan ? planBytes.length : 0L;
            long ptr = call.invoke(
                CREATE_SESSION_CONTEXT,
                readerPtr,
                runtimePtr,
                table.segment(),
                table.len(),
                contextId,
                queryConfigPtr,
                (byte) (hasPartialAggregate ? 1 : 0),
                planSegment,
                planLen
            );
            return new SessionContextHandle(ptr);
        }
    }

    /**
     * Creates a SessionContext configured for indexed execution with filter delegation.
     * Registers the delegated_predicate UDF and stores treeShape + delegatedPredicateCount
     * on the Rust handle for use during execution.
     *
     * @param tableName the logical table name (alias/pattern) to register the table under
     * @param hasPartialAggregate whether the fragment contains a partial aggregate — signals Rust to
     *                            exclude the CombinePartialFinalAggregate optimizer rule
     * @param queryConfigPtr pointer to a WireDatafusionQueryConfig struct, or 0 for fallback defaults
     * @param planBytes Substrait plan bytes for multi-index schema widening (empty = skip)
     */
    public static SessionContextHandle createSessionContextForIndexedExecution(
        long readerPtr,
        long runtimePtr,
        String tableName,
        long contextId,
        int treeShapeOrdinal,
        int delegatedPredicateCount,
        boolean requestsRowIds,
        boolean hasPartialAggregate,
        long queryConfigPtr,
        byte[] planBytes
    ) {
        NativeHandle.validatePointer(readerPtr, "reader");
        NativeHandle.validatePointer(runtimePtr, "runtime");
        try (NativeCall call = new NativeCall()) {
            NativeCall.Str table = call.str(tableName);
            boolean hasPlan = planBytes != null && planBytes.length > 0;
            MemorySegment planSegment = hasPlan ? call.bytes(planBytes) : MemorySegment.NULL;
            long planLen = hasPlan ? planBytes.length : 0L;
            long ptr = call.invoke(
                CREATE_SESSION_CONTEXT_INDEXED,
                readerPtr,
                runtimePtr,
                table.segment(),
                table.len(),
                contextId,
                treeShapeOrdinal,
                delegatedPredicateCount,
                (byte) (requestsRowIds ? 1 : 0),
                (byte) (hasPartialAggregate ? 1 : 0),
                queryConfigPtr,
                planSegment,
                planLen
            );
            return new SessionContextHandle(ptr);
        }
    }

    /**
     * Frees a native {@code SessionContext} handle. Invoked from
     * {@link SessionContextHandle#doCloseNative()} ()} on error / never-executed paths; not called on the
     * happy path where Rust's {@code execute_with_context} consumes the handle itself.
     * Safe to call at most once per pointer.
     */
    public static void closeSessionContext(long ptr) {
        NativeCall.invokeVoid(CLOSE_SESSION_CONTEXT, ptr);
    }

    /**
     * Executes a Substrait plan against the configured SessionContext.
     *
     * <p>Rust's {@code execute_with_context} takes ownership of the {@code SessionContext} via
     * {@code Box::from_raw} on entry, regardless of whether the rest of the call then succeeds or
     * returns an error. The handle is therefore marked consumed in a {@code finally} block so
     * that both success and native-error paths skip {@code df_close_session_context} (which
     * would otherwise double-free). Only a Java-side failure before the downcall dispatches
     * (argument marshalling) leaves the handle unconsumed, in which case its
     * {@link SessionContextHandle#doCloseNative()} ()} will free it.
     */
    public static void executeWithContextAsync(SessionContextHandle sessionContext, byte[] substraitPlan, ActionListener<Long> listener) {
        final long sessionCtxPtr;
        try {
            sessionCtxPtr = sessionContext.getPointer();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try (var call = new NativeCall()) {
            var plan = call.bytes(substraitPlan);
            long planLen = (long) substraitPlan.length;
            long result;
            try {
                result = call.invoke(EXECUTE_WITH_CONTEXT, sessionCtxPtr, plan, planLen);
            } finally {
                // Rust took ownership via Box::from_raw; do not let doClose() double-free.
                sessionContext.markConsumed();
            }
            listener.onResponse(result);
        } catch (Throwable throwable) {
            listener.onFailure(convertNativeError(throwable));
        }
    }

    public static void destroyCustomCacheManager(long ptr) {
        NativeCall.invokeVoid(DESTROY_CUSTOM_CACHE_MANAGER, ptr);
    }

    // ---- Distributed aggregate: prepare partial/final plans ----

    /**
     * Prepares a partial-aggregate physical plan on the session context handle.
     * The plan is stored on the Rust handle for later execution.
     *
     * @param handlePtr pointer returned by {@link #createSessionContext}
     * @param substraitBytes Substrait plan bytes
     */
    public static void preparePartialPlan(long handlePtr, byte[] substraitBytes) {
        NativeHandle.validatePointer(handlePtr, "sessionContext");
        try (var call = new NativeCall()) {
            call.invoke(PREPARE_PARTIAL_PLAN, handlePtr, call.bytes(substraitBytes), (long) substraitBytes.length);
        }
    }

    /**
     * Prepares a final-aggregate physical plan on a local session.
     * The plan is stored on the Rust session for later execution via
     * {@link #executeLocalPreparedPlan}.
     *
     * @param sessionPtr pointer returned by {@link #createLocalSession}
     * @param substraitBytes Substrait plan bytes
     */
    public static void prepareFinalPlan(long sessionPtr, byte[] substraitBytes) {
        NativeHandle.validatePointer(sessionPtr, "session");
        try (var call = new NativeCall()) {
            call.invoke(PREPARE_FINAL_PLAN, sessionPtr, call.bytes(substraitBytes), (long) substraitBytes.length);
        }
    }

    /**
     * Executes the previously prepared final-aggregate plan on a local session.
     * Returns a stream pointer that can be drained via {@link #streamNext} and
     * freed by {@link #streamClose}.
     *
     * @param sessionPtr pointer returned by {@link #createLocalSession} with a plan
     *                   already prepared via {@link #prepareFinalPlan}
     * @param contextId  the parent {@code AnalyticsQueryTask.getId()}; see
     *                   {@link #executeLocalPlan(long, byte[], long)} for semantics
     * @return opaque stream pointer
     */
    public static long executeLocalPreparedPlan(long sessionPtr, long contextId) {
        NativeHandle.validatePointer(sessionPtr, "session");
        try (var call = new NativeCall()) {
            return call.invoke(EXECUTE_LOCAL_PREPARED_PLAN, sessionPtr, contextId);
        } catch (RuntimeException e) {
            throw rethrowConverted(e);
        }
    }

    /**
     * QTF fetch phase: reads specific rows by global row ID from parquet.
     * Row IDs are passed as a direct buffer pointer (zero-copy from BigIntVector's ArrowBuf).
     *
     * @param readerPtr pointer to the shard view (DatafusionReader)
     * @param rowIdsBufAddr memory address of the BigIntVector's data buffer (i64 values)
     * @param rowIdsCount number of row IDs
     * @param columns column names to read
     * @param runtimePtr pointer to the DataFusion runtime
     * @return opaque stream pointer
     */
    public static long fetchByRowIds(
        long readerPtr,
        long rowIdsBufAddr,
        int rowIdsCount,
        String[] columns,
        long runtimePtr,
        long contextId
    ) {
        NativeHandle.validatePointer(readerPtr, "reader");
        NativeHandle.validatePointer(runtimePtr, "runtime");
        if (rowIdsBufAddr == 0) {
            throw new IllegalArgumentException("rowIdsBufAddr must be non-zero");
        }
        try (var call = new NativeCall()) {
            var colNames = call.strArray(columns);
            return call.invoke(
                FETCH_BY_ROW_IDS,
                readerPtr,
                rowIdsBufAddr,
                (long) rowIdsCount,
                colNames.ptrs(),
                colNames.lens(),
                colNames.count(),
                runtimePtr,
                contextId
            );
        } catch (RuntimeException e) {
            throw rethrowConverted(e);
        }
    }

    public static void createCache(long cacheManagerPtr, String cacheType, long sizeLimit, String evictionType) {
        try (var call = new NativeCall()) {
            var type = call.str(cacheType);
            var eviction = call.str(evictionType);
            call.invoke(CREATE_CACHE, cacheManagerPtr, type.segment(), type.len(), sizeLimit, eviction.segment(), eviction.len());
        }
    }

    public static void cacheManagerAddFiles(long runtimePtr, String[] filePaths) {
        try (var call = new NativeCall()) {
            var f = call.strArray(filePaths);
            call.invoke(CACHE_MANAGER_ADD_FILES, runtimePtr, f.ptrs(), f.lens(), f.count());
        }
    }

    /**
     * Load metadata for files through the given TieredObjectStore.
     * Reads footer (lightweight) into heap cache, fetches page/offset index bytes
     * through the store (populating data Foyer), and returns for promotion to metadata Foyer.
     *
     * @param runtimePtr pointer from createGlobalRuntime
     * @param storePtr Box&lt;Arc&lt;dyn ObjectStore&gt;&gt; pointer (from TieredStorageBridge.getObjectStoreBoxPtr)
     * @param filePaths array of absolute file paths
     */
    public static void cacheManagerAddFilesWithStore(long runtimePtr, long storePtr, String[] filePaths) {
        try (var call = new NativeCall()) {
            var f = call.strArray(filePaths);
            call.invoke(CACHE_MANAGER_ADD_FILES_WITH_STORE, runtimePtr, storePtr, f.ptrs(), f.lens(), f.count());
        }
    }

    public static void cacheManagerRemoveFiles(long runtimePtr, String[] filePaths) {
        try (var call = new NativeCall()) {
            var f = call.strArray(filePaths);
            call.invoke(CACHE_MANAGER_REMOVE_FILES, runtimePtr, f.ptrs(), f.lens(), f.count());
        }
    }

    public static void cacheManagerClear(long runtimePtr) {
        try (var call = new NativeCall()) {
            call.invoke(CACHE_MANAGER_CLEAR, runtimePtr);
        }
    }

    public static void cacheManagerClearByCacheType(long runtimePtr, String cacheType) {
        try (var call = new NativeCall()) {
            var type = call.str(cacheType);
            call.invoke(CACHE_MANAGER_CLEAR_BY_TYPE, runtimePtr, type.segment(), type.len());
        }
    }

    public static long cacheManagerGetMemoryConsumedForCacheType(long runtimePtr, String cacheType) {
        try (var call = new NativeCall()) {
            var type = call.str(cacheType);
            return call.invoke(CACHE_MANAGER_GET_MEMORY_BY_TYPE, runtimePtr, type.segment(), type.len());
        }
    }

    public static long cacheManagerGetTotalMemoryConsumed(long runtimePtr) {
        try (var call = new NativeCall()) {
            return call.invoke(CACHE_MANAGER_GET_TOTAL_MEMORY, runtimePtr);
        }
    }

    public static boolean cacheManagerGetItemByCacheType(long runtimePtr, String cacheType, String filePath) {
        try (var call = new NativeCall()) {
            var type = call.str(cacheType);
            var file = call.str(filePath);
            long result = call.invoke(CACHE_MANAGER_CONTAINS_BY_TYPE, runtimePtr, type.segment(), type.len(), file.segment(), file.len());
            return result != 0;
        }
    }

    public static void cacheManagerUpdateSizeLimit(long runtimePtr, String cacheType, long newLimit) {
        try (var call = new NativeCall()) {
            var type = call.str(cacheType);
            call.invoke(CACHE_MANAGER_UPDATE_SIZE_LIMIT, runtimePtr, type.segment(), type.len(), newLimit);
        }
    }

    /**
     * Sets the byte budget of the process-global scoped ColumnIndex cache.
     * Shrinking evicts LRU entries immediately. Zero is ignored.
     */
    public static void setColumnIndexCacheLimit(long sizeLimitBytes) {
        try (var call = new NativeCall()) {
            call.invoke(SET_COLUMN_INDEX_CACHE_LIMIT, sizeLimitBytes);
        }
    }

    /**
     * Sets the byte budget of the process-global scoped OffsetIndex cache.
     * Shrinking evicts LRU entries immediately. Zero is ignored.
     */
    public static void setOffsetIndexCacheLimit(long sizeLimitBytes) {
        try (var call = new NativeCall()) {
            call.invoke(SET_OFFSET_INDEX_CACHE_LIMIT, sizeLimitBytes);
        }
    }

    /**
     * Clears the process-global scoped page-index cache (drops entries + resets
     * counters, keeps the budget). For operational testing.
     */
    public static void clearScopedPageIndexCache() {
        try (var call = new NativeCall()) {
            call.invoke(CLEAR_SCOPED_PAGE_INDEX_CACHE);
        }
    }

    /** Clears the scoped ColumnIndex (predicate) cache. */
    public static void clearColumnIndexCache() {
        // TODO(PR1): wire to df_clear_column_index_cache when available
        try (var call = new NativeCall()) {
            call.invoke(CLEAR_SCOPED_PAGE_INDEX_CACHE);
        }
    }

    /** Clears the scoped OffsetIndex (projection) cache. */
    public static void clearOffsetIndexCache() {
        // TODO(PR1): wire to df_clear_offset_index_cache when available
        try (var call = new NativeCall()) {
            call.invoke(CLEAR_SCOPED_PAGE_INDEX_CACHE);
        }
    }

    /**
     * Enable or disable the scoped page-index feature.
     * When disabled, the metadata cache retains the full page index (fallback mode)
     * and CI/OI scoped caches are bypassed entirely.
     */
    public static void setScopedPageIndexEnabled(boolean enabled) {
        try (var call = new NativeCall()) {
            call.invoke(SET_SCOPED_PAGE_INDEX_ENABLED, enabled ? 1L : 0L);
        }
    }

    public static void initLogger() {}
}

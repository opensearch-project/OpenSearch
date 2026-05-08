/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.core.action.ActionListener;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.LinkedHashMap;

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

    private static final MethodHandle INIT_RUNTIME_MANAGER;
    private static final MethodHandle SHUTDOWN_RUNTIME_MANAGER;
    private static final MethodHandle CREATE_GLOBAL_RUNTIME;
    private static final MethodHandle CLOSE_GLOBAL_RUNTIME;
    private static final MethodHandle GET_MEMORY_POOL_USAGE;
    private static final MethodHandle GET_MEMORY_POOL_LIMIT;
    private static final MethodHandle SET_MEMORY_POOL_LIMIT;
    private static final MethodHandle CREATE_READER;
    private static final MethodHandle CLOSE_READER;
    private static final MethodHandle EXECUTE_QUERY;
    private static final MethodHandle STREAM_GET_SCHEMA;
    private static final MethodHandle STREAM_NEXT;
    private static final MethodHandle STREAM_CLOSE;
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
    private static final MethodHandle CACHE_MANAGER_REMOVE_FILES;
    private static final MethodHandle CACHE_MANAGER_CLEAR;
    private static final MethodHandle CACHE_MANAGER_CLEAR_BY_TYPE;
    private static final MethodHandle CACHE_MANAGER_GET_MEMORY_BY_TYPE;
    private static final MethodHandle CACHE_MANAGER_GET_TOTAL_MEMORY;
    private static final MethodHandle CACHE_MANAGER_CONTAINS_BY_TYPE;
    private static final MethodHandle CREATE_SESSION_CONTEXT;
    private static final MethodHandle CREATE_SESSION_CONTEXT_INDEXED;
    private static final MethodHandle CLOSE_SESSION_CONTEXT;
    private static final MethodHandle EXECUTE_WITH_CONTEXT;
    private static final MethodHandle CANCEL_QUERY;
    private static final MethodHandle STATS;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        INIT_RUNTIME_MANAGER = linker.downcallHandle(
            lib.find("df_init_runtime_manager").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT)
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

        SET_MEMORY_POOL_LIMIT = linker.downcallHandle(
            lib.find("df_set_memory_pool_limit").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        CREATE_READER = linker.downcallHandle(
            lib.find("df_create_reader").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );

        CLOSE_READER = linker.downcallHandle(lib.find("df_close_reader").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

        EXECUTE_QUERY = linker.downcallHandle(
            lib.find("df_execute_query").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
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

        // i64 df_register_partition_stream(session_ptr, input_id_ptr, input_id_len, schema_ipc_ptr, schema_ipc_len)
        REGISTER_PARTITION_STREAM = linker.downcallHandle(
            lib.find("df_register_partition_stream").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );

        // i64 df_execute_local_plan(session_ptr, substrait_ptr, substrait_len)
        EXECUTE_LOCAL_PLAN = linker.downcallHandle(
            lib.find("df_execute_local_plan").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        // i64 df_sender_send(sender_ptr, array_ptr, schema_ptr)
        SENDER_SEND = linker.downcallHandle(
            lib.find("df_sender_send").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        // void df_sender_close(sender_ptr)
        SENDER_CLOSE = linker.downcallHandle(lib.find("df_sender_close").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

        // i64 df_register_memtable(session_ptr, input_id_ptr, input_id_len, schema_ipc_ptr, schema_ipc_len,
        // array_ptrs, schema_ptrs, n_batches)
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
                ValueLayout.JAVA_LONG
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
                ValueLayout.JAVA_INT
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

        CANCEL_QUERY = linker.downcallHandle(lib.find("df_cancel_query").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

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

        // i64 df_stats(out_ptr, out_cap)
        STATS = linker.downcallHandle(
            lib.find("df_stats").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
    }

    private NativeBridge() {}

    private static void installFilterTreeCallbacks(Linker linker) {
        try {
            java.lang.foreign.Arena arena = java.lang.foreign.Arena.global();
            Class<?> cb = org.opensearch.be.datafusion.indexfilter.FilterTreeCallbacks.class;
            var lookup = java.lang.invoke.MethodHandles.lookup();

            MethodHandle createProvider = lookup.findStatic(
                cb,
                "createProvider",
                java.lang.invoke.MethodType.methodType(int.class, int.class)
            );
            MethodHandle releaseProvider = lookup.findStatic(
                cb,
                "releaseProvider",
                java.lang.invoke.MethodType.methodType(void.class, int.class)
            );
            MethodHandle createCollector = lookup.findStatic(
                cb,
                "createCollector",
                java.lang.invoke.MethodType.methodType(int.class, int.class, int.class, int.class, int.class)
            );
            MethodHandle collectDocs = lookup.findStatic(
                cb,
                "collectDocs",
                java.lang.invoke.MethodType.methodType(
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
                java.lang.invoke.MethodType.methodType(void.class, int.class)
            );

            java.lang.foreign.MemorySegment createProviderStub = linker.upcallStub(
                createProvider,
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT),
                arena
            );
            java.lang.foreign.MemorySegment releaseProviderStub = linker.upcallStub(
                releaseProvider,
                FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT),
                arena
            );
            java.lang.foreign.MemorySegment createCollectorStub = linker.upcallStub(
                createCollector,
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT
                ),
                arena
            );
            java.lang.foreign.MemorySegment collectDocsStub = linker.upcallStub(
                collectDocs,
                FunctionDescriptor.of(
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
                FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT),
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

    public static void initTokioRuntimeManager(int cpuThreads) {
        NativeCall.invokeVoid(INIT_RUNTIME_MANAGER, cpuThreads);
    }

    public static void shutdownTokioRuntimeManager() {
        NativeCall.invokeVoid(SHUTDOWN_RUNTIME_MANAGER);
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

    /** Sets the memory pool limit at runtime. Takes effect for new allocations only. */
    public static void setMemoryPoolLimit(long runtimePtr, long newLimitBytes) {
        try (var call = new NativeCall()) {
            call.invoke(SET_MEMORY_POOL_LIMIT, runtimePtr, newLimitBytes);
        }
    }

    // ---- Reader management (confined Arena for path + file strings) ----

    /**
     * Creates a native reader. Returns an opaque native pointer.
     * Freed by {@link #closeDatafusionReader}.
     */
    public static long createDatafusionReader(String path, String[] files) {
        try (var call = new NativeCall()) {
            var p = call.str(path);
            var f = call.strArray(files);
            return call.invoke(CREATE_READER, p.segment(), p.len(), f.ptrs(), f.lens(), f.count());
        }
    }

    public static void closeDatafusionReader(long ptr) {
        NativeCall.invokeVoid(CLOSE_READER, ptr);
    }

    // ---- Query execution (confined Arena for tableName + plan bytes) ----

    public static void executeQueryAsync(
        long readerPtr,
        String tableName,
        byte[] substraitPlan,
        long runtimePtr,
        long contextId,
        long queryConfigPtr,
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
                queryConfigPtr
            );
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    // ---- Stream operations (no Arena needed — only long args) ----

    public static void streamGetSchema(long streamPtr, ActionListener<Long> listener) {
        try {
            NativeHandle.validatePointer(streamPtr, "stream");
            long result = NativeLibraryLoader.checkResult((long) STREAM_GET_SCHEMA.invokeExact(streamPtr));
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    public static void streamNext(long runtimePtr, long streamPtr, ActionListener<Long> listener) {
        try {
            NativeHandle.validatePointer(streamPtr, "stream");
            long result = NativeLibraryLoader.checkResult((long) STREAM_NEXT.invokeExact(streamPtr));
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    public static void streamClose(long streamPtr) {
        NativeCall.invokeVoid(STREAM_CLOSE, streamPtr);
    }

    // ---- Cancellation ----

    /** Fires the cancellation token for the given context. No-op if already completed. */
    public static void cancelQuery(long contextId) {
        NativeCall.invokeVoid(CANCEL_QUERY, contextId);
    }

    // ---- Stats collection ----

    /**
     * Collects all native executor metrics in a single FFM call.
     * Decodes directly from the MemorySegment — no intermediate long[].
     *
     * @return a fully constructed {@link DataFusionStats}
     * @throws IllegalStateException if the runtime manager is not initialized
     */
    public static DataFusionStats stats() {
        try (var call = new NativeCall()) {
            var seg = call.buf((int) StatsLayout.LAYOUT.byteSize());
            call.invoke(STATS, seg, StatsLayout.LAYOUT.byteSize());

            // IO runtime (always present — zeroed if not yet initialized)
            var ioRuntime = StatsLayout.readRuntimeMetrics(seg, "io_runtime");

            // CPU runtime (always present — zeroed when absent)
            var cpuRuntime = StatsLayout.readRuntimeMetrics(seg, "cpu_runtime");

            // Task monitors
            var taskMonitors = new LinkedHashMap<String, NativeExecutorsStats.TaskMonitorStats>();
            for (NativeExecutorsStats.OperationType op : NativeExecutorsStats.OperationType.values()) {
                taskMonitors.put(op.key(), StatsLayout.readTaskMonitor(seg, op.key()));
            }

            return new DataFusionStats(new NativeExecutorsStats(ioRuntime, cpuRuntime, taskMonitors));
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
     * Registers an input partition stream on the session under {@code inputId}, with the given
     * Arrow IPC-encoded schema. Returns an opaque sender pointer freed by {@link #senderClose}.
     */
    public static long registerPartitionStream(long sessionPtr, String inputId, byte[] schemaIpc) {
        NativeHandle.validatePointer(sessionPtr, "session");
        try (var call = new NativeCall()) {
            var id = call.str(inputId);
            return call.invoke(
                REGISTER_PARTITION_STREAM,
                sessionPtr,
                id.segment(),
                id.len(),
                call.bytes(schemaIpc),
                (long) schemaIpc.length
            );
        }
    }

    /**
     * Executes a Substrait plan on the session, returning an opaque stream pointer. The stream is
     * drained via {@link #streamNext} and freed by {@link #streamClose}.
     */
    public static long executeLocalPlan(long sessionPtr, byte[] substrait) {
        NativeHandle.validatePointer(sessionPtr, "session");
        try (var call = new NativeCall()) {
            return call.invoke(EXECUTE_LOCAL_PLAN, sessionPtr, call.bytes(substrait), (long) substrait.length);
        }
    }

    /**
     * Pushes one Arrow C Data-exported batch (array + schema addresses) into the sender. The
     * native side takes ownership of both FFI structs.
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
     * already-exported Arrow C Data batches in two parallel pointer arrays so the native side can
     * build a {@code MemTable} in one shot. Native takes ownership of all FFI structs on success.
     */
    public static long registerMemtable(long sessionPtr, String inputId, byte[] schemaIpc, long[] arrayPtrs, long[] schemaPtrs) {
        NativeHandle.validatePointer(sessionPtr, "session");
        if (arrayPtrs.length != schemaPtrs.length) {
            throw new IllegalArgumentException(
                "arrayPtrs.length (" + arrayPtrs.length + ") != schemaPtrs.length (" + schemaPtrs.length + ")"
            );
        }
        try (var call = new NativeCall()) {
            var id = call.str(inputId);
            return call.invoke(
                REGISTER_MEMTABLE,
                sessionPtr,
                id.segment(),
                id.len(),
                call.bytes(schemaIpc),
                (long) schemaIpc.length,
                call.longs(arrayPtrs),
                call.longs(schemaPtrs),
                (long) arrayPtrs.length
            );
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
     */
    public static SessionContextHandle createSessionContext(long readerPtr, long runtimePtr, String tableName, long contextId) {
        NativeHandle.validatePointer(readerPtr, "reader");
        NativeHandle.validatePointer(runtimePtr, "runtime");
        try (var call = new NativeCall()) {
            var table = call.str(tableName);
            long ptr = call.invoke(CREATE_SESSION_CONTEXT, readerPtr, runtimePtr, table.segment(), table.len(), contextId);
            return new SessionContextHandle(ptr);
        }
    }

    /**
     * Creates a SessionContext configured for indexed execution with filter delegation.
     * Registers the delegated_predicate UDF and stores treeShape + delegatedPredicateCount
     * on the Rust handle for use during execution.
     */
    public static SessionContextHandle createSessionContextForIndexedExecution(
        long readerPtr,
        long runtimePtr,
        String tableName,
        long contextId,
        int treeShapeOrdinal,
        int delegatedPredicateCount
    ) {
        NativeHandle.validatePointer(readerPtr, "reader");
        NativeHandle.validatePointer(runtimePtr, "runtime");
        try (NativeCall call = new NativeCall()) {
            NativeCall.Str table = call.str(tableName);
            long ptr = call.invoke(
                CREATE_SESSION_CONTEXT_INDEXED,
                readerPtr,
                runtimePtr,
                table.segment(),
                table.len(),
                contextId,
                treeShapeOrdinal,
                delegatedPredicateCount
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
            listener.onFailure(throwable instanceof Exception ? (Exception) throwable : new RuntimeException(throwable));
        }
    }

    public static void destroyCustomCacheManager(long ptr) {
        NativeCall.invokeVoid(DESTROY_CUSTOM_CACHE_MANAGER, ptr);
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

    public static void initLogger() {}
}

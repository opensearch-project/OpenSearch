/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

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
    private static final MethodHandle CREATE_READER;
    private static final MethodHandle CLOSE_READER;
    private static final MethodHandle EXECUTE_QUERY;
    private static final MethodHandle STREAM_GET_SCHEMA;
    private static final MethodHandle STREAM_NEXT;
    private static final MethodHandle STREAM_CLOSE;
    private static final MethodHandle SQL_TO_SUBSTRAIT;
    private static final MethodHandle CREATE_LOCAL_SESSION;
    private static final MethodHandle CLOSE_LOCAL_SESSION;
    private static final MethodHandle REGISTER_PARTITION_STREAM;
    private static final MethodHandle EXECUTE_LOCAL_PLAN;
    private static final MethodHandle SENDER_SEND;
    private static final MethodHandle SENDER_CLOSE;
    private static final MethodHandle REGISTER_MEMTABLE;

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
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        CLOSE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_close_global_runtime").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
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
    }

    private NativeBridge() {}

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
            return call.invoke(CREATE_GLOBAL_RUNTIME, memoryLimit, dir.segment(), dir.len(), spillLimit);
        }
    }

    /** Frees the native runtime. Safe to call once. */
    public static void closeGlobalRuntime(long ptr) {
        NativeCall.invokeVoid(CLOSE_GLOBAL_RUNTIME, ptr);
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
                contextId
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

    public static void cacheManagerAddFiles(long runtimePtr, String[] filePaths) {}

    public static void cacheManagerRemoveFiles(long runtimePtr, String[] filePaths) {}

    public static void initLogger() {}
}

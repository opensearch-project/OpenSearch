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
import org.opensearch.be.datafusion.NativeErrorConverter;
import org.opensearch.core.action.ActionListener;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Routes async native completions to per-call listeners. One static upcall stub,
 * registered once from {@link NativeBridge}'s static init via
 * {@code df_register_completion_callback}.
 *
 * <p>LISTENER CONTRACT: listeners registered here run ON A TOKIO IO-RUNTIME WORKER.
 * They MUST be O(1) and non-throwing — in practice, {@code CompletableFuture::complete}
 * wrappers. State transitions, allocator work, downstream feeds belong on the
 * Java side of the future, never here.
 */
public final class NativeCompletionCallbacks {

    private static final Logger LOGGER = LogManager.getLogger(NativeCompletionCallbacks.class);
    private static final ConcurrentHashMap<Long, ActionListener<Long>> PENDING = new ConcurrentHashMap<>();
    private static final AtomicLong IDS = new AtomicLong(1);

    static long register(ActionListener<Long> listener) {
        long id = IDS.getAndIncrement();
        PENDING.put(id, listener);
        return id;
    }

    /** Reclaim on initiation-failure or sync-outcome. Null if the completion raced us. */
    static ActionListener<Long> unregister(long callId) {
        return PENDING.remove(callId);
    }

    /** Test-only: number of listeners awaiting a completion. Used by leak probes. */
    public static int pendingCount() {
        return PENDING.size();
    }

    /**
     * FFM upcall: {@code int (long callId, long value, MemorySegment errPtr, long errLen)}.
     * A Throwable escaping an upcall stub crashes the JVM — catch everything.
     * Returns 1 iff Java accepted ownership of {@code value}.
     */
    static int onNativeComplete(long callId, long value, MemorySegment errPtr, long errLen) {
        try {
            ActionListener<Long> l = PENDING.remove(callId);
            if (l == null) {
                return 0; // query torn down; Rust reclaims
            }
            if (errLen > 0) {
                String msg = new String(errPtr.reinterpret(errLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
                l.onFailure(NativeErrorConverter.convert(new RuntimeException(msg)));
                return 1; // value is 0 on the error path
            }
            l.onResponse(value);
            return 1;
        } catch (Throwable t) {
            LOGGER.error("native completion listener threw (callId=" + callId + ")", t);
            return 0;
        }
    }

    private NativeCompletionCallbacks() {}
}

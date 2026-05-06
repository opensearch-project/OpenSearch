/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.analytics.spi.FilterDelegationHandle;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Static callback targets invoked by the native engine via FFM upcalls.
 *
 * <p>All calls delegate to the currently installed {@link FilterDelegationHandle}.
 * The handle is set per-query-per-shard before execution and cleared after.
 *
 * <h2>Error-handling contract</h2>
 * <p>Every method catches all {@link Throwable}s and returns {@code -1}
 * (or silently returns for void methods). A Java exception escaping through
 * an FFM upcall stub crashes the JVM.
 *
 * // TODO: remove old Registries-based code path and CollectorRegistry/FilterProviderRegistry
 * // once all tests are migrated to the FilterDelegationHandle path.
 */
public final class FilterTreeCallbacks {

    private static final Logger LOGGER = LogManager.getLogger(FilterTreeCallbacks.class);

    private static final AtomicReference<FilterDelegationHandle> HANDLE = new AtomicReference<>();

    private FilterTreeCallbacks() {}

    /**
     * Install the delegation handle for the current execution.
     * Called by {@code configureFilterDelegation} before query execution.
     * Tests may call with {@code null} to reset.
     */
    public static void setHandle(FilterDelegationHandle handle) {
        HANDLE.set(handle);
    }

    // ── Provider lifecycle (cold path, once per query) ────────────────

    /**
     * {@code createProvider(annotationId) -> providerKey|-1}.
     */
    public static int createProvider(int annotationId) {
        try {
            FilterDelegationHandle handle = HANDLE.get();
            if (handle == null) {
                return -1;
            }
            return handle.createProvider(annotationId);
        } catch (Throwable throwable) {
            LOGGER.error("createProvider failed for annotationId={}", annotationId, throwable);
            return -1;
        }
    }

    /**
     * {@code releaseProvider(providerKey)}. Never throws.
     */
    public static void releaseProvider(int providerKey) {
        try {
            FilterDelegationHandle handle = HANDLE.get();
            if (handle != null) {
                handle.releaseProvider(providerKey);
            }
        } catch (Throwable throwable) {
            LOGGER.error(new ParameterizedMessage("releaseProvider({}) failed", providerKey), throwable);
        }
    }

    // ── Collector lifecycle (hot path, per segment per query) ─────────

    /**
     * {@code createCollector(providerKey, segmentOrd, minDoc, maxDoc) -> collectorKey|-1}.
     */
    public static int createCollector(int providerKey, int segmentOrd, int minDoc, int maxDoc) {
        try {
            FilterDelegationHandle handle = HANDLE.get();
            if (handle == null) {
                return -1;
            }
            return handle.createCollector(providerKey, segmentOrd, minDoc, maxDoc);
        } catch (Throwable throwable) {
            LOGGER.error(
                new ParameterizedMessage(
                    "createCollector(providerKey={}, seg={}, [{}, {})) failed",
                    providerKey,
                    segmentOrd,
                    minDoc,
                    maxDoc
                ),
                throwable
            );
            return -1;
        }
    }

    /**
     * {@code collectDocs(collectorKey, minDoc, maxDoc, outPtr, outWordCap) -> wordsWritten|-1}.
     */
    public static long collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment outPtr, long outWordCap) {
        try {
            FilterDelegationHandle handle = HANDLE.get();
            if (handle == null) {
                return -1L;
            }
            int maxWords = (int) Math.min(outWordCap, (long) Integer.MAX_VALUE);
            MemorySegment view = outPtr.reinterpret((long) maxWords * Long.BYTES);
            int wordsWritten = handle.collectDocs(collectorKey, minDoc, maxDoc, view);
            return (wordsWritten < 0) ? -1L : wordsWritten;
        } catch (Throwable throwable) {
            LOGGER.error(new ParameterizedMessage("collectDocs(collectorKey={}, [{}, {})) failed", collectorKey, minDoc, maxDoc), throwable);
            return -1L;
        }
    }

    /**
     * {@code releaseCollector(collectorKey)}. Never throws.
     */
    public static void releaseCollector(int collectorKey) {
        try {
            FilterDelegationHandle handle = HANDLE.get();
            if (handle != null) {
                handle.releaseCollector(collectorKey);
            }
        } catch (Throwable throwable) {
            LOGGER.error(new ParameterizedMessage("releaseCollector({}) failed", collectorKey), throwable);
        }
    }
}

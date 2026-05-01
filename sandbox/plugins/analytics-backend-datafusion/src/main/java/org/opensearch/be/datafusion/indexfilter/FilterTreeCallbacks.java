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
import org.opensearch.be.datafusion.indexfilter.CollectorRegistry.CollectorHandle;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Static callback targets invoked by the native engine via FFM upcalls.
 *
 * <p>Delegates to two instance-based registries installed at plugin startup:
 * <ul>
 *   <li>{@link FilterProviderRegistry} — query-level provider create/release (cold path)</li>
 *   <li>{@link CollectorRegistry} — per-segment collector create/collect/release (hot path)</li>
 * </ul>
 *
 * <h2>Error-handling contract</h2>
 * <p>Every method catches all {@link Throwable}s and returns {@code -1}
 * (or silently returns for void methods). A Java exception escaping through
 * an FFM upcall stub crashes the JVM.
 */
public final class FilterTreeCallbacks {

    private static final Logger LOGGER = LogManager.getLogger(FilterTreeCallbacks.class);

    /** Both registries in one snapshot — one AtomicReference read per upcall. */
    record Registries(FilterProviderRegistry providers, CollectorRegistry collectors) {
    }

    private static final AtomicReference<Registries> REGISTRIES = new AtomicReference<>();

    private FilterTreeCallbacks() {}

    /**
     * Install the registries. Called once at plugin startup.
     * Tests may call with {@code null} to reset.
     */
    public static void setRegistries(FilterProviderRegistry providers, CollectorRegistry collectors) {
        REGISTRIES.set(providers == null ? null : new Registries(providers, collectors));
    }

    // ── Provider lifecycle (cold path, once per query) ────────────────

    /**
     * {@code createProvider(queryBytes, queryBytesLen) -> providerKey|-1}.
     */
    public static int createProvider(MemorySegment queryBytesPtr, long queryBytesLen) {
        try {
            Registries reg = REGISTRIES.get();
            if (reg == null) {
                return -1;
            }
            MemorySegment view = queryBytesPtr.reinterpret(queryBytesLen);
            byte[] bytes = view.toArray(ValueLayout.JAVA_BYTE);
            return reg.providers().createProvider(bytes);
        } catch (Throwable t) {
            LOGGER.error("createProvider failed", t);
            return -1;
        }
    }

    /**
     * {@code releaseProvider(providerKey)}. Never throws.
     */
    public static void releaseProvider(int providerKey) {
        try {
            Registries reg = REGISTRIES.get();
            if (reg != null) {
                reg.providers().releaseProvider(providerKey);
            }
        } catch (Throwable t) {
            LOGGER.error("releaseProvider({}) failed", providerKey, t);
        }
    }

    // ── Collector lifecycle (hot path, per segment per query) ─────────

    /**
     * {@code createCollector(providerKey, segmentOrd, minDoc, maxDoc) -> collectorKey|-1}.
     */
    public static int createCollector(int providerKey, int segmentOrd, int minDoc, int maxDoc) {
        try {
            Registries reg = REGISTRIES.get();
            if (reg == null) {
                return -1;
            }
            return reg.providers().createCollector(providerKey, segmentOrd, minDoc, maxDoc);
        } catch (Throwable t) {
            LOGGER.error("createCollector(providerKey={}, seg={}, [{}, {})) failed", providerKey, segmentOrd, minDoc, maxDoc, t);
            return -1;
        }
    }

    /**
     * {@code collectDocs(collectorKey, minDoc, maxDoc, outPtr, outWordCap) -> wordsWritten|-1}.
     *
     * <p>Single map lookup into {@link CollectorRegistry}. The provider
     * reference is already captured in the {@link CollectorHandle}.
     */
    public static long collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment outPtr, long outWordCap) {
        try {
            Registries reg = REGISTRIES.get();
            if (reg == null) {
                return -1L;
            }
            CollectorHandle handle = reg.collectors().collector(collectorKey);
            if (handle == null) {
                return -1L;
            }
            int maxWords = (int) Math.min(outWordCap, (long) Integer.MAX_VALUE);
            MemorySegment view = outPtr.reinterpret((long) maxWords * Long.BYTES);
            int n = handle.provider().collectDocs(handle.innerCollectorKey(), minDoc, maxDoc, view);
            return (n < 0) ? -1L : n;
        } catch (Throwable t) {
            LOGGER.error("collectDocs(collectorKey={}, [{}, {})) failed", collectorKey, minDoc, maxDoc, t);
            return -1L;
        }
    }

    /**
     * {@code releaseCollector(collectorKey)}. Never throws.
     */
    public static void releaseCollector(int collectorKey) {
        try {
            Registries reg = REGISTRIES.get();
            if (reg == null) {
                return;
            }
            CollectorHandle handle = reg.collectors().collector(collectorKey);
            if (handle == null) {
                return;
            }
            handle.provider().releaseCollector(handle.innerCollectorKey());
            reg.collectors().unregisterCollector(collectorKey);
        } catch (Throwable t) {
            LOGGER.error("releaseCollector({}) failed", collectorKey, t);
        }
    }
}

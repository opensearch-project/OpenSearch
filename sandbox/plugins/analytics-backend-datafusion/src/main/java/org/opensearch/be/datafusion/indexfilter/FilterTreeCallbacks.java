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
import org.opensearch.analytics.spi.IndexFilterProvider;
import org.opensearch.analytics.spi.IndexFilterProviderFactory;
import org.opensearch.be.datafusion.indexfilter.ProviderRegistry.CollectorHandle;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Static callback targets invoked by the native engine via FFM upcalls.
 *
 * <h2>Error-handling contract</h2>
 * <p>Every method MUST catch all {@link Throwable}s and report failure via the
 * method's return value ({@code -1} for int/long-returning methods, a log line
 * plus silent return for void-returning methods). A Java exception escaping
 * through an FFM upcall stub into native code crashes the JVM immediately —
 * the panic unwinds into Rust, which is undefined behaviour across the ABI —
 * so we trade exception propagation for explicit error codes at the boundary.
 *
 * <p>Errors are always logged before being converted to {@code -1}, so the
 * root cause is recoverable from server logs even though it is opaque to Rust.
 *
 * TODO : replace with delegate calls here
 */
public final class FilterTreeCallbacks {

    private static final Logger LOGGER = LogManager.getLogger(FilterTreeCallbacks.class);

    private FilterTreeCallbacks() {}

    /**
     * {@code createProvider(queryBytes, queryBytesLen) -> providerKey|-1}.
     *
     * Calls the registered {@link IndexFilterProviderFactory} to build a
     * provider from the opaque query payload, registers it, returns the
     * assigned key. Rust holds the key for the duration of the query.
     */
    public static int createProvider(MemorySegment queryBytesPtr, long queryBytesLen) {
        try {
            IndexFilterProviderFactory factory = ProviderRegistry.factory();
            if (factory == null) {
                return -1;
            }
            MemorySegment view = queryBytesPtr.reinterpret(queryBytesLen);
            byte[] bytes = view.toArray(ValueLayout.JAVA_BYTE);
            IndexFilterProvider provider = factory.create(bytes);
            if (provider == null) {
                return -1;
            }
            return ProviderRegistry.registerProvider(provider);
        } catch (Throwable t) {
            LOGGER.error("createProvider failed", t);
            return -1;
        }
    }

    /**
     * {@code releaseProvider(providerKey)}. Closes the provider and removes it
     * from the registry. Never throws.
     */
    public static void releaseProvider(int providerKey) {
        try {
            IndexFilterProvider provider = ProviderRegistry.unregisterProvider(providerKey);
            if (provider != null) {
                provider.close();
            }
        } catch (Throwable t) {
            LOGGER.error("releaseProvider({}) failed", providerKey, t);
        }
    }

    /**
     * {@code createCollector(providerKey, segmentOrd, minDoc, maxDoc) -> collectorKey|-1}.
     */
    public static int createCollector(int providerKey, int segmentOrd, int minDoc, int maxDoc) {
        try {
            IndexFilterProvider provider = ProviderRegistry.provider(providerKey);
            if (provider == null) {
                return -1;
            }
            int inner = provider.createCollector(segmentOrd, minDoc, maxDoc);
            if (inner < 0) {
                return -1;
            }
            return ProviderRegistry.registerCollector(providerKey, inner);
        } catch (Throwable t) {
            LOGGER.error("createCollector(providerKey={}, seg={}, [{}, {})) failed", providerKey, segmentOrd, minDoc, maxDoc, t);
            return -1;
        }
    }

    /**
     * {@code collectDocs(collectorKey, minDoc, maxDoc, outPtr, outWordCap) -> wordsWritten|-1}.
     *
     * <p>{@code outPtr} points at Rust-allocated memory of {@code outWordCap}
     * 64-bit words. We allocate a {@code long[]} the same size, let the
     * provider fill it, then copy into the native buffer. The SPI uses plain
     * {@code long[]} so backend implementers don't have to depend on JDK 22+
     * FFM APIs; the extra allocation + copy is the price of that portability.
     */
    public static long collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment outPtr, long outWordCap) {
        try {
            CollectorHandle handle = ProviderRegistry.collector(collectorKey);
            if (handle == null) {
                return -1L;
            }
            IndexFilterProvider provider = ProviderRegistry.provider(handle.providerKey());
            if (provider == null) {
                return -1L;
            }
            int maxWords = (int) Math.min(outWordCap, (long) Integer.MAX_VALUE);
            long[] buf = new long[maxWords];
            int n = provider.collectDocs(handle.innerCollectorKey(), minDoc, maxDoc, buf);
            if (n < 0) {
                return -1L;
            }
            MemorySegment view = outPtr.reinterpret((long) maxWords * Long.BYTES);
            for (int i = 0; i < n; i++) {
                view.setAtIndex(ValueLayout.JAVA_LONG, i, buf[i]);
            }
            return n;
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
            CollectorHandle handle = ProviderRegistry.collector(collectorKey);
            if (handle == null) {
                return;
            }
            IndexFilterProvider provider = ProviderRegistry.provider(handle.providerKey());
            if (provider != null) {
                provider.releaseCollector(handle.innerCollectorKey());
            }
            ProviderRegistry.unregisterCollector(collectorKey);
        } catch (Throwable t) {
            LOGGER.error("releaseCollector({}) failed", collectorKey, t);
        }
    }
}

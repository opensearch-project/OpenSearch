/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Static JNI callback registry for Substrait-driven tree-based index filtering.
 * <p>
 * During boolean tree query execution, the native (Rust) layer calls back into
 * Java to create providers, create collectors, collect doc IDs, and release
 * resources. This bridge routes those callbacks to the correct
 * {@link IndexFilterCollectorProvider} by resolving the column name to a
 * {@link DataFormat} and then to the registered provider for that format.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>{@link #createContext} — allocate a context with column and provider mappings</li>
 *   <li>Rust calls static JNI callback methods during execution</li>
 *   <li>{@link #unregister} — remove context and all associated state</li>
 * </ol>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class IndexFilterBridge {

    private IndexFilterBridge() {}

    private static final AtomicLong nextContextId = new AtomicLong(1);

    private static final ConcurrentHashMap<Long, BridgeContext> activeContexts = new ConcurrentHashMap<>();

    /**
     * Internal context holding column-to-format mappings, provider registry,
     * and active provider/collector tracking.
     */
    static final class BridgeContext {
        final Map<String, DataFormat> columnToFormat;
        final Map<DataFormat, IndexFilterCollectorProvider> providers;
        final ConcurrentHashMap<Integer, ProviderEntry> activeProviders = new ConcurrentHashMap<>();
        final ConcurrentHashMap<Integer, CollectorEntry> activeCollectors = new ConcurrentHashMap<>();
        final AtomicInteger nextProviderKey = new AtomicInteger(1);
        final AtomicInteger nextCollectorKey = new AtomicInteger(1);

        BridgeContext(Map<String, DataFormat> columnToFormat, Map<DataFormat, IndexFilterCollectorProvider> providers) {
            this.columnToFormat = columnToFormat;
            this.providers = providers;
        }
    }

    /**
     * Tracks which provider created a given providerKey.
     */
    static final class ProviderEntry {
        final IndexFilterCollectorProvider provider;
        final int internalProviderKey;

        ProviderEntry(IndexFilterCollectorProvider provider, int internalProviderKey) {
            this.provider = provider;
            this.internalProviderKey = internalProviderKey;
        }
    }

    /**
     * Tracks which provider owns a given collectorKey.
     */
    static final class CollectorEntry {
        final IndexFilterCollectorProvider provider;
        final int internalCollectorKey;

        CollectorEntry(IndexFilterCollectorProvider provider, int internalCollectorKey) {
            this.provider = provider;
            this.internalCollectorKey = internalCollectorKey;
        }
    }

    // ── Registration API (called by IndexFilterDelegate) ────────────

    /**
     * Creates a new context with column-to-format and format-to-provider mappings.
     *
     * @param columnToFormat mapping from column name to owning DataFormat
     * @param providers      mapping from DataFormat to provider implementation
     * @return a unique context ID for use in JNI callbacks
     */
    public static long createContext(
        Map<String, DataFormat> columnToFormat,
        Map<DataFormat, IndexFilterCollectorProvider> providers
    ) {
        long id = nextContextId.getAndIncrement();
        activeContexts.put(id, new BridgeContext(columnToFormat, providers));
        return id;
    }

    /**
     * Unregisters a context and removes all associated state.
     *
     * @param contextId the context ID from {@link #createContext}
     */
    public static void unregister(long contextId) {
        activeContexts.remove(contextId);
    }

    // ── JNI callback methods (called from Rust) ─────────────────────

    /**
     * Creates a provider for a (column, value) pair. Looks up the DataFormat
     * for the column, routes to the correct provider, serializes (column:value)
     * as query bytes, and calls {@code provider.createProvider(queryBytes)}.
     *
     * @param contextId the registered context ID
     * @param column    the column name
     * @param value     the filter value
     * @return providerKey, or -1 if column not found or context not found
     */
    public static int createProvider(long contextId, String column, String value) {
        BridgeContext ctx = activeContexts.get(contextId);
        if (ctx == null) {
            return -1;
        }

        DataFormat format = ctx.columnToFormat.get(column);
        if (format == null) {
            return -1;
        }

        IndexFilterCollectorProvider provider = ctx.providers.get(format);
        if (provider == null) {
            return -1;
        }

        try {
            byte[] queryBytes = serializeQuery(column, value);
            int internalKey = provider.createProvider(queryBytes);
            int providerKey = ctx.nextProviderKey.getAndIncrement();
            ctx.activeProviders.put(providerKey, new ProviderEntry(provider, internalKey));
            return providerKey;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * Creates a per-segment collector for the given provider.
     *
     * @param contextId   the registered context ID
     * @param providerKey the provider key from {@link #createProvider}
     * @param segmentOrd  segment ordinal
     * @param minDoc      inclusive lower bound
     * @param maxDoc      exclusive upper bound
     * @return collectorKey, or -1 on failure
     */
    public static int createCollector(long contextId, int providerKey, int segmentOrd, int minDoc, int maxDoc) {
        BridgeContext ctx = activeContexts.get(contextId);
        if (ctx == null) {
            return -1;
        }

        ProviderEntry entry = ctx.activeProviders.get(providerKey);
        if (entry == null) {
            return -1;
        }

        try {
            int internalCollectorKey = entry.provider.createCollector(entry.internalProviderKey, segmentOrd, minDoc, maxDoc);
            int collectorKey = ctx.nextCollectorKey.getAndIncrement();
            ctx.activeCollectors.put(collectorKey, new CollectorEntry(entry.provider, internalCollectorKey));
            return collectorKey;
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Collects matching doc IDs for the given collector.
     *
     * @param contextId    the registered context ID
     * @param collectorKey the collector key from {@link #createCollector}
     * @param minDoc       inclusive lower bound
     * @param maxDoc       exclusive upper bound
     * @return packed long[] bitset of matching doc IDs, or empty array on failure
     */
    public static long[] collectDocs(long contextId, int collectorKey, int minDoc, int maxDoc) {
        BridgeContext ctx = activeContexts.get(contextId);
        if (ctx == null) {
            return new long[0];
        }

        CollectorEntry entry = ctx.activeCollectors.get(collectorKey);
        if (entry == null) {
            return new long[0];
        }

        try {
            return entry.provider.collectDocs(entry.internalCollectorKey, minDoc, maxDoc);
        } catch (Exception e) {
            return new long[0];
        }
    }

    /**
     * Releases a per-segment collector.
     *
     * @param contextId    the registered context ID
     * @param collectorKey the collector key from {@link #createCollector}
     */
    public static void releaseCollector(long contextId, int collectorKey) {
        BridgeContext ctx = activeContexts.get(contextId);
        if (ctx == null) {
            return;
        }

        CollectorEntry entry = ctx.activeCollectors.remove(collectorKey);
        if (entry == null) {
            return;
        }

        entry.provider.releaseCollector(entry.internalCollectorKey);
    }

    /**
     * Releases a provider instance and all its remaining collectors.
     *
     * @param contextId   the registered context ID
     * @param providerKey the provider key from {@link #createProvider}
     */
    public static void releaseProvider(long contextId, int providerKey) {
        BridgeContext ctx = activeContexts.get(contextId);
        if (ctx == null) {
            return;
        }

        ProviderEntry entry = ctx.activeProviders.remove(providerKey);
        if (entry == null) {
            return;
        }

        entry.provider.releaseProvider(entry.internalProviderKey);
    }

    // ── Internal ────────────────────────────────────────────────────

    /**
     * Serializes a (column, value) pair to query bytes using UTF-8 encoding.
     * Format: column + ":" + value
     */
    static byte[] serializeQuery(String column, String value) {
        return (column + ":" + value).getBytes(StandardCharsets.UTF_8);
    }
}

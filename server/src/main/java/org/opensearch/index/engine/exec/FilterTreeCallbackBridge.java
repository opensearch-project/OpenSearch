/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Static JNI callback registry for tree-based index filtering.
 * <p>
 * During boolean tree query execution, the native (Rust) layer calls
 * back into Java to create collectors, collect doc IDs, and release
 * collectors for each collector leaf in the tree. This bridge routes
 * those callbacks to the correct {@link IndexFilterTreeProvider}
 * using a {@code (contextId, providerId)} key.
 * <p>
 * A single context can hold multiple providers (one per data format).
 * For example, a tree with Lucene collector leaves and Tantivy collector
 * leaves would have two providers registered under the same contextId.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>{@link #createContext()} — allocate a unique contextId</li>
 *   <li>{@link #registerProvider} — register one provider per format</li>
 *   <li>Rust executes tree, calling back via static JNI methods</li>
 *   <li>{@link #unregister} — remove all providers for the context</li>
 * </ol>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FilterTreeCallbackBridge {

    private FilterTreeCallbackBridge() {}

    private static final AtomicLong nextContextId = new AtomicLong(1);

    private static final Map<Long, Map<Integer, RegisteredProvider>> activeContexts = new ConcurrentHashMap<>();

    @SuppressWarnings("rawtypes")
    private static final class RegisteredProvider {
        final IndexFilterTreeProvider provider;
        final IndexFilterTreeContext context;

        RegisteredProvider(IndexFilterTreeProvider provider, IndexFilterTreeContext context) {
            this.provider = provider;
            this.context = context;
        }
    }

    // ── Registration API (called by DefaultPlanExecutor) ────────────

    /**
     * Creates a new context and returns a unique contextId.
     * The contextId must be passed to Rust for JNI callbacks.
     *
     * @return a unique context ID
     */
    public static long createContext() {
        long id = nextContextId.getAndIncrement();
        activeContexts.put(id, new ConcurrentHashMap<>());
        return id;
    }

    /**
     * Registers a provider under an existing contextId.
     *
     * @param contextId  the context ID from {@link #createContext()}
     * @param providerId the provider identifier (matches CollectorLeaf.providerId in the tree)
     * @param provider   the tree filter provider
     * @param context    the tree context holding per-leaf filter contexts
     */
    @SuppressWarnings("rawtypes")
    public static void registerProvider(
        long contextId,
        int providerId,
        IndexFilterTreeProvider provider,
        IndexFilterTreeContext context
    ) {
        Map<Integer, RegisteredProvider> providers = activeContexts.get(contextId);
        if (providers == null) {
            throw new IllegalStateException("contextId " + contextId + " not found; call createContext() first");
        }
        providers.put(providerId, new RegisteredProvider(provider, context));
    }

    /**
     * Unregisters all providers for a contextId. Called when the tree
     * query completes (success or failure).
     *
     * @param contextId the context ID to unregister
     */
    public static void unregister(long contextId) {
        activeContexts.remove(contextId);
    }

    // ── JNI callback methods (called from Rust) ─────────────────────

    /**
     * Returns the number of segments for the given collector leaf.
     *
     * @param contextId  the registered context ID
     * @param providerId the provider identifier
     * @param leafIndex  the collector leaf ordinal within this provider
     * @return segment count, or -1 if context/provider not found
     */
    public static int getSegmentCount(long contextId, int providerId, int leafIndex) {
        RegisteredProvider rp = lookupProvider(contextId, providerId);
        if (rp == null) {
            return -1;
        }
        return rp.context.leafContext(leafIndex).segmentCount();
    }

    /**
     * Returns the max doc count for a segment of the given collector leaf.
     *
     * @param contextId  the registered context ID
     * @param providerId the provider identifier
     * @param leafIndex  the collector leaf ordinal
     * @param segmentOrd the segment ordinal
     * @return max doc count, or -1 if context/provider not found
     */
    public static int getSegmentMaxDoc(long contextId, int providerId, int leafIndex, int segmentOrd) {
        RegisteredProvider rp = lookupProvider(contextId, providerId);
        if (rp == null) {
            return -1;
        }
        return rp.context.leafContext(leafIndex).segmentMaxDoc(segmentOrd);
    }

    /**
     * Creates a collector for a specific collector leaf and segment.
     *
     * @param contextId  the registered context ID
     * @param providerId the provider identifier
     * @param leafIndex  the collector leaf ordinal
     * @param segmentOrd the segment ordinal
     * @param minDoc     inclusive lower bound
     * @param maxDoc     exclusive upper bound
     * @return collector key for subsequent collectDocs/releaseCollector calls, or -1 on failure
     */
    @SuppressWarnings("unchecked")
    public static int createCollector(long contextId, int providerId, int leafIndex, int segmentOrd, int minDoc, int maxDoc) {
        RegisteredProvider rp = lookupProvider(contextId, providerId);
        if (rp == null) {
            return -1;
        }
        return rp.provider.createCollector(rp.context, leafIndex, segmentOrd, minDoc, maxDoc);
    }

    /**
     * Collects matching doc IDs for the given collector.
     *
     * @param contextId    the registered context ID
     * @param providerId   the provider identifier
     * @param leafIndex    the collector leaf ordinal
     * @param collectorKey the collector key from createCollector
     * @param minDoc       inclusive lower bound
     * @param maxDoc       exclusive upper bound
     * @return packed long[] bitset of matching doc IDs, or empty array if context/provider not found
     */
    @SuppressWarnings("unchecked")
    public static long[] collectDocs(long contextId, int providerId, int leafIndex, int collectorKey, int minDoc, int maxDoc) {
        RegisteredProvider rp = lookupProvider(contextId, providerId);
        if (rp == null) {
            return new long[0];
        }
        return rp.provider.collectDocs(rp.context, leafIndex, collectorKey, minDoc, maxDoc);
    }

    /**
     * Releases a collector.
     *
     * @param contextId    the registered context ID
     * @param providerId   the provider identifier
     * @param leafIndex    the collector leaf ordinal
     * @param collectorKey the collector key from createCollector
     */
    @SuppressWarnings("unchecked")
    public static void releaseCollector(long contextId, int providerId, int leafIndex, int collectorKey) {
        RegisteredProvider rp = lookupProvider(contextId, providerId);
        if (rp == null) {
            return;
        }
        rp.provider.releaseCollector(rp.context, leafIndex, collectorKey);
    }

    // ── Internal ────────────────────────────────────────────────────

    private static RegisteredProvider lookupProvider(long contextId, int providerId) {
        Map<Integer, RegisteredProvider> providers = activeContexts.get(contextId);
        if (providers == null) {
            return null;
        }
        return providers.get(providerId);
    }
}

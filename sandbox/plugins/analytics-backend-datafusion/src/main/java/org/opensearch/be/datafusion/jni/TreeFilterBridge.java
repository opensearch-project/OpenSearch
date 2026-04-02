/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.jni;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.IndexFilterTreeContext;
import org.opensearch.index.engine.exec.IndexFilterTreeProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JNI callback bridge for tree-based index filtering.
 * <p>
 * @deprecated Use {@link org.opensearch.index.engine.exec.FilterTreeCallbackBridge} in core instead.
 * This class is retained temporarily for backward compatibility during migration.
 *
 * @opensearch.experimental
 */
@Deprecated
@ExperimentalApi
public final class TreeFilterBridge {

    private TreeFilterBridge() {}

    private static final AtomicLong nextContextId = new AtomicLong(1);

    @SuppressWarnings("rawtypes")
    private static final Map<Long, RegisteredContext> activeContexts = new ConcurrentHashMap<>();

    @SuppressWarnings("rawtypes")
    private static final class RegisteredContext {
        final IndexFilterTreeProvider provider;
        final IndexFilterTreeContext context;

        @SuppressWarnings("unchecked")
        RegisteredContext(IndexFilterTreeProvider provider, IndexFilterTreeContext context) {
            this.provider = provider;
            this.context = context;
        }
    }

    /**
     * Registers a tree context for JNI callbacks.
     *
     * @param provider the tree filter provider
     * @param context the tree context holding per-leaf contexts
     * @return a context ID that Rust must pass back on every callback
     */
    @SuppressWarnings("rawtypes")
    public static long register(IndexFilterTreeProvider provider, IndexFilterTreeContext context) {
        long id = nextContextId.getAndIncrement();
        activeContexts.put(id, new RegisteredContext(provider, context));
        return id;
    }

    /**
     * Unregisters a tree context. Called when the tree query completes.
     *
     * @param contextId the context ID returned by {@link #register}
     */
    public static void unregister(long contextId) {
        activeContexts.remove(contextId);
    }

    // ── JNI callback methods (called from Rust) ─────────────────────

    /**
     * Returns the number of segments for the given index leaf.
     * Called by Rust to determine segment count per FTS leaf.
     *
     * @param contextId the registered context ID
     * @param leafIndex the index leaf ordinal
     * @return segment count, or -1 if context not found
     */
    public static int getSegmentCount(long contextId, int leafIndex) {
        RegisteredContext rc = activeContexts.get(contextId);
        if (rc == null) {
            return -1;
        }
        return rc.context.leafContext(leafIndex).segmentCount();
    }

    /**
     * Returns the max doc count for a segment of the given index leaf.
     *
     * @param contextId the registered context ID
     * @param leafIndex the index leaf ordinal
     * @param segmentOrd the segment ordinal
     * @return max doc count, or -1 if context not found
     */
    public static int getSegmentMaxDoc(long contextId, int leafIndex, int segmentOrd) {
        RegisteredContext rc = activeContexts.get(contextId);
        if (rc == null) {
            return -1;
        }
        return rc.context.leafContext(leafIndex).segmentMaxDoc(segmentOrd);
    }

    /**
     * Creates a collector for a specific index leaf and segment.
     *
     * @param contextId the registered context ID
     * @param leafIndex the index leaf ordinal
     * @param segmentOrd the segment ordinal
     * @param minDoc inclusive lower bound
     * @param maxDoc exclusive upper bound
     * @return collector key for subsequent collectDocs/releaseCollector calls
     */
    @SuppressWarnings("unchecked")
    public static int createCollector(long contextId, int leafIndex, int segmentOrd, int minDoc, int maxDoc) {
        RegisteredContext rc = activeContexts.get(contextId);
        if (rc == null) {
            return -1;
        }
        return rc.provider.createCollector(rc.context, leafIndex, segmentOrd, minDoc, maxDoc);
    }

    /**
     * Collects matching doc IDs for the given collector.
     *
     * @param contextId the registered context ID
     * @param leafIndex the index leaf ordinal
     * @param collectorKey the collector key from createCollector
     * @param minDoc inclusive lower bound
     * @param maxDoc exclusive upper bound
     * @return packed long[] bitset of matching doc IDs
     */
    @SuppressWarnings("unchecked")
    public static long[] collectDocs(long contextId, int leafIndex, int collectorKey, int minDoc, int maxDoc) {
        RegisteredContext rc = activeContexts.get(contextId);
        if (rc == null) {
            return new long[0];
        }
        return rc.provider.collectDocs(rc.context, leafIndex, collectorKey, minDoc, maxDoc);
    }

    /**
     * Releases a collector.
     *
     * @param contextId the registered context ID
     * @param leafIndex the index leaf ordinal
     * @param collectorKey the collector key from createCollector
     */
    @SuppressWarnings("unchecked")
    public static void releaseCollector(long contextId, int leafIndex, int collectorKey) {
        RegisteredContext rc = activeContexts.get(contextId);
        if (rc == null) {
            return;
        }
        rc.provider.releaseCollector(rc.context, leafIndex, collectorKey);
    }
}

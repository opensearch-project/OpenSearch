/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.IndexFilterTree;

import java.io.Closeable;
import java.io.IOException;

/**
 * Provides tree-based index filtering for boolean queries with
 * multiple collector leaves.
 * <p>
 * Composes multiple {@link IndexFilterProvider} instances, one per
 * collector leaf in the {@link IndexFilterTree}. Each leaf gets its own
 * context and collectors, while the tree structure and predicate leaves
 * are evaluated on the native (Rust) side.
 *
 * @param <Q>       the query type (e.g. Lucene Query)
 * @param <C>       the per-leaf context type
 * @param <ReaderT> the engine-specific reader type
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexFilterTreeProvider<Q, C extends IndexFilterContext, ReaderT> extends Closeable {

    /**
     * Creates a tree context that holds per-leaf filter contexts.
     *
     * @param queries the queries for each collector leaf (indexed by collectorIndex)
     * @param reader  the engine-specific reader
     * @param tree    the boolean filter tree
     * @return a tree context managing all per-leaf contexts
     * @throws IOException if context creation fails
     */
    IndexFilterTreeContext<C> createTreeContext(Q[] queries, ReaderT reader, IndexFilterTree tree) throws IOException;

    /**
     * Creates a collector for a specific collector leaf and segment.
     *
     * @param treeContext the tree context
     * @param leafIndex   the collector leaf ordinal (collectorIndex in the tree)
     * @param segmentOrd  the segment ordinal
     * @param minDoc      inclusive lower bound
     * @param maxDoc      exclusive upper bound
     * @return a collector key for subsequent collectDocs/releaseCollector calls
     */
    int createCollector(IndexFilterTreeContext<C> treeContext, int leafIndex, int segmentOrd, int minDoc, int maxDoc);

    /**
     * Collects matching doc IDs for the given collector.
     *
     * @param treeContext  the tree context
     * @param leafIndex    the collector leaf ordinal
     * @param collectorKey the collector key from {@link #createCollector}
     * @param minDoc       inclusive lower bound
     * @param maxDoc       exclusive upper bound
     * @return packed long[] bitset of matching doc IDs
     */
    long[] collectDocs(IndexFilterTreeContext<C> treeContext, int leafIndex, int collectorKey, int minDoc, int maxDoc);

    /**
     * Releases a collector previously created by {@link #createCollector}.
     *
     * @param treeContext  the tree context
     * @param leafIndex    the collector leaf ordinal
     * @param collectorKey the collector key from {@link #createCollector}
     */
    void releaseCollector(IndexFilterTreeContext<C> treeContext, int leafIndex, int collectorKey);
}

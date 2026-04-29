/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.index.engine.exec.IndexFilterTreeContext;
import org.opensearch.index.engine.exec.IndexFilterTreeProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lucene-backed {@link IndexFilterTreeProvider}.
 * <p>
 * Creates one {@link LuceneIndexFilterContext} per collector leaf in the
 * boolean tree. Each leaf context holds its own Lucene Weight and
 * manages per-segment collectors independently.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneIndexFilterTreeProvider implements IndexFilterTreeProvider<Query, LuceneIndexFilterContext, DirectoryReader> {

    private final LuceneIndexFilterProvider delegate;

    /** Creates a new LuceneIndexFilterTreeProvider. */
    public LuceneIndexFilterTreeProvider() {
        this.delegate = new LuceneIndexFilterProvider();
    }

    /**
     * Creates a new LuceneIndexFilterTreeProvider with an existing provider.
     *
     * @param delegate the single-query provider to delegate collector operations to
     */
    public LuceneIndexFilterTreeProvider(LuceneIndexFilterProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public IndexFilterTreeContext<LuceneIndexFilterContext> createTreeContext(Query[] queries, DirectoryReader reader, IndexFilterTree tree)
        throws IOException {
        List<LuceneIndexFilterContext> leafContexts = new ArrayList<>(queries.length);
        try {
            for (Query query : queries) {
                leafContexts.add(delegate.createContext(query, reader));
            }
        } catch (IOException e) {
            // Clean up any contexts already created
            for (LuceneIndexFilterContext ctx : leafContexts) {
                ctx.close();
            }
            throw e;
        }
        return new IndexFilterTreeContext<>(tree, leafContexts);
    }

    @Override
    public int createCollector(
        IndexFilterTreeContext<LuceneIndexFilterContext> treeContext,
        int leafIndex,
        int segmentOrd,
        int minDoc,
        int maxDoc
    ) {
        return delegate.createCollector(treeContext.leafContext(leafIndex), segmentOrd, minDoc, maxDoc);
    }

    @Override
    public long[] collectDocs(
        IndexFilterTreeContext<LuceneIndexFilterContext> treeContext,
        int leafIndex,
        int collectorKey,
        int minDoc,
        int maxDoc
    ) {
        return delegate.collectDocs(treeContext.leafContext(leafIndex), collectorKey, minDoc, maxDoc);
    }

    @Override
    public void releaseCollector(IndexFilterTreeContext<LuceneIndexFilterContext> treeContext, int leafIndex, int collectorKey) {
        delegate.releaseCollector(treeContext.leafContext(leafIndex), collectorKey);
    }

    @Override
    public void close() {
        delegate.close();
    }
}

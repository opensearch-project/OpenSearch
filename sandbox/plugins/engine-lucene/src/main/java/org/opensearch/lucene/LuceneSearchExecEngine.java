/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.engine.EngineReaderManager;
import org.opensearch.index.engine.EngineSearcherSupplier;
import org.opensearch.index.engine.LuceneReaderManager;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * Lucene-backed {@link SearchExecEngine}.
 * Plan type is {@link Query}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchExecEngine implements SearchExecEngine<LuceneSearchContext, Query> {

    private final LuceneReaderManager readerManager;
    private long nextContextId;

    public LuceneSearchExecEngine(ReferenceManager<? extends DirectoryReader> referenceManager) {
        this.readerManager = new LuceneReaderManager(referenceManager);
    }

    @Override
    public void execute(LuceneSearchContext context) throws IOException {
        LuceneEngineSearcher searcher = (LuceneEngineSearcher) acquireSearcher("lucene-exec");
        try {
            searcher.search(context);
        } finally {
            searcher.close();
        }
    }

    @Override
    public LuceneSearchContext createContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        SearchShardTask task,
        BigArrays bigArrays
    ) throws IOException {
        return new LuceneSearchContext(new ShardSearchContextId("", nextContextId++), request, shardTarget);
    }

    @Override
    public Query convertFragment(Object fragment) {
        // DQE passes a Lucene Query directly
        if (fragment instanceof Query) {
            return (Query) fragment;
        }
        throw new UnsupportedOperationException("Expected Lucene Query, got " + fragment.getClass().getSimpleName());
    }

    @Override
    public EngineSearcherSupplier<LuceneEngineSearcher> acquireSearcherSupplier() throws IOException {
        DirectoryReader reader = readerManager.acquire();
        return new EngineSearcherSupplier<>() {
            @Override
            protected LuceneEngineSearcher acquireSearcherInternal(String source) {
                return new LuceneEngineSearcher(source, new IndexSearcher(reader), reader);
            }

            @Override
            protected void doClose() {
                try {
                    readerManager.release(reader);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }
        };
    }

    @Override
    public EngineReaderManager<?> getReferenceManager() {
        return readerManager;
    }
}

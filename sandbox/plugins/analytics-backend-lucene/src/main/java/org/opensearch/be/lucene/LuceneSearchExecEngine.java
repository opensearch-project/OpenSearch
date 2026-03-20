/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * Lucene-backed search execution engine.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchExecEngine implements SearchExecEngine<LuceneSearchContext, Query, Void> {

    @Override
    public Query convertFragment(Object fragment) {
        if (fragment instanceof Query) {
            return (Query) fragment;
        }
        throw new UnsupportedOperationException("Expected Lucene Query, got " + fragment.getClass().getSimpleName());
    }

    @Override
    public LuceneSearchContext createContext(
        Object reader,
        Query plan,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        SearchShardTask task
    ) throws IOException {
        DirectoryReader directoryReader = (DirectoryReader) reader;
        return new LuceneSearchContext(request, shardTarget, directoryReader);
    }

    @Override
    public Void execute(LuceneSearchContext context) throws IOException {
        DirectoryReader reader = context.getReader();
        LuceneEngineSearcher searcher = new LuceneEngineSearcher(new IndexSearcher(reader), reader);
        try {
            searcher.search(context);
        } finally {
            searcher.close();
        }
        return null; // TODO : figure out this path or remove this class for now
    }
}

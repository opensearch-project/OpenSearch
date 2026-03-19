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
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchExecEngine implements SearchExecEngine<LuceneSearchContext, Query> {

    @Override
    public void execute(LuceneSearchContext context) throws IOException {
        DirectoryReader reader = context.getReader();
        LuceneEngineSearcher searcher = new LuceneEngineSearcher(new IndexSearcher(reader), reader);
        try {
            searcher.search(context);
        } finally {
            searcher.close();
        }
    }

    @Override
    public LuceneSearchContext createContext(
        Object reader,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        SearchShardTask task
    ) throws IOException {
        return new LuceneSearchContext(request, shardTarget, (DirectoryReader) reader);
    }

    @Override
    public Query convertFragment(Object fragment) {
        if (fragment instanceof Query) {
            return (Query) fragment;
        }
        throw new UnsupportedOperationException("Expected Lucene Query, got " + fragment.getClass().getSimpleName());
    }
}

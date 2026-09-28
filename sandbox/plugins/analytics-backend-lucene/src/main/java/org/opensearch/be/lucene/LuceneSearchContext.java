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
import org.opensearch.search.SearchExecutionContext;

import java.io.IOException;

/**
 * Lucene-specific search execution context.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchContext implements SearchExecutionContext<LuceneEngineSearcher> {

    private final SearchShardTask task;
    private final DirectoryReader reader;
    private final LuceneEngineSearcher searcher;
    private Query query;

    /**
     * Creates a new LuceneSearchContext.
     *
     * @param task the search shard task
     * @param reader the directory reader over the index
     * @param query the Lucene query to execute
     */
    public LuceneSearchContext(SearchShardTask task, DirectoryReader reader, Query query) throws IOException {
        this.reader = reader;
        IndexSearcher indexSearcher = new IndexSearcher(reader);
        this.searcher = new LuceneEngineSearcher(indexSearcher, reader);
        this.task = task;
        this.query = query;
    }

    /** Returns the current query. */
    public Query getQuery() {
        return query;
    }

    @Override
    public SearchShardTask task() {
        return task;
    }

    @Override
    public LuceneEngineSearcher getSearcher() {
        return searcher;
    }

    /**
     * Sets the query for this context.
     *
     * @param query the Lucene query to set
     */
    public void setQuery(Query query) {
        this.query = query;
    }

    @Override
    public void close() throws IOException {
        searcher.close();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.EngineSearcher;

import java.io.IOException;
import java.util.List;

/**
 * Lucene-backed engine searcher.
 * <p>
 * This class is stateless with respect to active queries
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneEngineSearcher implements EngineSearcher<LuceneSearchContext> {

    private final IndexSearcher indexSearcher;
    private final DirectoryReader directoryReader;

    public LuceneEngineSearcher(IndexSearcher indexSearcher, DirectoryReader directoryReader) {
        this.indexSearcher = indexSearcher;
        this.directoryReader = directoryReader;
    }

    /**
     * Execute: create a Weight from the query, register it on the
     * context's lifecycle manager, and store the key + segment metadata
     * on the context for JNI callbacks.
     */
    @Override
    public void search(LuceneSearchContext context) throws IOException {
        Query query = context.getQuery();
        if (query == null) {
            throw new IllegalStateException("No query set on LuceneSearchContext");
        }
        Query rewritten = indexSearcher.rewrite(query);
        Weight weight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        List<LeafReaderContext> leaves = directoryReader.leaves();
        //TODO : Complete the wiring for search execution

    }

    public IndexSearcher getIndexSearcher() {
        return indexSearcher;
    }

    public DirectoryReader getDirectoryReader() {
        return directoryReader;
    }

    @Override
    public void close() {}
}

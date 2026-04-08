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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchContext implements SearchExecutionContext {

    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final DirectoryReader reader;
    private final LuceneEngineSearcher searcher;
    private Query query;
    private long weightPointer;
    private int segmentCount;
    private int[] segmentMaxDocs;

    public LuceneSearchContext(ShardSearchRequest request, SearchShardTarget shardTarget, DirectoryReader reader) throws IOException {
        this.reader = reader;
        IndexSearcher indexSearcher = new IndexSearcher(reader);
        searcher = new LuceneEngineSearcher(indexSearcher, reader);
        this.request = request;
        this.shardTarget = shardTarget;
    }

    public Query getQuery() {
        return query;
    }

    public DirectoryReader getReader() {
        return reader;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public long getWeightPointer() {
        return weightPointer;
    }

    public void setWeightPointer(long weightPointer) {
        this.weightPointer = weightPointer;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public void setSegmentCount(int segmentCount) {
        this.segmentCount = segmentCount;
    }

    public int[] getSegmentMaxDocs() {
        return segmentMaxDocs;
    }

    public void setSegmentMaxDocs(int[] segmentMaxDocs) {
        this.segmentMaxDocs = segmentMaxDocs;
    }

    @Override
    public ShardSearchRequest request() {
        return request;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public void close() throws IOException {
        if (weightPointer != 0) {
            LuceneEngineSearcher.releaseWeight(weightPointer);
            weightPointer = 0;
        }
        searcher.close();
    }
}

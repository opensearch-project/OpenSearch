/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * DataFusion-specific search execution context.
 * <p>
 * Carries the DataFusion query plan, engine searcher, optional {@link IndexFilterTree},
 * and columnar results.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionContext implements SearchExecutionContext {

    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final DatafusionSearcher engineSearcher;
    private DatafusionQuery datafusionQuery;
    private IndexFilterTree filterTree;

    public DatafusionContext(
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        DatafusionReader reader
    ) throws IOException {
        this.request = request;
        this.shardTarget = shardTarget;
        this.engineSearcher = new DatafusionSearcher(reader.getReaderHandle());
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
        try {
            if (filterTree != null) {
                filterTree.close();
            }
        } finally {
            engineSearcher.close();
        }
    }

    // DataFusion-specific

    public DatafusionSearcher getEngineSearcher() {
        return engineSearcher;
    }

    public DatafusionQuery getDatafusionQuery() {
        return datafusionQuery;
    }

    public void setDatafusionQuery(DatafusionQuery query) {
        this.datafusionQuery = query;
    }

    /**
     * Returns the optional filter tree for indexed parquet queries.
     * {@code null} indicates a pure parquet query with no external index involvement.
     */
    public IndexFilterTree getFilterTree() {
        return filterTree;
    }

    /**
     * Sets the filter tree for indexed parquet queries.
     */
    public void setFilterTree(IndexFilterTree filterTree) {
        this.filterTree = filterTree;
    }

}

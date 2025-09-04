/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.datafusion.DatafusionEngine;
import org.opensearch.search.ContextEngineSearcher;

/**
 * Search context for Datafusion engine
 */
public class DatafusionContext extends SearchContext {
    private final ReaderContext readerContext;
    private final ShardSearchRequest request;
    private final SearchShardTask task;
    private final DatafusionEngine readEngine;
    private final ContextEngineSearcher<DatafusionQuery> engineSearcher;

    public DatafusionContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTask task) {
        this.readerContext = readerContext;
        this.request = request;
        this.task = task;
        this.readEngine = (DatafusionEngine) readerContext.indexShard()
            .getIndexingExecutionCoordinator()
            .getPrimaryReadEngine();
        this.engineSearcher = null;//TODO readerContext.contextEngineSearcher();
    }

    public DatafusionEngine readEngine() {
        return readEngine;
    }

    public DatafusionQuery query() {
        // Extract query from request
        return null;
    }

    public ContextEngineSearcher<DatafusionQuery> contextEngineSearcher() {
        return engineSearcher;
    }
}

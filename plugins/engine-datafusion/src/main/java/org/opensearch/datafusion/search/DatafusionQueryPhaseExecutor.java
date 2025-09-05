/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.index.engine.EngineSearcher;
import org.opensearch.search.query.QueryPhaseExecutor;
import org.opensearch.search.query.QueryPhaseExecutionException;
import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.search.ContextEngineSearcher;
import org.opensearch.search.query.GenericQueryPhase;
import org.opensearch.search.query.GenericQueryPhaseSearcher;
import org.opensearch.vectorized.execution.search.spi.RecordBatchStream;

/**
 * Query phase executor for Datafusion engine
 */
public class DatafusionQueryPhaseExecutor implements QueryPhaseExecutor<DatafusionContext> {

    @Override
    public boolean execute(DatafusionContext context) throws QueryPhaseExecutionException {
        if (!canHandle(context)) {
            // TODO : throw new QueryPhaseExecutionException("Cannot handle datafusion context");
        }

        GenericQueryPhaseSearcher<DatafusionContext, DatafusionSearcher, DatafusionQuery> searcher =
            context.readEngine().getQueryPhaseSearcher();

        GenericQueryPhase<DatafusionContext, DatafusionSearcher, DatafusionQuery> queryPhase =
            new GenericQueryPhase<>(searcher);

        DatafusionQuery query = context.getDatafusionQuery();
        // TODO : rework interfaces as context itself has many objects
        return queryPhase.executeInternal(context, context.getEngineSearcher(), query);
    }

    @Override
    public boolean canHandle(DatafusionContext context) {
        return context != null &&
            context.readEngine() != null &&
            context.query() != null;
    }
}

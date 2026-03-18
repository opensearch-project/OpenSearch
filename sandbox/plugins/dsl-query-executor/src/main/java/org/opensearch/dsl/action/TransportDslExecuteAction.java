/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.DslQueryPlanExecutor;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.dsl.result.SearchResponseBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.List;

/**
 * Coordinates DSL query execution: converts SearchSourceBuilder to Calcite RelNode plans,
 * executes them via the analytics engine, and builds a SearchResponse.
 *
 * <p>Receives {@link QueryPlanExecutor} and {@link EngineContext} from the analytics engine
 * via Guice injection (enabled by {@code extendedPlugins = ['analytics-engine']}).
 */
public class TransportDslExecuteAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDslExecuteAction.class);

    private final EngineContext engineContext;
    private final DslQueryPlanExecutor planExecutor;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * Guice-injected constructor — receives analytics engine dependencies.
     *
     * @param transportService transport service
     * @param actionFilters action filters
     * @param engineContext analytics engine context providing schema and operator table
     * @param executor analytics engine plan executor
     * @param clusterService cluster service for resolving index aliases
     * @param indexNameExpressionResolver resolves aliases and wildcards to concrete indices
     */
    @Inject
    public TransportDslExecuteAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContext engineContext,
        QueryPlanExecutor<RelNode, Iterable<Object[]>> executor,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(DslExecuteAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.engineContext = engineContext;
        this.planExecutor = new DslQueryPlanExecutor(executor);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        try {
            String indexName = resolveToSingleIndex(request);
            long startTime = System.currentTimeMillis();

            SearchSourceConverter converter = new SearchSourceConverter(engineContext.getSchema());
            QueryPlans plans = converter.convert(request.source(), indexName);
            List<ExecutionResult> results = planExecutor.execute(plans);
            long tookInMillis = System.currentTimeMillis() - startTime;

            SearchResponse response = SearchResponseBuilder.build(results, tookInMillis);
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("DSL execution failed", e);
            listener.onFailure(e);
        }
    }

    // TODO: add multi-index support:
    //  1. aliases pointing to multiple indices (e.g. my-alias → [index-a, index-b])
    //  2. comma-separated indices (e.g. GET /index-a,index-b/_search)
    //  3. wildcard patterns (e.g. GET /index-*/_search)
    /**
     * Resolves the request's indices (which may be aliases or wildcards) to a single concrete index.
     * Throws if the resolution yields zero or more than one concrete index.
     */
    private String resolveToSingleIndex(SearchRequest request) {
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(
            clusterService.state(),
            request
        );
        if (concreteIndices.length != 1) {
            throw new IllegalArgumentException(
                "DSL execution currently supports exactly one concrete index, but resolved to "
                    + concreteIndices.length
                    + " indices"
            );
        }
        return concreteIndices[0].getName();
    }
}

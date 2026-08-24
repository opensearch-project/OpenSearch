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
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.DslQueryPlanExecutor;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.dsl.result.SearchResponseBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates DSL query execution: converts SearchSourceBuilder to Calcite RelNode plans,
 * executes them via the analytics engine, and builds a SearchResponse.
 *
 * <p>Receives {@link QueryPlanExecutor} and {@link EngineContextProvider} from the analytics engine
 * via Guice injection (enabled by {@code extendedPlugins = ['analytics-engine']}).
 */
public class TransportDslExecuteAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDslExecuteAction.class);

    private final EngineContextProvider contextProvider;
    private final DslQueryPlanExecutor planExecutor;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ThreadPool threadPool;

    /**
     * Guice-injected constructor — receives analytics engine dependencies.
     *
     * @param transportService transport service
     * @param actionFilters action filters
     * @param contextProvider analytics engine context providing schema and operator table
     * @param executor analytics engine plan executor
     * @param clusterService cluster service for resolving index aliases
     * @param indexNameExpressionResolver resolves aliases and wildcards to concrete indices
     */
    @Inject
    public TransportDslExecuteAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContextProvider contextProvider,
        QueryPlanExecutor<RelNode, Iterable<Object[]>> executor,
        ClusterService clusterService,
        IndicesService indicesService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ThreadPool threadPool
    ) {
        super(DslExecuteAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.contextProvider = contextProvider;
        this.planExecutor = new DslQueryPlanExecutor(executor);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            final long startNanos = System.nanoTime();
            final IndexMetadata indexMetadata;
            try {
                indexMetadata = resolveToSingleIndex(request);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            final String indexName = indexMetadata.getIndex().getName();

            // Response typing works off the mapping pinned at request start: one immutable
            // snapshot for conversion and response building, created lazily, and closed when
            // the request completes whether it succeeded or failed.
            // TODO: cache per (indexUUID, mappingVersion) to avoid rebuilding analyzers.
            final RequestScopedMapperService mapperServiceHolder = new RequestScopedMapperService(
                indexMetadata,
                indicesService::createIndexMapperService
            );
            final ActionListener<SearchResponse> requestListener = ActionListener.runAfter(listener, mapperServiceHolder::close);

            final QueryPlans plans;
            final SearchSourceConverter converter;
            try {
                converter = new SearchSourceConverter(contextProvider.getContext().schema(), mapperServiceHolder);
                plans = converter.convert(request.source(), indexName);
            } catch (ConversionException e) {
                // The request carries a shape or parameter this path cannot honor — a client
                // error (400), matching classic search's rejection of unsupported parameters.
                logger.debug("DSL conversion rejected the request", e);
                requestListener.onFailure(new IllegalArgumentException(e.getMessage(), e));
                return;
            } catch (Exception e) {
                requestListener.onFailure(e);
                return;
            }
            executePlans(plans, request, converter, startNanos, requestListener);
        });
    }

    /**
     * Submits the main plans as one batch and each COUNT plan as its own concurrent engine
     * call, joins all results, and responds through
     * {@link #buildAndRespond(List, SearchRequest, SearchSourceConverter, long, ActionListener)}.
     * Any branch failing fails the request.
     */
    private void executePlans(
        QueryPlans plans,
        SearchRequest request,
        SearchSourceConverter converter,
        long startNanos,
        ActionListener<SearchResponse> listener
    ) {
        List<QueryPlans.QueryPlan> countPlans = plans.get(QueryPlans.Type.COUNT);
        QueryPlans.Builder mainBuilder = new QueryPlans.Builder();
        for (QueryPlans.QueryPlan plan : plans.getAll()) {
            if (plan.type() != QueryPlans.Type.COUNT) {
                mainBuilder.add(plan);
            }
        }
        final QueryPlans mainPlans = mainBuilder.build();

        if (countPlans.isEmpty()) {
            try {
                planExecutor.execute(
                    mainPlans,
                    ActionListener.wrap(results -> buildAndRespond(results, request, converter, startNanos, listener), listener::onFailure)
                );
            } catch (Exception e) {
                listener.onFailure(e);
            }
            return;
        }

        // COUNT plans run concurrently with the main plans - all are engine calls.
        final GroupedActionListener<List<ExecutionResult>> joined = new GroupedActionListener<>(ActionListener.wrap(collections -> {
            List<ExecutionResult> allResults = new ArrayList<>();
            for (List<ExecutionResult> branch : collections) {
                allResults.addAll(branch);
            }
            buildAndRespond(allResults, request, converter, startNanos, listener);
        }, listener::onFailure), 1 + countPlans.size());

        try {
            planExecutor.execute(mainPlans, joined);
        } catch (Exception e) {
            joined.onFailure(e);
        }

        for (QueryPlans.QueryPlan countPlan : countPlans) {
            try {
                planExecutor.execute(new QueryPlans.Builder().add(countPlan).build(), joined);
            } catch (Exception e) {
                joined.onFailure(e);
            }
        }
    }

    private void buildAndRespond(
        List<ExecutionResult> results,
        SearchRequest request,
        SearchSourceConverter converter,
        long startNanos,
        ActionListener<SearchResponse> listener
    ) {
        final SearchResponse response;
        try {
            long tookInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            response = SearchResponseBuilder.build(results, request, converter.getAggregationRegistry(), tookInMillis);
        } catch (Exception buildEx) {
            listener.onFailure(buildEx);
            return;
        }
        listener.onResponse(response);
    }

    // TODO: Consider delegating index resolution to Analytics Core plugin (e.g. via
    // EngineContextProvider or Schema table lookup) for consistency, and return RelOptTable directly
    // so this plugin doesn't need its own resolution logic.
    /**
     * Resolves the request's indices (which may be aliases or wildcards) to a single concrete
     * index, returning its metadata from the same cluster state snapshot. Throws if the
     * resolution yields zero or more than one concrete index.
     */
    private IndexMetadata resolveToSingleIndex(SearchRequest request) {
        ClusterState state = clusterService.state();
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices.length != 1) {
            throw new IllegalArgumentException(
                "DSL execution currently supports exactly one concrete index, but resolved to " + concreteIndices.length + " indices"
            );
        }
        return state.metadata().getIndexSafe(concreteIndices[0]);
    }
}

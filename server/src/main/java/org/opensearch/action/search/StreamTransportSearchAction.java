/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * Deprecated shim: streaming is now enabled via flags on the regular SearchAction.
 * Keeping the class to avoid large refactors; it delegates to the parent wiring and
 * does not register its own action.
 * @opensearch.internal
 */
public class StreamTransportSearchAction extends TransportSearchAction {
    @Inject
    public StreamTransportSearchAction(
        NodeClient client,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        TransportService transportService,
        @Nullable StreamTransportService streamTransportService,
        SearchService searchService,
        @Nullable StreamSearchTransportService searchTransportService,
        SearchPhaseController searchPhaseController,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchPipelineService searchPipelineService,
        MetricsRegistry metricsRegistry,
        SearchRequestOperationsCompositeListenerFactory searchRequestOperationsCompositeListenerFactory,
        Tracer tracer,
        TaskResourceTrackingService taskResourceTrackingService
    ) {
        super(
            client,
            threadPool,
            circuitBreakerService,
            transportService,
            streamTransportService,
            searchService,
            searchTransportService,
            searchPhaseController,
            clusterService,
            actionFilters,
            indexNameExpressionResolver,
            namedWriteableRegistry,
            searchPipelineService,
            metricsRegistry,
            searchRequestOperationsCompositeListenerFactory,
            tracer,
            taskResourceTrackingService
        );
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        // Enable streaming scoring for stream search requests
        System.out.println("DEBUG: StreamTransportSearchAction.doExecute() called");
        System.out.println("DEBUG: Original streaming mode: " + searchRequest.getStreamingSearchMode());
        
        // Preserve the streaming mode that was set in the request
        String originalStreamingMode = searchRequest.getStreamingSearchMode();
        
        // Enable streaming scoring BEFORE calling super.doExecute() so ShardSearchRequest objects are created with correct mode
        searchRequest.setStreamingScoring(true);
        
        // Restore the streaming mode if it was set
        if (originalStreamingMode != null) {
            searchRequest.setStreamingSearchMode(originalStreamingMode);
        }
        
        System.out.println("DEBUG: After setStreamingScoring, streaming mode: " + searchRequest.getStreamingSearchMode());
        
        // Now call super.doExecute() - the ShardSearchRequest objects will be created with the correct streaming mode
        super.doExecute(task, searchRequest, listener);
    }

    AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction(
        SearchTask task,
        SearchRequest searchRequest,
        Executor executor,
        GroupShardsIterator<SearchShardIterator> shardIterators,
        SearchTimeProvider timeProvider,
        BiFunction<String, String, Transport.Connection> connectionLookup,
        ClusterState clusterState,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Map<String, Set<String>> indexRoutings,
        ActionListener<SearchResponse> listener,
        boolean preFilter,
        ThreadPool threadPool,
        SearchResponse.Clusters clusters,
        SearchRequestContext searchRequestContext
    ) {
        if (preFilter) {
            throw new IllegalStateException("Search pre-filter is not supported in streaming");
        } else {
            // Wrap the listener to support streaming responses
            ActionListener<SearchResponse> streamingListener = listener;
            if (searchRequest.isStreamingScoring() && searchRequest.source() != null && searchRequest.source().size() > 0) {
                streamingListener = new StreamingSearchResponseListener(listener, searchRequest);
            }

            // Create a streaming progress listener that can send partial TopDocs
            SearchProgressListener progressListener = task.getProgressListener();
            if (searchRequest.isStreamingScoring() && searchRequest.source() != null && searchRequest.source().size() > 0) {
                // Use streaming listener for search queries with hits
                progressListener = new StreamingSearchProgressListener(streamingListener, searchPhaseController, searchRequest);
            }

            // Ensure streaming mode is set on the request that will be used to create the consumer
            if (searchRequest.getStreamingSearchMode() == null) {
                // If no streaming mode was set, use NO_SCORING as default for streaming searches
                searchRequest.setStreamingSearchMode("NO_SCORING");
                System.out.println("DEBUG: Set default streaming mode: NO_SCORING");
            }
            
            final QueryPhaseResultConsumer queryResultConsumer = searchPhaseController.newSearchPhaseResults(
                executor,
                circuitBreaker,
                progressListener,
                searchRequest,
                shardIterators.size(),
                exc -> cancelTask(task, exc)
            );
            AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction;
            switch (searchRequest.searchType()) {
                case QUERY_THEN_FETCH:
                    searchAsyncAction = new StreamSearchQueryThenFetchAsyncAction(
                        logger,
                        searchTransportService,
                        connectionLookup,
                        aliasFilter,
                        concreteIndexBoosts,
                        indexRoutings,
                        searchPhaseController,
                        executor,
                        queryResultConsumer,
                        searchRequest,
                        streamingListener,  // Use the streaming-aware listener
                        shardIterators,
                        timeProvider,
                        clusterState,
                        task,
                        clusters,
                        searchRequestContext,
                        tracer
                    );
                    break;
                default:
                    throw new IllegalStateException("Unknown search type: [" + searchRequest.searchType() + "]");
            }
            return searchAsyncAction;
        }
    }
}

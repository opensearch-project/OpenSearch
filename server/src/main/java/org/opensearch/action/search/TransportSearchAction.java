/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.search;

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.client.Client;
import org.opensearch.client.OriginSettingClient;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.pipeline.PipelinedRequest;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanBuilder;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.listener.TraceableActionListener;
import org.opensearch.telemetry.tracing.listener.TraceableSearchRequestOperationsListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteClusterAware;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.QueryGroupTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.opensearch.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.opensearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.opensearch.search.sort.FieldSortBuilder.hasPrimaryFieldSort;

/**
 * Perform search action
 *
 * @opensearch.internal
 */
public class TransportSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    /** The maximum number of shards for a single search request. */
    public static final Setting<Long> SHARD_COUNT_LIMIT_SETTING = Setting.longSetting(
        "action.search.shard_count.limit",
        Long.MAX_VALUE,
        1L,
        Property.Dynamic,
        Property.NodeScope
    );

    // cluster level setting for timeout based search cancellation. If search request level parameter is present then that will take
    // precedence over the cluster setting value
    public static final String SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY = "search.cancel_after_time_interval";
    public static final Setting<TimeValue> SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING = Setting.timeSetting(
        SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY,
        SearchService.NO_TIMEOUT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final String SEARCH_PHASE_TOOK_ENABLED_KEY = "search.phase_took_enabled";
    public static final Setting<Boolean> SEARCH_PHASE_TOOK_ENABLED = Setting.boolSetting(
        SEARCH_PHASE_TOOK_ENABLED_KEY,
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final NodeClient client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final RemoteClusterService remoteClusterService;
    private final SearchPhaseController searchPhaseController;
    private final SearchService searchService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final CircuitBreaker circuitBreaker;
    private final SearchPipelineService searchPipelineService;
    private final SearchRequestOperationsCompositeListenerFactory searchRequestOperationsCompositeListenerFactory;
    private final Tracer tracer;

    private final MetricsRegistry metricsRegistry;

    private TaskResourceTrackingService taskResourceTrackingService;

    @Inject
    public TransportSearchAction(
        NodeClient client,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        TransportService transportService,
        SearchService searchService,
        SearchTransportService searchTransportService,
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
        super(SearchAction.NAME, transportService, actionFilters, (Writeable.Reader<SearchRequest>) SearchRequest::new);
        this.client = client;
        this.threadPool = threadPool;
        this.circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
        this.searchPhaseController = searchPhaseController;
        this.searchTransportService = searchTransportService;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        SearchTransportService.registerRequestHandler(transportService, searchService);
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.searchPipelineService = searchPipelineService;
        this.metricsRegistry = metricsRegistry;
        this.searchRequestOperationsCompositeListenerFactory = searchRequestOperationsCompositeListenerFactory;
        this.tracer = tracer;
        this.taskResourceTrackingService = taskResourceTrackingService;
    }

    private Map<String, AliasFilter> buildPerIndexAliasFilter(
        SearchRequest request,
        ClusterState clusterState,
        Index[] concreteIndices,
        Map<String, AliasFilter> remoteAliasMap
    ) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        for (Index index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index.getName());
            AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, index.getName(), indicesAndAliases);
            assert aliasFilter != null;
            aliasFilterMap.put(index.getUUID(), aliasFilter);
        }
        aliasFilterMap.putAll(remoteAliasMap);
        return aliasFilterMap;
    }

    private Map<String, Float> resolveIndexBoosts(SearchRequest searchRequest, ClusterState clusterState) {
        if (searchRequest.source() == null) {
            return Collections.emptyMap();
        }

        SearchSourceBuilder source = searchRequest.source();
        if (source.indexBoosts() == null) {
            return Collections.emptyMap();
        }

        Map<String, Float> concreteIndexBoosts = new HashMap<>();
        for (SearchSourceBuilder.IndexBoost ib : source.indexBoosts()) {
            Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(
                clusterState,
                searchRequest.indicesOptions(),
                ib.getIndex()
            );

            for (Index concreteIndex : concreteIndices) {
                concreteIndexBoosts.putIfAbsent(concreteIndex.getUUID(), ib.getBoost());
            }
        }
        return Collections.unmodifiableMap(concreteIndexBoosts);
    }

    /**
     * Search operations need two clocks. One clock is to fulfill real clock needs (e.g., resolving
     * "now" to an index name). Another clock is needed for measuring how long a search operation
     * took. These two uses are at odds with each other. There are many issues with using a real
     * clock for measuring how long an operation took (they often lack precision, they are subject
     * to moving backwards due to NTP and other such complexities, etc.). There are also issues with
     * using a relative clock for reporting real time. Thus, we simply separate these two uses.
     *
     * @opensearch.internal
     */
    static final class SearchTimeProvider {
        private final long absoluteStartMillis;
        private final long relativeStartNanos;
        private final LongSupplier relativeCurrentNanosProvider;

        /**
         * Instantiates a new search time provider. The absolute start time is the real clock time
         * used for resolving index expressions that include dates. The relative start time is the
         * start of the search operation according to a relative clock. The total time the search
         * operation took can be measured against the provided relative clock and the relative start
         * time.
         *
         * @param absoluteStartMillis the absolute start time in milliseconds since the epoch
         * @param relativeStartNanos the relative start time in nanoseconds
         * @param relativeCurrentNanosProvider provides the current relative time
         */
        SearchTimeProvider(final long absoluteStartMillis, final long relativeStartNanos, final LongSupplier relativeCurrentNanosProvider) {
            this.absoluteStartMillis = absoluteStartMillis;
            this.relativeStartNanos = relativeStartNanos;
            this.relativeCurrentNanosProvider = relativeCurrentNanosProvider;
        }

        long getAbsoluteStartMillis() {
            return absoluteStartMillis;
        }

        long buildTookInMillis() {
            return TimeUnit.NANOSECONDS.toMillis(relativeCurrentNanosProvider.getAsLong() - relativeStartNanos);
        }
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        // only if task is of type CancellableTask and support cancellation on timeout, treat this request eligible for timeout based
        // cancellation. There may be other top level requests like AsyncSearch which is using SearchRequest internally and has it's own
        // cancellation mechanism. For such cases, the SearchRequest when created can override the createTask and set the
        // cancelAfterTimeInterval to NO_TIMEOUT and bypass this mechanism
        if (task instanceof CancellableTask) {
            listener = TimeoutTaskCancellationUtility.wrapWithCancellationListener(
                client,
                (CancellableTask) task,
                clusterService.getClusterSettings().get(SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING),
                listener,
                e -> {}
            );
        }
        executeRequest(task, searchRequest, this::searchAsyncAction, listener);
    }

    /**
     * The single phase search action.
     *
     * @opensearch.internal
     */
    public interface SinglePhaseSearchAction {
        void executeOnShardTarget(
            SearchTask searchTask,
            SearchShardTarget target,
            Transport.Connection connection,
            ActionListener<SearchPhaseResult> listener
        );
    }

    public void executeRequest(
        Task task,
        SearchRequest searchRequest,
        String actionName,
        boolean includeSearchContext,
        SinglePhaseSearchAction phaseSearchAction,
        ActionListener<SearchResponse> listener
    ) {
        executeRequest(task, searchRequest, new SearchAsyncActionProvider() {
            @Override
            public AbstractSearchAsyncAction<? extends SearchPhaseResult> asyncSearchAction(
                SearchTask task,
                SearchRequest searchRequest,
                Executor executor,
                GroupShardsIterator<SearchShardIterator> shardsIts,
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
                return new AbstractSearchAsyncAction<SearchPhaseResult>(
                    actionName,
                    logger,
                    searchTransportService,
                    connectionLookup,
                    aliasFilter,
                    concreteIndexBoosts,
                    indexRoutings,
                    executor,
                    searchRequest,
                    listener,
                    shardsIts,
                    timeProvider,
                    clusterState,
                    task,
                    new ArraySearchPhaseResults<>(shardsIts.size()),
                    searchRequest.getMaxConcurrentShardRequests(),
                    clusters,
                    searchRequestContext,
                    tracer
                ) {
                    @Override
                    protected void executePhaseOnShard(
                        SearchShardIterator shardIt,
                        SearchShardTarget shard,
                        SearchActionListener<SearchPhaseResult> listener
                    ) {
                        final Transport.Connection connection = getConnection(shard.getClusterAlias(), shard.getNodeId());
                        phaseSearchAction.executeOnShardTarget(task, shard, connection, listener);
                    }

                    @Override
                    protected SearchPhase getNextPhase(SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
                        return new SearchPhase(getName()) {
                            @Override
                            public void run() {
                                final AtomicArray<SearchPhaseResult> atomicArray = results.getAtomicArray();
                                sendSearchResponse(InternalSearchResponse.empty(), atomicArray);
                            }
                        };
                    }

                    @Override
                    boolean buildPointInTimeFromSearchResults() {
                        return includeSearchContext;
                    }
                };
            }
        }, listener);
    }

    private void executeRequest(
        Task task,
        SearchRequest originalSearchRequest,
        SearchAsyncActionProvider searchAsyncActionProvider,
        ActionListener<SearchResponse> originalListener
    ) {
        final long relativeStartNanos = System.nanoTime();
        final SearchTimeProvider timeProvider = new SearchTimeProvider(
            originalSearchRequest.getOrCreateAbsoluteStartMillis(),
            relativeStartNanos,
            System::nanoTime
        );
        if (originalSearchRequest.isPhaseTook() == null) {
            originalSearchRequest.setPhaseTook(clusterService.getClusterSettings().get(SEARCH_PHASE_TOOK_ENABLED));
        }

        final Span requestSpan = tracer.startSpan(SpanBuilder.from(task, actionName));
        try (final SpanScope spanScope = tracer.withSpanInScope(requestSpan)) {
            SearchRequestOperationsListener.CompositeListener requestOperationsListeners;
            final ActionListener<SearchResponse> updatedListener = TraceableActionListener.create(originalListener, requestSpan, tracer);
            requestOperationsListeners = searchRequestOperationsCompositeListenerFactory.buildCompositeListener(
                originalSearchRequest,
                logger,
                TraceableSearchRequestOperationsListener.create(tracer, requestSpan)
            );
            SearchRequestContext searchRequestContext = new SearchRequestContext(
                requestOperationsListeners,
                originalSearchRequest,
                taskResourceTrackingService::getTaskResourceUsageFromThreadContext
            );
            searchRequestContext.getSearchRequestOperationsListener().onRequestStart(searchRequestContext);

            // At this point either the QUERY_GROUP_ID header will be present in ThreadContext either via ActionFilter
            // or HTTP header (HTTP header will be deprecated once ActionFilter is implemented)
            if (task instanceof QueryGroupTask) {
                ((QueryGroupTask) task).setQueryGroupId(threadPool.getThreadContext());
            }

            PipelinedRequest searchRequest;
            ActionListener<SearchResponse> listener;
            try {
                searchRequest = searchPipelineService.resolvePipeline(originalSearchRequest, indexNameExpressionResolver);
                listener = searchRequest.transformResponseListener(updatedListener);
            } catch (Exception e) {
                updatedListener.onFailure(e);
                return;
            }

            ActionListener<SearchRequest> requestTransformListener = ActionListener.wrap(sr -> {

                ActionListener<SearchSourceBuilder> rewriteListener = buildRewriteListener(
                    sr,
                    task,
                    timeProvider,
                    searchAsyncActionProvider,
                    listener,
                    searchRequestContext
                );
                if (sr.source() == null) {
                    rewriteListener.onResponse(sr.source());
                } else {
                    Rewriteable.rewriteAndFetch(
                        sr.source(),
                        searchService.getRewriteContext(timeProvider::getAbsoluteStartMillis),
                        rewriteListener
                    );
                }
            }, listener::onFailure);
            searchRequest.transformRequest(requestTransformListener);
        }
    }

    private ActionListener<SearchSourceBuilder> buildRewriteListener(
        SearchRequest searchRequest,
        Task task,
        SearchTimeProvider timeProvider,
        SearchAsyncActionProvider searchAsyncActionProvider,
        ActionListener<SearchResponse> listener,
        SearchRequestContext searchRequestContext
    ) {
        return ActionListener.wrap(source -> {
            if (source != searchRequest.source()) {
                // only set it if it changed - we don't allow null values to be set but it might be already null. this way we catch
                // situations when source is rewritten to null due to a bug
                searchRequest.source(source);
            }
            final ClusterState clusterState = clusterService.state();
            final SearchContextId searchContext;
            final Map<String, OriginalIndices> remoteClusterIndices;
            if (searchRequest.pointInTimeBuilder() != null) {
                searchContext = SearchContextId.decode(namedWriteableRegistry, searchRequest.pointInTimeBuilder().getId());
                remoteClusterIndices = getIndicesFromSearchContexts(searchContext, searchRequest.indicesOptions());
            } else {
                searchContext = null;
                remoteClusterIndices = remoteClusterService.groupIndices(
                    searchRequest.indicesOptions(),
                    searchRequest.indices(),
                    idx -> indexNameExpressionResolver.hasIndexAbstraction(idx, clusterState)
                );
            }
            OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            if (remoteClusterIndices.isEmpty()) {
                executeLocalSearch(
                    task,
                    timeProvider,
                    searchRequest,
                    localIndices,
                    clusterState,
                    listener,
                    searchContext,
                    searchAsyncActionProvider,
                    searchRequestContext
                );
            } else {
                if (shouldMinimizeRoundtrips(searchRequest)) {
                    ccsRemoteReduce(
                        searchRequest,
                        localIndices,
                        remoteClusterIndices,
                        timeProvider,
                        searchService.aggReduceContextBuilder(searchRequest.source()),
                        remoteClusterService,
                        threadPool,
                        listener,
                        (r, l) -> executeLocalSearch(
                            task,
                            timeProvider,
                            r,
                            localIndices,
                            clusterState,
                            l,
                            searchContext,
                            searchAsyncActionProvider,
                            searchRequestContext
                        ),
                        searchRequestContext
                    );
                } else {
                    AtomicInteger skippedClusters = new AtomicInteger(0);
                    collectSearchShards(
                        searchRequest.indicesOptions(),
                        searchRequest.preference(),
                        searchRequest.routing(),
                        skippedClusters,
                        remoteClusterIndices,
                        remoteClusterService,
                        threadPool,
                        ActionListener.wrap(searchShardsResponses -> {
                            final BiFunction<String, String, DiscoveryNode> clusterNodeLookup = getRemoteClusterNodeLookup(
                                searchShardsResponses
                            );
                            final Map<String, AliasFilter> remoteAliasFilters;
                            final List<SearchShardIterator> remoteShardIterators;
                            if (searchContext != null) {
                                remoteAliasFilters = searchContext.aliasFilter();
                                remoteShardIterators = getRemoteShardsIteratorFromPointInTime(
                                    searchShardsResponses,
                                    searchContext,
                                    searchRequest.pointInTimeBuilder().getKeepAlive(),
                                    remoteClusterIndices
                                );
                            } else {
                                remoteAliasFilters = getRemoteAliasFilters(searchShardsResponses);
                                remoteShardIterators = getRemoteShardsIterator(
                                    searchShardsResponses,
                                    remoteClusterIndices,
                                    remoteAliasFilters
                                );
                            }
                            int localClusters = localIndices == null ? 0 : 1;
                            int totalClusters = remoteClusterIndices.size() + localClusters;
                            int successfulClusters = searchShardsResponses.size() + localClusters;
                            executeSearch(
                                (SearchTask) task,
                                timeProvider,
                                searchRequest,
                                localIndices,
                                remoteShardIterators,
                                clusterNodeLookup,
                                clusterState,
                                remoteAliasFilters,
                                listener,
                                new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters.get()),
                                searchContext,
                                searchAsyncActionProvider,
                                searchRequestContext
                            );
                        }, listener::onFailure)
                    );
                }
            }
        }, listener::onFailure);
    }

    static boolean shouldMinimizeRoundtrips(SearchRequest searchRequest) {
        if (searchRequest.isCcsMinimizeRoundtrips() == false) {
            return false;
        }
        if (searchRequest.scroll() != null) {
            return false;
        }
        if (searchRequest.pointInTimeBuilder() != null) {
            return false;
        }
        if (searchRequest.searchType() == DFS_QUERY_THEN_FETCH) {
            return false;
        }
        SearchSourceBuilder source = searchRequest.source();
        return source == null
            || source.collapse() == null
            || source.collapse().getInnerHits() == null
            || source.collapse().getInnerHits().isEmpty();
    }

    static void ccsRemoteReduce(
        SearchRequest searchRequest,
        OriginalIndices localIndices,
        Map<String, OriginalIndices> remoteIndices,
        SearchTimeProvider timeProvider,
        InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
        RemoteClusterService remoteClusterService,
        ThreadPool threadPool,
        ActionListener<SearchResponse> listener,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> localSearchConsumer,
        SearchRequestContext searchRequestContext
    ) {

        if (localIndices == null && remoteIndices.size() == 1) {
            // if we are searching against a single remote cluster, we simply forward the original search request to such cluster
            // and we directly perform final reduction in the remote cluster
            Map.Entry<String, OriginalIndices> entry = remoteIndices.entrySet().iterator().next();
            String clusterAlias = entry.getKey();
            boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
            OriginalIndices indices = entry.getValue();
            SearchRequest ccsSearchRequest = SearchRequest.subSearchRequest(
                searchRequest,
                indices.indices(),
                clusterAlias,
                timeProvider.getAbsoluteStartMillis(),
                true
            );
            Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
            remoteClusterClient.search(ccsSearchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    Map<String, ProfileShardResult> profileResults = searchResponse.getProfileResults();
                    SearchProfileShardResults profile = profileResults == null || profileResults.isEmpty()
                        ? null
                        : new SearchProfileShardResults(profileResults);
                    InternalSearchResponse internalSearchResponse = new InternalSearchResponse(
                        searchResponse.getHits(),
                        (InternalAggregations) searchResponse.getAggregations(),
                        searchResponse.getSuggest(),
                        profile,
                        searchResponse.isTimedOut(),
                        searchResponse.isTerminatedEarly(),
                        searchResponse.getNumReducePhases()
                    );
                    listener.onResponse(
                        new SearchResponse(
                            internalSearchResponse,
                            searchResponse.getScrollId(),
                            searchResponse.getTotalShards(),
                            searchResponse.getSuccessfulShards(),
                            searchResponse.getSkippedShards(),
                            timeProvider.buildTookInMillis(),
                            searchRequestContext.getPhaseTook(),
                            searchResponse.getShardFailures(),
                            new SearchResponse.Clusters(1, 1, 0),
                            searchResponse.pointInTimeId()
                        )
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    if (skipUnavailable) {
                        listener.onResponse(SearchResponse.empty(timeProvider::buildTookInMillis, new SearchResponse.Clusters(1, 0, 1)));
                    } else {
                        listener.onFailure(wrapRemoteClusterFailure(clusterAlias, e));
                    }
                }
            });
        } else {
            SearchResponseMerger searchResponseMerger = createSearchResponseMerger(
                searchRequest.source(),
                timeProvider,
                aggReduceContextBuilder
            );
            AtomicInteger skippedClusters = new AtomicInteger(0);
            final AtomicReference<Exception> exceptions = new AtomicReference<>();
            int totalClusters = remoteIndices.size() + (localIndices == null ? 0 : 1);
            final CountDown countDown = new CountDown(totalClusters);
            for (Map.Entry<String, OriginalIndices> entry : remoteIndices.entrySet()) {
                String clusterAlias = entry.getKey();
                boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
                OriginalIndices indices = entry.getValue();
                SearchRequest ccsSearchRequest = SearchRequest.subSearchRequest(
                    searchRequest,
                    indices.indices(),
                    clusterAlias,
                    timeProvider.getAbsoluteStartMillis(),
                    false
                );
                ActionListener<SearchResponse> ccsListener = createCCSListener(
                    clusterAlias,
                    skipUnavailable,
                    countDown,
                    skippedClusters,
                    exceptions,
                    searchResponseMerger,
                    totalClusters,
                    listener,
                    searchRequestContext
                );
                Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
                remoteClusterClient.search(ccsSearchRequest, ccsListener);
            }
            if (localIndices != null) {
                ActionListener<SearchResponse> ccsListener = createCCSListener(
                    RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                    false,
                    countDown,
                    skippedClusters,
                    exceptions,
                    searchResponseMerger,
                    totalClusters,
                    listener,
                    searchRequestContext
                );
                SearchRequest ccsLocalSearchRequest = SearchRequest.subSearchRequest(
                    searchRequest,
                    localIndices.indices(),
                    RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                    timeProvider.getAbsoluteStartMillis(),
                    false
                );
                localSearchConsumer.accept(ccsLocalSearchRequest, ccsListener);
            }
        }
    }

    static SearchResponseMerger createSearchResponseMerger(
        SearchSourceBuilder source,
        SearchTimeProvider timeProvider,
        InternalAggregation.ReduceContextBuilder aggReduceContextBuilder
    ) {
        final int from;
        final int size;
        final int trackTotalHitsUpTo;
        if (source == null) {
            from = SearchService.DEFAULT_FROM;
            size = SearchService.DEFAULT_SIZE;
            trackTotalHitsUpTo = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;
        } else {
            from = source.from() == -1 ? SearchService.DEFAULT_FROM : source.from();
            size = source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size();
            trackTotalHitsUpTo = source.trackTotalHitsUpTo() == null
                ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : source.trackTotalHitsUpTo();
            // here we modify the original source so we can re-use it by setting it to each outgoing search request
            source.from(0);
            source.size(from + size);
        }
        return new SearchResponseMerger(from, size, trackTotalHitsUpTo, timeProvider, aggReduceContextBuilder);
    }

    static void collectSearchShards(
        IndicesOptions indicesOptions,
        String preference,
        String routing,
        AtomicInteger skippedClusters,
        Map<String, OriginalIndices> remoteIndicesByCluster,
        RemoteClusterService remoteClusterService,
        ThreadPool threadPool,
        ActionListener<Map<String, ClusterSearchShardsResponse>> listener
    ) {
        final CountDown responsesCountDown = new CountDown(remoteIndicesByCluster.size());
        final Map<String, ClusterSearchShardsResponse> searchShardsResponses = new ConcurrentHashMap<>();
        final AtomicReference<Exception> exceptions = new AtomicReference<>();
        for (Map.Entry<String, OriginalIndices> entry : remoteIndicesByCluster.entrySet()) {
            final String clusterAlias = entry.getKey();
            boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
            Client clusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
            final String[] indices = entry.getValue().indices();
            ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest(indices).indicesOptions(indicesOptions)
                .local(true)
                .preference(preference)
                .routing(routing);
            clusterClient.admin()
                .cluster()
                .searchShards(
                    searchShardsRequest,
                    new CCSActionListener<ClusterSearchShardsResponse, Map<String, ClusterSearchShardsResponse>>(
                        clusterAlias,
                        skipUnavailable,
                        responsesCountDown,
                        skippedClusters,
                        exceptions,
                        listener
                    ) {
                        @Override
                        void innerOnResponse(ClusterSearchShardsResponse clusterSearchShardsResponse) {
                            searchShardsResponses.put(clusterAlias, clusterSearchShardsResponse);
                        }

                        @Override
                        Map<String, ClusterSearchShardsResponse> createFinalResponse() {
                            return searchShardsResponses;
                        }
                    }
                );
        }
    }

    private static ActionListener<SearchResponse> createCCSListener(
        String clusterAlias,
        boolean skipUnavailable,
        CountDown countDown,
        AtomicInteger skippedClusters,
        AtomicReference<Exception> exceptions,
        SearchResponseMerger searchResponseMerger,
        int totalClusters,
        ActionListener<SearchResponse> originalListener,
        SearchRequestContext searchRequestContext
    ) {
        return new CCSActionListener<SearchResponse, SearchResponse>(
            clusterAlias,
            skipUnavailable,
            countDown,
            skippedClusters,
            exceptions,
            originalListener
        ) {
            @Override
            void innerOnResponse(SearchResponse searchResponse) {
                searchResponseMerger.add(searchResponse);
            }

            @Override
            SearchResponse createFinalResponse() {
                SearchResponse.Clusters clusters = new SearchResponse.Clusters(
                    totalClusters,
                    searchResponseMerger.numResponses(),
                    skippedClusters.get()
                );
                return searchResponseMerger.getMergedResponse(clusters, searchRequestContext);
            }
        };
    }

    private void executeLocalSearch(
        Task task,
        SearchTimeProvider timeProvider,
        SearchRequest searchRequest,
        OriginalIndices localIndices,
        ClusterState clusterState,
        ActionListener<SearchResponse> listener,
        SearchContextId searchContext,
        SearchAsyncActionProvider searchAsyncActionProvider,
        SearchRequestContext searchRequestContext
    ) {
        executeSearch(
            (SearchTask) task,
            timeProvider,
            searchRequest,
            localIndices,
            Collections.emptyList(),
            (clusterName, nodeId) -> null,
            clusterState,
            Collections.emptyMap(),
            listener,
            SearchResponse.Clusters.EMPTY,
            searchContext,
            searchAsyncActionProvider,
            searchRequestContext
        );
    }

    static BiFunction<String, String, DiscoveryNode> getRemoteClusterNodeLookup(Map<String, ClusterSearchShardsResponse> searchShardsResp) {
        Map<String, Map<String, DiscoveryNode>> clusterToNode = new HashMap<>();
        for (Map.Entry<String, ClusterSearchShardsResponse> entry : searchShardsResp.entrySet()) {
            String clusterAlias = entry.getKey();
            for (DiscoveryNode remoteNode : entry.getValue().getNodes()) {
                clusterToNode.computeIfAbsent(clusterAlias, k -> new HashMap<>()).put(remoteNode.getId(), remoteNode);
            }
        }
        return (clusterAlias, nodeId) -> {
            Map<String, DiscoveryNode> clusterNodes = clusterToNode.get(clusterAlias);
            if (clusterNodes == null) {
                throw new IllegalArgumentException("unknown remote cluster: " + clusterAlias);
            }
            return clusterNodes.get(nodeId);
        };
    }

    static Map<String, AliasFilter> getRemoteAliasFilters(Map<String, ClusterSearchShardsResponse> searchShardsResp) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        for (Map.Entry<String, ClusterSearchShardsResponse> entry : searchShardsResp.entrySet()) {
            ClusterSearchShardsResponse searchShardsResponse = entry.getValue();
            final Map<String, AliasFilter> indicesAndFilters = searchShardsResponse.getIndicesAndFilters();
            for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
                ShardId shardId = clusterSearchShardsGroup.getShardId();
                final AliasFilter aliasFilter;
                if (indicesAndFilters == null) {
                    aliasFilter = AliasFilter.EMPTY;
                } else {
                    aliasFilter = indicesAndFilters.get(shardId.getIndexName());
                    assert aliasFilter != null : "alias filter must not be null for index: " + shardId.getIndex();
                }
                // here we have to map the filters to the UUID since from now on we use the uuid for the lookup
                aliasFilterMap.put(shardId.getIndex().getUUID(), aliasFilter);
            }
        }
        return aliasFilterMap;
    }

    static List<SearchShardIterator> getRemoteShardsIterator(
        Map<String, ClusterSearchShardsResponse> searchShardsResponses,
        Map<String, OriginalIndices> remoteIndicesByCluster,
        Map<String, AliasFilter> aliasFilterMap
    ) {
        final List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
        for (Map.Entry<String, ClusterSearchShardsResponse> entry : searchShardsResponses.entrySet()) {
            for (ClusterSearchShardsGroup clusterSearchShardsGroup : entry.getValue().getGroups()) {
                // add the cluster name to the remote index names for indices disambiguation
                // this ends up in the hits returned with the search response
                ShardId shardId = clusterSearchShardsGroup.getShardId();
                AliasFilter aliasFilter = aliasFilterMap.get(shardId.getIndex().getUUID());
                String[] aliases = aliasFilter.getAliases();
                String clusterAlias = entry.getKey();
                String[] finalIndices = aliases.length == 0 ? new String[] { shardId.getIndexName() } : aliases;
                final OriginalIndices originalIndices = remoteIndicesByCluster.get(clusterAlias);
                assert originalIndices != null : "original indices are null for clusterAlias: " + clusterAlias;
                SearchShardIterator shardIterator = new SearchShardIterator(
                    clusterAlias,
                    shardId,
                    Arrays.asList(clusterSearchShardsGroup.getShards()),
                    new OriginalIndices(finalIndices, originalIndices.indicesOptions())
                );
                remoteShardIterators.add(shardIterator);
            }
        }
        return remoteShardIterators;
    }

    static List<SearchShardIterator> getRemoteShardsIteratorFromPointInTime(
        Map<String, ClusterSearchShardsResponse> searchShardsResponses,
        SearchContextId searchContextId,
        TimeValue searchContextKeepAlive,
        Map<String, OriginalIndices> remoteClusterIndices
    ) {
        final List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
        for (Map.Entry<String, ClusterSearchShardsResponse> entry : searchShardsResponses.entrySet()) {
            for (ClusterSearchShardsGroup group : entry.getValue().getGroups()) {
                final ShardId shardId = group.getShardId();
                final String clusterAlias = entry.getKey();
                final SearchContextIdForNode perNode = searchContextId.shards().get(shardId);
                assert clusterAlias.equals(perNode.getClusterAlias()) : clusterAlias + " != " + perNode.getClusterAlias();
                final List<String> targetNodes = Collections.singletonList(perNode.getNode());
                SearchShardIterator shardIterator = new SearchShardIterator(
                    clusterAlias,
                    shardId,
                    targetNodes,
                    remoteClusterIndices.get(clusterAlias),
                    perNode.getSearchContextId(),
                    searchContextKeepAlive
                );
                remoteShardIterators.add(shardIterator);
            }
        }
        return remoteShardIterators;
    }

    private Index[] resolveLocalIndices(OriginalIndices localIndices, ClusterState clusterState, SearchTimeProvider timeProvider) {
        if (localIndices == null) {
            return Index.EMPTY_ARRAY; // don't search on any local index (happens when only remote indices were specified)
        }
        return indexNameExpressionResolver.concreteIndices(clusterState, localIndices, timeProvider.getAbsoluteStartMillis());
    }

    private void executeSearch(
        SearchTask task,
        SearchTimeProvider timeProvider,
        SearchRequest searchRequest,
        OriginalIndices localIndices,
        List<SearchShardIterator> remoteShardIterators,
        BiFunction<String, String, DiscoveryNode> remoteConnections,
        ClusterState clusterState,
        Map<String, AliasFilter> remoteAliasMap,
        ActionListener<SearchResponse> listener,
        SearchResponse.Clusters clusters,
        @Nullable SearchContextId searchContext,
        SearchAsyncActionProvider searchAsyncActionProvider,
        SearchRequestContext searchRequestContext
    ) {
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        // TODO: I think startTime() should become part of ActionRequest and that should be used both for index name
        // date math expressions and $now in scripts. This way all apis will deal with now in the same way instead
        // of just for the _search api
        final List<SearchShardIterator> localShardIterators;
        final Map<String, AliasFilter> aliasFilter;
        final Map<String, Set<String>> indexRoutings;

        final String[] concreteLocalIndices;
        if (searchContext != null) {
            assert searchRequest.pointInTimeBuilder() != null;
            aliasFilter = searchContext.aliasFilter();
            indexRoutings = Collections.emptyMap();
            concreteLocalIndices = localIndices == null ? new String[0] : localIndices.indices();
            localShardIterators = getLocalLocalShardsIteratorFromPointInTime(
                clusterState,
                localIndices,
                searchRequest.getLocalClusterAlias(),
                searchContext,
                searchRequest.pointInTimeBuilder().getKeepAlive()
            );
        } else {
            final Index[] indices = resolveLocalIndices(localIndices, clusterState, timeProvider);
            Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(
                clusterState,
                searchRequest.routing(),
                searchRequest.indices()
            );
            routingMap = routingMap == null ? Collections.emptyMap() : Collections.unmodifiableMap(routingMap);
            concreteLocalIndices = new String[indices.length];
            for (int i = 0; i < indices.length; i++) {
                concreteLocalIndices[i] = indices[i].getName();
            }
            Map<String, Long> nodeSearchCounts = searchTransportService.getPendingSearchRequests();
            GroupShardsIterator<ShardIterator> localShardRoutings = clusterService.operationRouting()
                .searchShards(
                    clusterState,
                    concreteLocalIndices,
                    routingMap,
                    searchRequest.preference(),
                    searchService.getResponseCollectorService(),
                    nodeSearchCounts
                );
            localShardIterators = StreamSupport.stream(localShardRoutings.spliterator(), false)
                .map(it -> new SearchShardIterator(searchRequest.getLocalClusterAlias(), it.shardId(), it.getShardRoutings(), localIndices))
                .collect(Collectors.toList());
            aliasFilter = buildPerIndexAliasFilter(searchRequest, clusterState, indices, remoteAliasMap);
            indexRoutings = routingMap;
        }
        final GroupShardsIterator<SearchShardIterator> shardIterators = mergeShardsIterators(localShardIterators, remoteShardIterators);
        failIfOverShardCountLimit(clusterService, shardIterators.size());
        Map<String, Float> concreteIndexBoosts = resolveIndexBoosts(searchRequest, clusterState);
        // optimize search type for cases where there is only one shard group to search on
        if (shardIterators.size() == 1) {
            // if we only have one group, then we always want Q_T_F, no need for DFS, and no need to do THEN since we hit one shard
            searchRequest.searchType(QUERY_THEN_FETCH);
        }
        if (searchRequest.allowPartialSearchResults() == null) {
            // No user preference defined in search request - apply cluster service default
            searchRequest.allowPartialSearchResults(searchService.defaultAllowPartialSearchResults());
        }
        if (searchRequest.isSuggestOnly()) {
            // disable request cache if we have only suggest
            searchRequest.requestCache(false);
            switch (searchRequest.searchType()) {
                case DFS_QUERY_THEN_FETCH:
                    // convert to Q_T_F if we have only suggest
                    searchRequest.searchType(QUERY_THEN_FETCH);
                    break;
            }
        }
        final DiscoveryNodes nodes = clusterState.nodes();
        BiFunction<String, String, Transport.Connection> connectionLookup = buildConnectionLookup(
            searchRequest.getLocalClusterAlias(),
            nodes::get,
            remoteConnections,
            searchTransportService::getConnection
        );
        final Executor asyncSearchExecutor = asyncSearchExecutor(concreteLocalIndices, clusterState);
        final boolean preFilterSearchShards = shouldPreFilterSearchShards(
            clusterState,
            searchRequest,
            concreteLocalIndices,
            localShardIterators.size() + remoteShardIterators.size()
        );
        searchAsyncActionProvider.asyncSearchAction(
            task,
            searchRequest,
            asyncSearchExecutor,
            shardIterators,
            timeProvider,
            connectionLookup,
            clusterState,
            Collections.unmodifiableMap(aliasFilter),
            concreteIndexBoosts,
            indexRoutings,
            listener,
            preFilterSearchShards,
            threadPool,
            clusters,
            searchRequestContext
        ).start();
    }

    Executor asyncSearchExecutor(final String[] indices, final ClusterState clusterState) {
        final boolean onlySystemIndices = Arrays.stream(indices).allMatch(index -> {
            final IndexMetadata indexMetadata = clusterState.metadata().index(index);
            return indexMetadata != null && indexMetadata.isSystem();
        });
        return onlySystemIndices ? threadPool.executor(ThreadPool.Names.SYSTEM_READ) : threadPool.executor(ThreadPool.Names.SEARCH);
    }

    static BiFunction<String, String, Transport.Connection> buildConnectionLookup(
        String requestClusterAlias,
        Function<String, DiscoveryNode> localNodes,
        BiFunction<String, String, DiscoveryNode> remoteNodes,
        BiFunction<String, DiscoveryNode, Transport.Connection> nodeToConnection
    ) {
        return (clusterAlias, nodeId) -> {
            final DiscoveryNode discoveryNode;
            final boolean remoteCluster;
            if (clusterAlias == null || requestClusterAlias != null) {
                assert requestClusterAlias == null || requestClusterAlias.equals(clusterAlias);
                discoveryNode = localNodes.apply(nodeId);
                remoteCluster = false;
            } else {
                discoveryNode = remoteNodes.apply(clusterAlias, nodeId);
                remoteCluster = true;
            }
            if (discoveryNode == null) {
                throw new IllegalStateException("no node found for id: " + nodeId);
            }
            return nodeToConnection.apply(remoteCluster ? clusterAlias : null, discoveryNode);
        };
    }

    static boolean shouldPreFilterSearchShards(ClusterState clusterState, SearchRequest searchRequest, String[] indices, int numShards) {
        SearchSourceBuilder source = searchRequest.source();
        Integer preFilterShardSize = searchRequest.getPreFilterShardSize();
        if (preFilterShardSize == null && (hasReadOnlyIndices(indices, clusterState) || hasPrimaryFieldSort(source))) {
            preFilterShardSize = 1;
        } else if (preFilterShardSize == null) {
            preFilterShardSize = SearchRequest.DEFAULT_PRE_FILTER_SHARD_SIZE;
        }
        return searchRequest.searchType() == QUERY_THEN_FETCH // we can't do this for DFS it needs to fan out to all shards all the time
            && (SearchService.canRewriteToMatchNone(source) || hasPrimaryFieldSort(source))
            && preFilterShardSize < numShards;
    }

    private static boolean hasReadOnlyIndices(String[] indices, ClusterState clusterState) {
        for (String index : indices) {
            ClusterBlockException writeBlock = clusterState.blocks().indexBlockedException(ClusterBlockLevel.WRITE, index);
            if (writeBlock != null) {
                return true;
            }
        }
        return false;
    }

    static GroupShardsIterator<SearchShardIterator> mergeShardsIterators(
        List<SearchShardIterator> localShardIterators,
        List<SearchShardIterator> remoteShardIterators
    ) {
        List<SearchShardIterator> shards = new ArrayList<>(remoteShardIterators);
        shards.addAll(localShardIterators);
        return GroupShardsIterator.sortAndCreate(shards);
    }

    interface SearchAsyncActionProvider {
        AbstractSearchAsyncAction<? extends SearchPhaseResult> asyncSearchAction(
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
        );
    }

    private AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction(
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
            return new CanMatchPreFilterSearchPhase(
                logger,
                searchTransportService,
                connectionLookup,
                aliasFilter,
                concreteIndexBoosts,
                indexRoutings,
                executor,
                searchRequest,
                listener,
                shardIterators,
                timeProvider,
                clusterState,
                task,
                (iter) -> new WrappingSearchAsyncActionPhase(
                    searchAsyncAction(
                        task,
                        searchRequest,
                        executor,
                        iter,
                        timeProvider,
                        connectionLookup,
                        clusterState,
                        aliasFilter,
                        concreteIndexBoosts,
                        indexRoutings,
                        listener,
                        false,
                        threadPool,
                        clusters,
                        searchRequestContext
                    )
                ),
                clusters,
                searchRequestContext,
                tracer
            );
        } else {
            final QueryPhaseResultConsumer queryResultConsumer = searchPhaseController.newSearchPhaseResults(
                executor,
                circuitBreaker,
                task.getProgressListener(),
                searchRequest,
                shardIterators.size(),
                exc -> cancelTask(task, exc)
            );
            AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction;
            switch (searchRequest.searchType()) {
                case DFS_QUERY_THEN_FETCH:
                    searchAsyncAction = new SearchDfsQueryThenFetchAsyncAction(
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
                        listener,
                        shardIterators,
                        timeProvider,
                        clusterState,
                        task,
                        clusters,
                        searchRequestContext,
                        tracer
                    );
                    break;
                case QUERY_THEN_FETCH:
                    searchAsyncAction = new SearchQueryThenFetchAsyncAction(
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
                        listener,
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

    private void cancelTask(SearchTask task, Exception exc) {
        String errorMsg = exc.getMessage() != null ? exc.getMessage() : "";
        CancelTasksRequest req = new CancelTasksRequest().setTaskId(new TaskId(client.getLocalNodeId(), task.getId()))
            .setReason("Fatal failure during search: " + errorMsg);
        // force the origin to execute the cancellation as a system user
        new OriginSettingClient(client, TASKS_ORIGIN).admin().cluster().cancelTasks(req, ActionListener.wrap(() -> {}));
    }

    private static void failIfOverShardCountLimit(ClusterService clusterService, int shardCount) {
        final long shardCountLimit = clusterService.getClusterSettings().get(SHARD_COUNT_LIMIT_SETTING);
        if (shardCount > shardCountLimit) {
            throw new IllegalArgumentException(
                "Trying to query "
                    + shardCount
                    + " shards, which is over the limit of "
                    + shardCountLimit
                    + ". This limit exists because querying many shards at the same time can make the "
                    + "job of the coordinating node very CPU and/or memory intensive. It is usually a better idea to "
                    + "have a smaller number of larger shards. Update ["
                    + SHARD_COUNT_LIMIT_SETTING.getKey()
                    + "] to a greater value if you really want to query that many shards at the same time."
            );
        }
    }

    /**
     * xcluster search listener
     *
     * @opensearch.internal
     */
    abstract static class CCSActionListener<Response, FinalResponse> implements ActionListener<Response> {
        private final String clusterAlias;
        private final boolean skipUnavailable;
        private final CountDown countDown;
        private final AtomicInteger skippedClusters;
        private final AtomicReference<Exception> exceptions;
        private final ActionListener<FinalResponse> originalListener;

        CCSActionListener(
            String clusterAlias,
            boolean skipUnavailable,
            CountDown countDown,
            AtomicInteger skippedClusters,
            AtomicReference<Exception> exceptions,
            ActionListener<FinalResponse> originalListener
        ) {
            this.clusterAlias = clusterAlias;
            this.skipUnavailable = skipUnavailable;
            this.countDown = countDown;
            this.skippedClusters = skippedClusters;
            this.exceptions = exceptions;
            this.originalListener = originalListener;
        }

        @Override
        public final void onResponse(Response response) {
            innerOnResponse(response);
            maybeFinish();
        }

        abstract void innerOnResponse(Response response);

        @Override
        public final void onFailure(Exception e) {
            if (skipUnavailable) {
                skippedClusters.incrementAndGet();
            } else {
                Exception exception = e;
                if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) == false) {
                    exception = wrapRemoteClusterFailure(clusterAlias, e);
                }
                if (exceptions.compareAndSet(null, exception) == false) {
                    exceptions.accumulateAndGet(exception, (previous, current) -> {
                        current.addSuppressed(previous);
                        return current;
                    });
                }
            }
            maybeFinish();
        }

        private void maybeFinish() {
            if (countDown.countDown()) {
                Exception exception = exceptions.get();
                if (exception == null) {
                    FinalResponse response;
                    try {
                        response = createFinalResponse();
                    } catch (Exception e) {
                        originalListener.onFailure(e);
                        return;
                    }
                    originalListener.onResponse(response);
                } else {
                    originalListener.onFailure(exceptions.get());
                }
            }
        }

        abstract FinalResponse createFinalResponse();
    }

    private static RemoteTransportException wrapRemoteClusterFailure(String clusterAlias, Exception e) {
        return new RemoteTransportException("error while communicating with remote cluster [" + clusterAlias + "]", e);
    }

    static Map<String, OriginalIndices> getIndicesFromSearchContexts(SearchContextId searchContext, IndicesOptions indicesOptions) {
        final Map<String, Set<String>> indices = new HashMap<>();
        for (Map.Entry<ShardId, SearchContextIdForNode> entry : searchContext.shards().entrySet()) {
            String clusterAlias = entry.getValue().getClusterAlias() == null
                ? RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY
                : entry.getValue().getClusterAlias();
            indices.computeIfAbsent(clusterAlias, k -> new HashSet<>()).add(entry.getKey().getIndexName());
        }
        return indices.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new OriginalIndices(e.getValue().toArray(new String[0]), indicesOptions)));
    }

    static List<SearchShardIterator> getLocalLocalShardsIteratorFromPointInTime(
        ClusterState clusterState,
        OriginalIndices originalIndices,
        String localClusterAlias,
        SearchContextId searchContext,
        TimeValue keepAlive
    ) {
        final List<SearchShardIterator> iterators = new ArrayList<>(searchContext.shards().size());
        for (Map.Entry<ShardId, SearchContextIdForNode> entry : searchContext.shards().entrySet()) {
            final SearchContextIdForNode perNode = entry.getValue();
            if (Strings.isEmpty(perNode.getClusterAlias())) {
                final ShardId shardId = entry.getKey();
                OperationRouting.getShards(clusterState, shardId);
                final List<String> targetNodes = Collections.singletonList(perNode.getNode());
                iterators.add(
                    new SearchShardIterator(
                        localClusterAlias,
                        shardId,
                        targetNodes,
                        originalIndices,
                        perNode.getSearchContextId(),
                        keepAlive
                    )
                );
            }
        }
        return iterators;
    }
}

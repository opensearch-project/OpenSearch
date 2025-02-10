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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.support.TransportActions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.FailAwareWeightedRouting;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.pipeline.PipelinedRequest;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.transport.Transport;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * This is an abstract base class that encapsulates the logic to fan out to all shards in provided {@link GroupShardsIterator}
 * and collect the results. If a shard request returns a failure this class handles the advance to the next replica of the shard until
 * the shards replica iterator is exhausted. Each shard is referenced by position in the {@link GroupShardsIterator} which is later
 * referred to as the {@code shardIndex}.
 * The fan out and collect algorithm is traditionally used as the initial phase which can either be a query execution or collection of
 * distributed frequencies
 *
 * @opensearch.internal
 */
abstract class AbstractSearchAsyncAction<Result extends SearchPhaseResult> extends SearchPhase implements SearchPhaseContext {
    private static final float DEFAULT_INDEX_BOOST = 1.0f;
    private final Logger logger;
    private final SearchTransportService searchTransportService;
    private final Executor executor;
    private final ActionListener<SearchResponse> listener;
    private final SearchRequest request;
    /**
     * Used by subclasses to resolve node ids to DiscoveryNodes.
     **/
    private final BiFunction<String, String, Transport.Connection> nodeIdToConnection;
    private final SearchTask task;
    protected final SearchPhaseResults<Result> results;
    private final ClusterState clusterState;
    private final Map<String, AliasFilter> aliasFilter;
    private final Map<String, Float> concreteIndexBoosts;
    private final Map<String, Set<String>> indexRoutings;
    private final SetOnce<AtomicArray<ShardSearchFailure>> shardFailures = new SetOnce<>();
    private final Object shardFailuresMutex = new Object();
    private final AtomicBoolean hasShardResponse = new AtomicBoolean(false);
    private final AtomicInteger successfulOps = new AtomicInteger();
    private final AtomicInteger skippedOps = new AtomicInteger();
    private final TransportSearchAction.SearchTimeProvider timeProvider;
    private final SearchResponse.Clusters clusters;
    protected final GroupShardsIterator<SearchShardIterator> toSkipShardsIts;
    protected final GroupShardsIterator<SearchShardIterator> shardsIts;
    private final int expectedTotalOps;
    private final AtomicInteger totalOps = new AtomicInteger();
    private final int maxConcurrentRequestsPerNode;
    private final Map<String, PendingExecutions> pendingExecutionsPerNode = new ConcurrentHashMap<>();
    private final boolean throttleConcurrentRequests;
    private final SearchRequestContext searchRequestContext;
    private final Tracer tracer;

    private SearchPhase currentPhase;
    private boolean currentPhaseHasLifecycle;

    private final List<Releasable> releasables = new ArrayList<>();

    AbstractSearchAsyncAction(
        String name,
        Logger logger,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Map<String, Set<String>> indexRoutings,
        Executor executor,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchPhaseResults<Result> resultConsumer,
        int maxConcurrentRequestsPerNode,
        SearchResponse.Clusters clusters,
        SearchRequestContext searchRequestContext,
        Tracer tracer
    ) {
        super(name);
        final List<SearchShardIterator> toSkipIterators = new ArrayList<>();
        final List<SearchShardIterator> iterators = new ArrayList<>();
        for (final SearchShardIterator iterator : shardsIts) {
            if (iterator.skip()) {
                toSkipIterators.add(iterator);
            } else {
                iterators.add(iterator);
            }
        }
        this.toSkipShardsIts = new GroupShardsIterator<>(toSkipIterators);
        this.shardsIts = new GroupShardsIterator<>(iterators);
        // we need to add 1 for non active partition, since we count it in the total. This means for each shard in the iterator we sum up
        // it's number of active shards but use 1 as the default if no replica of a shard is active at this point.
        // on a per shards level we use shardIt.remaining() to increment the totalOps pointer but add 1 for the current shard result
        // we process hence we add one for the non active partition here.
        this.expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();
        this.maxConcurrentRequestsPerNode = maxConcurrentRequestsPerNode;
        // in the case were we have less shards than maxConcurrentRequestsPerNode we don't need to throttle
        this.throttleConcurrentRequests = maxConcurrentRequestsPerNode < shardsIts.size();
        this.timeProvider = timeProvider;
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.executor = executor;
        this.request = request;
        this.task = task;
        this.listener = ActionListener.runAfter(listener, this::releaseContext);
        this.nodeIdToConnection = nodeIdToConnection;
        this.clusterState = clusterState;
        this.concreteIndexBoosts = concreteIndexBoosts;
        this.aliasFilter = aliasFilter;
        this.indexRoutings = indexRoutings;
        this.results = resultConsumer;
        this.clusters = clusters;
        this.searchRequestContext = searchRequestContext;
        this.tracer = tracer;
    }

    @Override
    public void addReleasable(Releasable releasable) {
        releasables.add(releasable);
    }

    public void releaseContext() {
        Releasables.close(releasables);
    }

    /**
     * Builds how long it took to execute the search.
     */
    long buildTookInMillis() {
        return timeProvider.buildTookInMillis();
    }

    /**
     * This is the main entry point for a search. This method starts the search execution of the initial phase.
     */
    public final void start() {
        if (getNumShards() == 0) {
            // no search shards to search on, bail with empty response
            // (it happens with search across _all with no indices around and consistent with broadcast operations)
            int trackTotalHitsUpTo = request.source() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : request.source().trackTotalHitsUpTo() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : request.source().trackTotalHitsUpTo();
            // total hits is null in the response if the tracking of total hits is disabled
            boolean withTotalHits = trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
            listener.onResponse(
                new SearchResponse(
                    InternalSearchResponse.empty(withTotalHits),
                    null,
                    0,
                    0,
                    0,
                    buildTookInMillis(),
                    searchRequestContext.getPhaseTook(),
                    ShardSearchFailure.EMPTY_ARRAY,
                    clusters,
                    null
                )
            );
            onRequestEnd(searchRequestContext);
            return;
        }
        executePhase(this);
    }

    @Override
    public final void run() {
        for (final SearchShardIterator iterator : toSkipShardsIts) {
            assert iterator.skip();
            skipShard(iterator);
        }
        if (shardsIts.size() > 0) {
            assert request.allowPartialSearchResults() != null : "SearchRequest missing setting for allowPartialSearchResults";
            if (request.allowPartialSearchResults() == false) {
                final StringBuilder missingShards = new StringBuilder();
                // Fail-fast verification of all shards being available
                for (int index = 0; index < shardsIts.size(); index++) {
                    final SearchShardIterator shardRoutings = shardsIts.get(index);
                    if (shardRoutings.size() == 0) {
                        if (missingShards.length() > 0) {
                            missingShards.append(", ");
                        }
                        missingShards.append(shardRoutings.shardId());
                    }
                }
                if (missingShards.length() > 0) {
                    // Status red - shard is missing all copies and would produce partial results for an index search
                    final String msg = "Search rejected due to missing shards ["
                        + missingShards
                        + "]. Consider using `allow_partial_search_results` setting to bypass this error.";
                    throw new SearchPhaseExecutionException(getName(), msg, null, ShardSearchFailure.EMPTY_ARRAY);
                }
            }
            for (int index = 0; index < shardsIts.size(); index++) {
                final SearchShardIterator shardRoutings = shardsIts.get(index);
                assert shardRoutings.skip() == false;
                performPhaseOnShard(index, shardRoutings, shardRoutings.nextOrNull());
            }
        }
    }

    void skipShard(SearchShardIterator iterator) {
        successfulOps.incrementAndGet();
        skippedOps.incrementAndGet();
        assert iterator.skip();
        successfulShardExecution(iterator);
    }

    private void performPhaseOnShard(final int shardIndex, final SearchShardIterator shardIt, final SearchShardTarget shard) {
        /*
         * We capture the thread that this phase is starting on. When we are called back after executing the phase, we are either on the
         * same thread (because we never went async, or the same thread was selected from the thread pool) or a different thread. If we
         * continue on the same thread in the case that we never went async and this happens repeatedly we will end up recursing deeply and
         * could stack overflow. To prevent this, we fork if we are called back on the same thread that execution started on and otherwise
         * we can continue (cf. InitialSearchPhase#maybeFork).
         */
        if (shard == null) {
            fork(() -> onShardFailure(shardIndex, null, shardIt, new NoShardAvailableActionException(shardIt.shardId())));
        } else {
            final PendingExecutions pendingExecutions = throttleConcurrentRequests
                ? pendingExecutionsPerNode.computeIfAbsent(shard.getNodeId(), n -> new PendingExecutions(maxConcurrentRequestsPerNode))
                : null;
            Runnable r = () -> {
                final Thread thread = Thread.currentThread();
                try {
                    final SearchPhase phase = this;
                    executePhaseOnShard(shardIt, shard, new SearchActionListener<Result>(shard, shardIndex) {
                        @Override
                        public void innerOnResponse(Result result) {
                            try {
                                onShardResult(result, shardIt);
                            } finally {
                                executeNext(pendingExecutions, thread);
                            }
                        }

                        @Override
                        public void onFailure(Exception t) {
                            try {
                                // It only happens when onPhaseDone() is called and executePhaseOnShard() fails hard with an exception.
                                if (totalOps.get() == expectedTotalOps) {
                                    onPhaseFailure(phase, "The phase has failed", t);
                                } else {
                                    onShardFailure(shardIndex, shard, shardIt, t);
                                }
                            } finally {
                                executeNext(pendingExecutions, thread);
                            }
                        }
                    });
                } catch (final Exception e) {
                    try {
                        /*
                         * It is possible to run into connection exceptions here because we are getting the connection early and might
                         * run into nodes that are not connected. In this case, on shard failure will move us to the next shard copy.
                         */
                        fork(() -> {
                            // It only happens when onPhaseDone() is called and executePhaseOnShard() fails hard with an exception.
                            // In this case calling onShardFailure() would overflow the operations counter, so the best we could do
                            // here is to fail the phase and move on to the next one.
                            if (totalOps.get() == expectedTotalOps) {
                                onPhaseFailure(this, "The phase has failed", e);
                            } else {
                                onShardFailure(shardIndex, shard, shardIt, e);
                            }
                        });
                    } finally {
                        executeNext(pendingExecutions, thread);
                    }
                }
            };
            if (throttleConcurrentRequests) {
                pendingExecutions.tryRun(r);
            } else {
                r.run();
            }
        }
    }

    /**
     * Sends the request to the actual shard.
     * @param shardIt the shards iterator
     * @param shard the shard routing to send the request for
     * @param listener the listener to notify on response
     */
    protected abstract void executePhaseOnShard(
        SearchShardIterator shardIt,
        SearchShardTarget shard,
        SearchActionListener<Result> listener
    );

    private void fork(final Runnable runnable) {
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {

            }

            @Override
            protected void doRun() {
                runnable.run();
            }

            @Override
            public boolean isForceExecution() {
                // we can not allow a stuffed queue to reject execution here
                return true;
            }
        });
    }

    @Override
    public final void executeNextPhase(SearchPhase currentPhase, SearchPhase nextPhase) {
        /* This is the main search phase transition where we move to the next phase. At this point we check if there is
         * at least one successful operation left and if so we move to the next phase. If not we immediately fail the
         * search phase as "all shards failed"*/
        if (successfulOps.get() == 0) { // we have 0 successful results that means we shortcut stuff and return a failure
            final ShardOperationFailedException[] shardSearchFailures = ExceptionsHelper.groupBy(buildShardFailures());
            Throwable cause = shardSearchFailures.length == 0
                ? null
                : OpenSearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
            logger.debug(() -> new ParameterizedMessage("All shards failed for phase: [{}]", getName()), cause);
            onPhaseFailure(currentPhase, "all shards failed", cause);

        } else {
            Boolean allowPartialResults = request.allowPartialSearchResults();
            assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
            if (allowPartialResults == false && successfulOps.get() != getNumShards()) {
                // check if there are actual failures in the atomic array since
                // successful retries can reset the failures to null
                ShardOperationFailedException[] shardSearchFailures = buildShardFailures();
                if (shardSearchFailures.length > 0) {
                    if (logger.isDebugEnabled()) {
                        int numShardFailures = shardSearchFailures.length;
                        shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
                        Throwable cause = OpenSearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
                        logger.debug(
                            () -> new ParameterizedMessage("{} shards failed for phase: [{}]", numShardFailures, getName()),
                            cause
                        );
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure", null);
                    return;
                } else {
                    int discrepancy = getNumShards() - successfulOps.get();
                    assert discrepancy > 0 : "discrepancy: " + discrepancy;
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Partial shards failure (unavailable: {}, successful: {}, skipped: {}, num-shards: {}, phase: {})",
                            discrepancy,
                            successfulOps.get(),
                            skippedOps.get(),
                            getNumShards(),
                            currentPhase.getName()
                        );
                    }
                    if (!request.indicesOptions().ignoreUnavailable()) {
                        onPhaseFailure(currentPhase, "Partial shards failure (" + discrepancy + " shards unavailable)", null);
                        return;
                    }
                }
            }
            if (logger.isTraceEnabled()) {
                final String resultsFrom = results.getSuccessfulResults()
                    .map(r -> r.getSearchShardTarget().toString())
                    .collect(Collectors.joining(","));
                logger.trace(
                    "[{}] Moving to next phase: [{}], based on results from: {} (cluster state version: {})",
                    currentPhase.getName(),
                    nextPhase.getName(),
                    resultsFrom,
                    clusterState.version()
                );
            }
            onPhaseEnd(searchRequestContext);
            executePhase(nextPhase);
        }
    }

    private void onPhaseEnd(SearchRequestContext searchRequestContext) {
        if (getCurrentPhase() != null) {
            long tookInNanos = System.nanoTime() - getCurrentPhase().getStartTimeInNanos();
            searchRequestContext.updatePhaseTookMap(getCurrentPhase().getName(), TimeUnit.NANOSECONDS.toMillis(tookInNanos));
        }
        if (currentPhaseHasLifecycle) {
            this.searchRequestContext.getSearchRequestOperationsListener().onPhaseEnd(this, searchRequestContext);
        }
    }

    private void onPhaseStart(SearchPhase phase) {
        setCurrentPhase(phase);
        if (currentPhaseHasLifecycle) {
            this.searchRequestContext.getSearchRequestOperationsListener().onPhaseStart(this);
        }
    }

    private void onRequestEnd(SearchRequestContext searchRequestContext) {
        this.searchRequestContext.getSearchRequestOperationsListener().onRequestEnd(this, searchRequestContext);
    }

    private void onRequestFailure(SearchRequestContext searchRequestContext) {
        this.searchRequestContext.getSearchRequestOperationsListener().onRequestFailure(this, searchRequestContext);
    }

    private void executePhase(SearchPhase phase) {
        Span phaseSpan = tracer.startSpan(SpanCreationContext.server().name("[phase/" + phase.getName() + "]"));
        try (final SpanScope scope = tracer.withSpanInScope(phaseSpan)) {
            onPhaseStart(phase);
            phase.recordAndRun();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(new ParameterizedMessage("Failed to execute [{}] while moving to [{}] phase", request, phase.getName()), e);
            }

            if (currentPhaseHasLifecycle == false) {
                phaseSpan.setError(e);
            }

            onPhaseFailure(phase, "", e);
        } finally {
            if (currentPhaseHasLifecycle == false) {
                phaseSpan.endSpan();
            }
        }
    }

    ShardSearchFailure[] buildShardFailures() {
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
        if (shardFailures == null) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        List<ShardSearchFailure> entries = shardFailures.asList();
        ShardSearchFailure[] failures = new ShardSearchFailure[entries.size()];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = entries.get(i);
        }
        return failures;
    }

    private void onShardFailure(final int shardIndex, @Nullable SearchShardTarget shard, final SearchShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        setPhaseResourceUsages();
        onShardFailure(shardIndex, shard, e);
        SearchShardTarget nextShard = FailAwareWeightedRouting.getInstance()
            .findNext(shardIt, clusterState, e, () -> totalOps.incrementAndGet());

        final boolean lastShard = nextShard == null;
        if (logger.isTraceEnabled()) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "{}: Failed to execute [{}] lastShard [{}]",
                    shard != null ? shard : shardIt.shardId(),
                    request,
                    lastShard
                ),
                e
            );
        } else {
            // Log the message without an exception.
            logger.debug(
                new ParameterizedMessage(
                    "{}: Failed to execute [{}] lastShard [{}]",
                    shard != null ? shard : shardIt.shardId(),
                    request,
                    lastShard
                )
            );
        }
        if (lastShard) {
            onShardGroupFailure(shardIndex, shard, e);
        }
        final int totalOps = this.totalOps.incrementAndGet();
        if (totalOps == expectedTotalOps) {
            try {
                onPhaseDone();
            } catch (final Exception ex) {
                onPhaseFailure(this, "The phase has failed", ex);
            }
        } else if (totalOps > expectedTotalOps) {
            throw new AssertionError(
                "unexpected higher total ops [" + totalOps + "] compared to expected [" + expectedTotalOps + "]",
                new SearchPhaseExecutionException(getName(), "Shard failures", null, buildShardFailures())
            );
        } else {
            if (lastShard == false) {
                performPhaseOnShard(shardIndex, shardIt, nextShard);
            }
        }
    }

    /**
     * Executed once for every {@link ShardId} that failed on all available shard routing.
     *
     * @param shardIndex the shard index that failed
     * @param shardTarget the last shard target for this failure
     * @param exc the last failure reason
     */
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {}

    /**
     * Executed once for every failed shard level request. This method is invoked before the next replica is tried for the given
     * shard target.
     * @param shardIndex the internal index for this shard. Each shard has an index / ordinal assigned that is used to reference
     *                   it's results
     * @param shardTarget the shard target for this failure
     * @param e the failure reason
     */
    @Override
    public final void onShardFailure(final int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
        // we don't aggregate shard failures on non active shards (but do keep the header counts right)
        if (TransportActions.isShardNotAvailableException(e) == false) {
            AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
            // lazily create shard failures, so we can early build the empty shard failure list in most cases (no failures)
            if (shardFailures == null) { // this is double checked locking but it's fine since SetOnce uses a volatile read internally
                synchronized (shardFailuresMutex) {
                    shardFailures = this.shardFailures.get(); // read again otherwise somebody else has created it?
                    if (shardFailures == null) { // still null so we are the first and create a new instance
                        shardFailures = new AtomicArray<>(getNumShards());
                        this.shardFailures.set(shardFailures);
                    }
                }
            }
            ShardSearchFailure failure = shardFailures.get(shardIndex);
            if (failure == null) {
                shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
            } else {
                // the failure is already present, try and not override it with an exception that is less meaningless
                // for example, getting illegal shard state
                if (TransportActions.isReadOverrideException(e)) {
                    shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
                }
            }

            if (results.hasResult(shardIndex)) {
                assert failure == null : "shard failed before but shouldn't: " + failure;
                successfulOps.decrementAndGet(); // if this shard was successful before (initial phase) we have to adjust the counter
            }
        }
        results.consumeShardFailure(shardIndex);
    }

    /**
     * Executed once for every successful shard level request.
     * @param result the result returned form the shard
     * @param shardIt the shard iterator
     */
    protected void onShardResult(Result result, SearchShardIterator shardIt) {
        assert result.getShardIndex() != -1 : "shard index is not set";
        assert result.getSearchShardTarget() != null : "search shard target must not be null";
        hasShardResponse.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("got first-phase result from {}", result != null ? result.getSearchShardTarget() : null);
        }
        this.setPhaseResourceUsages();
        results.consumeResult(result, () -> onShardResultConsumed(result, shardIt));
    }

    public void setPhaseResourceUsages() {
        TaskResourceInfo taskResourceUsage = searchRequestContext.getTaskResourceUsageSupplier().get();
        searchRequestContext.recordPhaseResourceUsage(taskResourceUsage);
    }

    private void onShardResultConsumed(Result result, SearchShardIterator shardIt) {
        successfulOps.incrementAndGet();
        // clean a previous error on this shard group (note, this code will be serialized on the same shardIndex value level
        // so its ok concurrency wise to miss potentially the shard failures being created because of another failure
        // in the #addShardFailure, because by definition, it will happen on *another* shardIndex
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
        if (shardFailures != null) {
            shardFailures.set(result.getShardIndex(), null);
        }
        // we need to increment successful ops first before we compare the exit condition otherwise if we
        // are fast we could concurrently update totalOps but then preempt one of the threads which can
        // cause the successor to read a wrong value from successfulOps if second phase is very fast ie. count etc.
        // increment all the "future" shards to update the total ops since we some may work and some may not...
        // and when that happens, we break on total ops, so we must maintain them
        successfulShardExecution(shardIt);
    }

    private void successfulShardExecution(SearchShardIterator shardsIt) {
        final int remainingOpsOnIterator;
        if (shardsIt.skip()) {
            remainingOpsOnIterator = shardsIt.remaining();
        } else {
            remainingOpsOnIterator = shardsIt.remaining() + 1;
        }
        final int xTotalOps = totalOps.addAndGet(remainingOpsOnIterator);
        if (xTotalOps == expectedTotalOps) {
            try {
                onPhaseDone();
            } catch (final Exception ex) {
                onPhaseFailure(this, "The phase has failed", ex);
            }
        } else if (xTotalOps > expectedTotalOps) {
            throw new AssertionError(
                "unexpected higher total ops [" + xTotalOps + "] compared to expected [" + expectedTotalOps + "]",
                new SearchPhaseExecutionException(getName(), "Shard failures", null, buildShardFailures())
            );
        }
    }

    public SearchPhase getCurrentPhase() {
        return currentPhase;
    }

    private void setCurrentPhase(SearchPhase phase) {
        currentPhase = phase;
        // The WrappingSearchAsyncActionPhase (see please CanMatchPreFilterSearchPhase as one example) is a special case
        // of search phase that wraps SearchAsyncActionPhase as SearchPhase. The AbstractSearchAsyncAction manages own
        // onPhaseStart / onPhaseFailure / OnPhaseDone callbacks and the wrapping SearchPhase is being abandoned
        // (fe, has no onPhaseEnd callbacks called ever). To fix that, we would not send any notifications for this
        // phase.
        currentPhaseHasLifecycle = ((phase instanceof WrappingSearchAsyncActionPhase) == false);
    }

    @Override
    public final int getNumShards() {
        return results.getNumShards();
    }

    @Override
    public final Logger getLogger() {
        return logger;
    }

    @Override
    public final SearchTask getTask() {
        return task;
    }

    @Override
    public final SearchRequest getRequest() {
        return request;
    }

    protected final SearchResponse buildSearchResponse(
        InternalSearchResponse internalSearchResponse,
        ShardSearchFailure[] failures,
        String scrollId,
        String searchContextId
    ) {
        return new SearchResponse(
            internalSearchResponse,
            scrollId,
            getNumShards(),
            successfulOps.get(),
            skippedOps.get(),
            buildTookInMillis(),
            searchRequestContext.getPhaseTook(),
            failures,
            clusters,
            searchContextId
        );
    }

    boolean buildPointInTimeFromSearchResults() {
        // TODO: Until we implement the retry mechanism for point in times (i.e., replace an unavailable shard with an equivalent copy),
        // we can simply return the point in time of the search request.
        return false;
    }

    @Override
    public void sendSearchResponse(InternalSearchResponse internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        ShardSearchFailure[] failures = buildShardFailures();
        Boolean allowPartialResults = request.allowPartialSearchResults();
        assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (allowPartialResults == false && failures.length > 0) {
            raisePhaseFailure(new SearchPhaseExecutionException("", "Shard failures", null, failures));
        } else {
            final Version minNodeVersion = clusterState.nodes().getMinNodeVersion();
            final String scrollId = request.scroll() != null ? TransportSearchHelper.buildScrollId(queryResults, minNodeVersion) : null;
            final String searchContextId;
            if (buildPointInTimeFromSearchResults()) {
                searchContextId = SearchContextId.encode(queryResults.asList(), aliasFilter, minNodeVersion);
            } else {
                if (request.source() != null && request.source().pointInTimeBuilder() != null) {
                    searchContextId = request.source().pointInTimeBuilder().getId();
                } else {
                    searchContextId = null;
                }
            }
            searchRequestContext.setTotalHits(internalSearchResponse.hits().getTotalHits());
            searchRequestContext.setShardStats(results.getNumShards(), successfulOps.get(), skippedOps.get(), failures.length);
            searchRequestContext.setSuccessfulSearchShardIndices(
                results.getSuccessfulResults()
                    .map(result -> result.getSearchShardTarget().getShardId().getIndex())
                    .collect(Collectors.toSet())
            );
            onPhaseEnd(searchRequestContext);
            onRequestEnd(searchRequestContext);
            listener.onResponse(buildSearchResponse(internalSearchResponse, failures, scrollId, searchContextId));
        }
        setCurrentPhase(null);
    }

    @Override
    public final void onPhaseFailure(SearchPhase phase, String msg, Throwable cause) {
        setPhaseResourceUsages();
        if (currentPhaseHasLifecycle) {
            this.searchRequestContext.getSearchRequestOperationsListener().onPhaseFailure(this, cause);
        }
        raisePhaseFailure(new SearchPhaseExecutionException(phase.getName(), msg, cause, buildShardFailures()));
    }

    /**
     * This method should be called if a search phase failed to ensure all relevant reader contexts are released.
     * This method will also notify the listener and sends back a failure to the user.
     *
     * @param exception the exception explaining or causing the phase failure
     */
    private void raisePhaseFailure(SearchPhaseExecutionException exception) {
        // we don't release persistent readers (point in time).
        if (request.pointInTimeBuilder() == null) {
            results.getSuccessfulResults().forEach((entry) -> {
                if (entry.getContextId() != null) {
                    try {
                        SearchShardTarget searchShardTarget = entry.getSearchShardTarget();
                        Transport.Connection connection = getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
                        sendReleaseSearchContext(entry.getContextId(), connection, searchShardTarget.getOriginalIndices());
                    } catch (Exception inner) {
                        inner.addSuppressed(exception);
                        logger.trace("failed to release context", inner);
                    }
                }
            });
        }
        Releasables.close(releasables);
        onRequestFailure(searchRequestContext);
        listener.onFailure(exception);
    }

    /**
     * Executed once all shard results have been received and processed
     * @see #onShardFailure(int, SearchShardTarget, Exception)
     * @see #onShardResult(SearchPhaseResult, SearchShardIterator)
     */
    final void onPhaseDone() {  // as a tribute to @kimchy aka. finishHim()
        final SearchPhase nextPhase = getNextPhase(results, this);
        if (request instanceof PipelinedRequest && nextPhase != null) {
            ((PipelinedRequest) request).transformSearchPhaseResults(results, this, this.getName(), nextPhase.getName());
        }
        executeNextPhase(this, nextPhase);
    }

    @Override
    public final Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return nodeIdToConnection.apply(clusterAlias, nodeId);
    }

    @Override
    public final SearchTransportService getSearchTransport() {
        return searchTransportService;
    }

    @Override
    public final void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public final void onFailure(Exception e) {
        listener.onFailure(e);
    }

    @Override
    public final ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt) {
        AliasFilter filter = aliasFilter.get(shardIt.shardId().getIndex().getUUID());
        assert filter != null;
        float indexBoost = concreteIndexBoosts.getOrDefault(shardIt.shardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST);
        String indexName = shardIt.shardId().getIndex().getName();
        final String[] routings = indexRoutings.getOrDefault(indexName, Collections.emptySet()).toArray(new String[0]);
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            shardIt.getOriginalIndices(),
            request,
            shardIt.shardId(),
            getNumShards(),
            filter,
            indexBoost,
            timeProvider.getAbsoluteStartMillis(),
            shardIt.getClusterAlias(),
            routings,
            shardIt.getSearchContextId(),
            shardIt.getSearchContextKeepAlive()
        );
        // if we already received a search result we can inform the shard that it
        // can return a null response if the request rewrites to match none rather
        // than creating an empty response in the search thread pool.
        // Note that, we have to disable this shortcut for queries that create a context (scroll and search context).
        shardRequest.canReturnNullResponseIfMatchNoDocs(hasShardResponse.get() && shardRequest.scroll() == null);
        return shardRequest;
    }

    /**
     * Returns the next phase based on the results of the initial search phase
     * @param results the results of the initial search phase. Each non null element in the result array represent a successfully
     *                executed shard request
     * @param context the search context for the next phase
     */
    protected abstract SearchPhase getNextPhase(SearchPhaseResults<Result> results, SearchPhaseContext context);

    private void executeNext(PendingExecutions pendingExecutions, Thread originalThread) {
        executeNext(pendingExecutions == null ? null : pendingExecutions::finishAndRunNext, originalThread);
    }

    void executeNext(Runnable runnable, Thread originalThread) {
        if (throttleConcurrentRequests) {
            if (originalThread == Thread.currentThread()) {
                fork(runnable);
            } else {
                runnable.run();
            }
        } else {
            assert runnable == null;
        }
    }

    /**
     * Pending Executions
     *
     * @opensearch.internal
     */
    private static final class PendingExecutions {
        private final int permits;
        private int permitsTaken = 0;
        private ArrayDeque<Runnable> queue = new ArrayDeque<>();

        PendingExecutions(int permits) {
            assert permits > 0 : "not enough permits: " + permits;
            this.permits = permits;
        }

        void finishAndRunNext() {
            synchronized (this) {
                permitsTaken--;
                assert permitsTaken >= 0 : "illegal taken permits: " + permitsTaken;
            }
            tryRun(null);
        }

        void tryRun(Runnable runnable) {
            Runnable r = tryQueue(runnable);
            if (r != null) {
                r.run();
            }
        }

        private synchronized Runnable tryQueue(Runnable runnable) {
            Runnable toExecute = null;
            if (permitsTaken < permits) {
                permitsTaken++;
                toExecute = runnable;
                if (toExecute == null) { // only poll if we don't have anything to execute
                    toExecute = queue.poll();
                }
                if (toExecute == null) {
                    permitsTaken--;
                }
            } else if (runnable != null) {
                queue.add(runnable);
            }
            return toExecute;
        }
    }
}

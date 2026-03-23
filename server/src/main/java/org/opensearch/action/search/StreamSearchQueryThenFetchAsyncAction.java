/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.transport.Transport;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * Stream search async action for query then fetch mode
 */
public class StreamSearchQueryThenFetchAsyncAction extends SearchQueryThenFetchAsyncAction {

    StreamSearchQueryThenFetchAsyncAction(
        Logger logger,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Map<String, Set<String>> indexRoutings,
        SearchPhaseController searchPhaseController,
        Executor executor,
        QueryPhaseResultConsumer resultConsumer,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchResponse.Clusters clusters,
        SearchRequestContext searchRequestContext,
        Tracer tracer
    ) {
        super(
            logger,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            indexRoutings,
            searchPhaseController,
            executor,
            resultConsumer,
            request,
            listener,
            shardsIts,
            timeProvider,
            clusterState,
            task,
            clusters,
            searchRequestContext,
            tracer
        );
    }

    @Override
    SearchActionListener<SearchPhaseResult> createShardActionListener(
        final SearchShardTarget shard,
        final int shardIndex,
        final SearchShardIterator shardIt,
        final SearchPhase phase,
        final PendingExecutions pendingExecutions,
        final Thread thread
    ) {
        return new StreamSearchActionListener<SearchPhaseResult>(shard, shardIndex) {

            @Override
            protected void innerOnStreamResponse(SearchPhaseResult result) {
                try {
                    onStreamResult(result, shardIt, () -> {});
                } finally {
                    executeNext(pendingExecutions, thread);
                }
            }

            @Override
            protected void innerOnCompleteResponse(SearchPhaseResult result) {
                try {
                    onShardResult(result, shardIt);
                } finally {
                    executeNext(pendingExecutions, thread);
                }
            }

            @Override
            public void onFailure(Exception t) {
                try {
                    if (totalOps.get() == expectedTotalOps) {
                        onPhaseFailure(phase, "The phase has failed", t);
                    } else {
                        onShardFailure(shardIndex, shard, shardIt, t);
                    }
                } finally {
                    executeNext(pendingExecutions, thread);
                }
            }
        };
    }

    /**
     * Handle streaming results from shards
     */
    protected void onStreamResult(SearchPhaseResult result, SearchShardIterator shardIt, Runnable next) {
        assert result.getShardIndex() != -1 : "shard index is not set";
        assert result.getSearchShardTarget() != null : "search shard target must not be null";
        this.setPhaseResourceUsages();
        results.consumeResult(result, next);
    }

    @Override
    void successfulShardExecution(SearchShardIterator shardsIt) {
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

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TopFieldDocs;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ProtobufShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.transport.Transport;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * Async transport action for query then fetch
 *
 * @opensearch.internal
 */
class ProtobufSearchQueryThenFetchAsyncAction extends ProtobufAbstractSearchAsyncAction<SearchPhaseResult> {

    private final SearchPhaseController searchPhaseController;
    private final SearchProgressListener progressListener;

    // informations to track the best bottom top doc globally.
    private final int topDocsSize;
    private final int trackTotalHitsUpTo;
    private volatile BottomSortValuesCollector bottomSortCollector;

    ProtobufSearchQueryThenFetchAsyncAction(
        final Logger logger,
        final SearchTransportService searchTransportService,
        final BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        final Map<String, AliasFilter> aliasFilter,
        final Map<String, Float> concreteIndexBoosts,
        final Map<String, Set<String>> indexRoutings,
        final SearchPhaseController searchPhaseController,
        final Executor executor,
        final QueryPhaseResultConsumer resultConsumer,
        final ProtobufSearchRequest request,
        final ActionListener<ProtobufSearchResponse> listener,
        final GroupShardsIterator<SearchShardIterator> shardsIts,
        final ProtobufTransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        ProtobufSearchTask task,
        ProtobufSearchResponse.Clusters clusters
    ) {
        super(
            "query",
            logger,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            indexRoutings,
            executor,
            request,
            listener,
            shardsIts,
            timeProvider,
            clusterState,
            task,
            resultConsumer,
            request.getMaxConcurrentShardRequests(),
            clusters
        );
        this.topDocsSize = SearchPhaseController.getTopDocsSizeProtobuf(request);
        this.trackTotalHitsUpTo = request.resolveTrackTotalHitsUpTo();
        this.searchPhaseController = searchPhaseController;
        this.progressListener = task.getProgressListener();

        // register the release of the query consumer to free up the circuit breaker memory
        // at the end of the search
        addReleasable(resultConsumer);

        boolean hasFetchPhase = request.source() == null ? true : request.source().size() > 0;
        progressListener.notifyListShardsProtobuf(
            SearchProgressListener.buildSearchShards(this.shardsIts),
            SearchProgressListener.buildSearchShards(toSkipShardsIts),
            clusters,
            hasFetchPhase
        );
    }

    protected void executePhaseOnShard(
        final SearchShardIterator shardIt,
        final SearchShardTarget shard,
        final SearchActionListener<SearchPhaseResult> listener
    ) {
        ProtobufShardSearchRequest request = rewriteShardSearchRequest(super.buildProtobufShardSearchRequest(shardIt));
        // update inbound network time with current time before sending request over n/w to data node
        if (request != null) {
            request.setInboundNetworkTime(System.currentTimeMillis());
        }
        getSearchTransport().sendExecuteQueryProtobuf(getConnection(shard.getClusterAlias(), shard.getNodeId()), request, getProtobufTask(), listener);
    }

    @Override
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected void onShardResult(SearchPhaseResult result, SearchShardIterator shardIt) {
        QuerySearchResult queryResult = result.queryResult();
        if (queryResult.isNull() == false
            // disable sort optims for scroll requests because they keep track of the last bottom doc locally (per shard)
            && getProtobufRequest().scroll() == null
            && queryResult.topDocs() != null
            && queryResult.topDocs().topDocs.getClass() == TopFieldDocs.class) {
            TopFieldDocs topDocs = (TopFieldDocs) queryResult.topDocs().topDocs;
            if (bottomSortCollector == null) {
                synchronized (this) {
                    if (bottomSortCollector == null) {
                        bottomSortCollector = new BottomSortValuesCollector(topDocsSize, topDocs.fields);
                    }
                }
            }
            bottomSortCollector.consumeTopDocs(topDocs, queryResult.sortValueFormats());
        }
        super.onShardResult(result, shardIt);
    }

    @Override
    protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
        return new ProtobufFetchSearchPhase(results, searchPhaseController, null, this);
    }

    private ProtobufShardSearchRequest rewriteShardSearchRequest(ProtobufShardSearchRequest request) {
        if (bottomSortCollector == null) {
            return request;
        }

        // disable tracking total hits if we already reached the required estimation.
        if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_ACCURATE && bottomSortCollector.getTotalHits() > trackTotalHitsUpTo) {
            request.source(request.source().shallowCopy().trackTotalHits(false));
        }

        // set the current best bottom field doc
        if (bottomSortCollector.getBottomSortValues() != null) {
            // request.setBottomSortValues(bottomSortCollector.getBottomSortValues());
            System.out.println("Bottom sort values is not null......now what????");
        }
        return request;
    }
}

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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.search.stream.StreamSearchResult;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * Async transport action for query then fetch
 *
 * @opensearch.internal
 */
class StreamAsyncAction extends SearchQueryThenFetchAsyncAction {

    public StreamAsyncAction(
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
    protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
        return new StreamSearchReducePhase(SearchPhaseName.STREAM_REDUCE.getName(), context);
    }

    class StreamSearchReducePhase extends SearchPhase {
        private SearchPhaseContext context;

        protected StreamSearchReducePhase(String name, SearchPhaseContext context) {
            super(name);
            this.context = context;
        }

        @Override
        public void run() {
            context.execute(new StreamReduceAction(context, this));
        }
    };

    class StreamReduceAction extends AbstractRunnable {
        private SearchPhaseContext context;
        private SearchPhase phase;

        StreamReduceAction(SearchPhaseContext context, SearchPhase phase) {
            this.context = context;

        }

        @Override
        protected void doRun() throws Exception {
            List<OSTicket> tickets = new ArrayList<>();
            for (SearchPhaseResult entry : results.getAtomicArray().asList()) {
                if (entry instanceof StreamSearchResult) {
                    tickets.addAll(((StreamSearchResult) entry).getFlightTickets());
                }
            }
            InternalSearchResponse internalSearchResponse = new InternalSearchResponse(
                SearchHits.empty(),
                null,
                null,
                null,
                false,
                false,
                1,
                Collections.emptyList(),
                tickets
            );
            context.sendSearchResponse(internalSearchResponse, results.getAtomicArray());
        }

        @Override
        public void onFailure(Exception e) {
            context.onPhaseFailure(phase, "", e);
        }
    }
}

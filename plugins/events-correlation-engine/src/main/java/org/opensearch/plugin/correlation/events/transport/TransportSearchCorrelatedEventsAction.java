/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.transport;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.correlation.core.index.query.CorrelationQueryBuilder;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsAction;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsRequest;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsResponse;
import org.opensearch.plugin.correlation.events.model.Correlation;
import org.opensearch.plugin.correlation.events.model.EventWithScore;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Transport Action for searching correlated events of a particular event
 *
 * @opensearch.internal
 */
public class TransportSearchCorrelatedEventsAction extends HandledTransportAction<
    SearchCorrelatedEventsRequest,
    SearchCorrelatedEventsResponse> {

    private final Client client;

    /**
     * Parameterized ctor for Transport Action
     * @param transportService TransportService
     * @param client OS client
     * @param actionFilters ActionFilters
     */
    @Inject
    public TransportSearchCorrelatedEventsAction(TransportService transportService, Client client, ActionFilters actionFilters) {
        super(SearchCorrelatedEventsAction.NAME, transportService, actionFilters, SearchCorrelatedEventsRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, SearchCorrelatedEventsRequest request, ActionListener<SearchCorrelatedEventsResponse> listener) {
        AsyncSearchCorrelatedEventsAction asyncAction = new AsyncSearchCorrelatedEventsAction(request, listener);
        asyncAction.start();
    }

    class AsyncSearchCorrelatedEventsAction {

        private SearchCorrelatedEventsRequest request;
        private ActionListener<SearchCorrelatedEventsResponse> listener;

        AsyncSearchCorrelatedEventsAction(SearchCorrelatedEventsRequest request, ActionListener<SearchCorrelatedEventsResponse> listener) {
            this.request = request;
            this.listener = listener;
        }

        void start() {
            String index = request.getIndex();
            String event = request.getEvent();
            String timestampField = request.getTimestampField();
            Long timeWindow = request.getTimeWindow();
            Integer nearbyEvents = request.getNearbyEvents();

            MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("_id", event);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(queryBuilder);
            searchSourceBuilder.fetchSource(false);
            searchSourceBuilder.fetchField(timestampField);
            searchSourceBuilder.size(1);

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            searchRequest.source(searchSourceBuilder);

            client.search(searchRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {
                    if (response.isTimedOut()) {
                        onFailures(new OpenSearchStatusException(response.toString(), RestStatus.REQUEST_TIMEOUT));
                    }
                    if (response.getHits().getTotalHits().value != 1) {
                        onFailures(new OpenSearchStatusException("Event not found", RestStatus.INTERNAL_SERVER_ERROR));
                    }

                    SearchHit hit = response.getHits().getAt(0);
                    long eventTimestamp = hit.getFields().get(timestampField).<Long>getValue();

                    BoolQueryBuilder scoreQueryBuilder = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("score_timestamp", 0L));
                    SearchSourceBuilder scoreSearchSourceBuilder = new SearchSourceBuilder();
                    scoreSearchSourceBuilder.query(scoreQueryBuilder);
                    scoreSearchSourceBuilder.fetchSource(true);
                    scoreSearchSourceBuilder.size(1);

                    SearchRequest scoreSearchRequest = new SearchRequest();
                    scoreSearchRequest.indices(Correlation.CORRELATION_HISTORY_INDEX);
                    scoreSearchRequest.source(scoreSearchSourceBuilder);

                    client.search(scoreSearchRequest, new ActionListener<>() {
                        @Override
                        public void onResponse(SearchResponse response) {
                            if (response.isTimedOut()) {
                                onFailures(new OpenSearchStatusException(response.toString(), RestStatus.REQUEST_TIMEOUT));
                            }
                            if (response.getHits().getTotalHits().value != 1) {
                                onFailures(new OpenSearchStatusException("Score Root Record not found", RestStatus.INTERNAL_SERVER_ERROR));
                            }

                            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
                            assert source != null;

                            long scoreTimestamp;
                            if (source.get("score_timestamp") instanceof Integer) {
                                scoreTimestamp = ((Integer) source.get("score_timestamp")).longValue();
                            } else {
                                scoreTimestamp = (long) source.get("score_timestamp");
                            }

                            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                                .must(QueryBuilders.matchQuery("event1", event))
                                .must(QueryBuilders.matchQuery("event2", ""));

                            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                            searchSourceBuilder.query(queryBuilder);
                            searchSourceBuilder.fetchSource(false);
                            searchSourceBuilder.fetchField("level");
                            searchSourceBuilder.size(1);

                            SearchRequest searchRequest = new SearchRequest();
                            searchRequest.indices(Correlation.CORRELATION_HISTORY_INDEX);
                            searchRequest.source(searchSourceBuilder);

                            client.search(searchRequest, new ActionListener<>() {
                                @Override
                                public void onResponse(SearchResponse response) {
                                    if (response.isTimedOut()) {
                                        onFailures(new OpenSearchStatusException(response.toString(), RestStatus.REQUEST_TIMEOUT));
                                    }
                                    if (response.getHits().getTotalHits().value != 1) {
                                        onFailures(
                                            new OpenSearchStatusException(
                                                "Event not found in Correlation Index",
                                                RestStatus.INTERNAL_SERVER_ERROR
                                            )
                                        );
                                    }

                                    SearchHit hit = response.getHits().getHits()[0];
                                    long level = hit.getFields().get("level").<Long>getValue();
                                    float[] query = new float[3];
                                    for (int i = 0; i < 2; ++i) {
                                        query[i] = (2.0f * ((float) level) - 50.0f) / 2.0f;
                                    }
                                    query[2] = Long.valueOf((eventTimestamp - scoreTimestamp) / 1000L).floatValue();

                                    CorrelationQueryBuilder correlationQueryBuilder = new CorrelationQueryBuilder(
                                        "corr_vector",
                                        query,
                                        nearbyEvents,
                                        QueryBuilders.boolQuery()
                                            .mustNot(QueryBuilders.matchQuery("event1", ""))
                                            .mustNot(QueryBuilders.matchQuery("event2", ""))
                                            .filter(
                                                QueryBuilders.rangeQuery("timestamp")
                                                    .gte(eventTimestamp - timeWindow)
                                                    .lte(eventTimestamp + timeWindow)
                                            )
                                    );

                                    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                                    searchSourceBuilder.query(correlationQueryBuilder);
                                    searchSourceBuilder.fetchSource(true);
                                    searchSourceBuilder.size(nearbyEvents);

                                    SearchRequest searchRequest = new SearchRequest();
                                    searchRequest.indices(Correlation.CORRELATION_HISTORY_INDEX);
                                    searchRequest.source(searchSourceBuilder);

                                    client.search(searchRequest, new ActionListener<>() {
                                        @Override
                                        public void onResponse(SearchResponse response) {
                                            if (response.isTimedOut()) {
                                                onFailures(new OpenSearchStatusException(response.toString(), RestStatus.REQUEST_TIMEOUT));
                                            }
                                            SearchHit[] hits = response.getHits().getHits();
                                            Map<String, Double> correlatedFindings = new HashMap<>();

                                            for (SearchHit hit : hits) {
                                                Map<String, Object> source = hit.getSourceAsMap();
                                                assert source != null;
                                                if (!source.get("event1").toString().equals(event)) {
                                                    String eventKey1 = source.get("event1").toString()
                                                        + " "
                                                        + source.get("index1").toString();

                                                    if (correlatedFindings.containsKey(eventKey1)) {
                                                        correlatedFindings.put(
                                                            eventKey1,
                                                            Math.max(correlatedFindings.get(eventKey1), hit.getScore())
                                                        );
                                                    } else {
                                                        correlatedFindings.put(eventKey1, (double) hit.getScore());
                                                    }
                                                }
                                                if (!source.get("event2").toString().equals(event)) {
                                                    String eventKey2 = source.get("event2").toString()
                                                        + " "
                                                        + source.get("index2").toString();

                                                    if (correlatedFindings.containsKey(eventKey2)) {
                                                        correlatedFindings.put(
                                                            eventKey2,
                                                            Math.max(correlatedFindings.get(eventKey2), hit.getScore())
                                                        );
                                                    } else {
                                                        correlatedFindings.put(eventKey2, (double) hit.getScore());
                                                    }
                                                }
                                            }

                                            List<EventWithScore> eventWithScores = new ArrayList<>();
                                            for (Map.Entry<String, Double> correlatedFinding : correlatedFindings.entrySet()) {
                                                String[] eventIndexSplit = correlatedFinding.getKey().split(" ");
                                                eventWithScores.add(
                                                    new EventWithScore(
                                                        eventIndexSplit[1],
                                                        eventIndexSplit[0],
                                                        correlatedFinding.getValue(),
                                                        List.of()
                                                    )
                                                );
                                            }
                                            onOperation(eventWithScores);
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            onFailures(e);
                                        }
                                    });
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    onFailures(e);
                                }
                            });
                        }

                        @Override
                        public void onFailure(Exception e) {
                            onFailures(e);
                        }
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    onFailures(e);
                }
            });
        }

        private void onOperation(List<EventWithScore> events) {
            finishHim(events, null);
        }

        private void onFailures(Exception t) {
            finishHim(null, t);
        }

        private void finishHim(List<EventWithScore> events, Exception t) {
            if (t != null) {
                listener.onFailure(t);
            } else {
                listener.onResponse(new SearchCorrelatedEventsResponse(events, RestStatus.OK));
            }
        }
    }
}

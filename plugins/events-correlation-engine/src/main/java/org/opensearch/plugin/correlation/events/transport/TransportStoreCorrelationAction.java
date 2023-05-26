/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.correlation.core.index.query.CorrelationQueryBuilder;
import org.opensearch.plugin.correlation.events.action.StoreCorrelationAction;
import org.opensearch.plugin.correlation.events.action.StoreCorrelationRequest;
import org.opensearch.plugin.correlation.events.action.StoreCorrelationResponse;
import org.opensearch.plugin.correlation.events.model.Correlation;
import org.opensearch.plugin.correlation.settings.EventsCorrelationSettings;
import org.opensearch.plugin.correlation.utils.CorrelationIndices;
import org.opensearch.plugin.correlation.utils.IndexUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Transport Action for converting events to vectors and then storing them on disk.
 *
 * @opensearch.internal
 */
public class TransportStoreCorrelationAction extends HandledTransportAction<StoreCorrelationRequest, StoreCorrelationResponse> {

    private static final Logger log = LogManager.getLogger(TransportStoreCorrelationAction.class);

    private final Client client;

    private final Settings settings;

    private final CorrelationIndices correlationIndices;

    private final ClusterService clusterService;

    private final long setupTimestamp;

    private volatile long correlationTimeWindow;

    /**
     * Parameterized ctor for Transport Action
     * @param transportService TransportService
     * @param client OS client
     * @param settings Settings
     * @param actionFilters ActionFilters
     * @param correlationIndices CorrelationIndices which manages lifecycle of correlation history index.
     * @param clusterService ClusterService
     */
    @Inject
    public TransportStoreCorrelationAction(
        TransportService transportService,
        Client client,
        Settings settings,
        ActionFilters actionFilters,
        CorrelationIndices correlationIndices,
        ClusterService clusterService
    ) {
        super(StoreCorrelationAction.NAME, transportService, actionFilters, StoreCorrelationRequest::new);
        this.client = client;
        this.settings = settings;
        this.correlationIndices = correlationIndices;
        this.clusterService = clusterService;
        this.setupTimestamp = System.currentTimeMillis();
        this.correlationTimeWindow = EventsCorrelationSettings.CORRELATION_TIME_WINDOW.get(this.settings).getMillis();

        this.clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(EventsCorrelationSettings.CORRELATION_TIME_WINDOW, it -> correlationTimeWindow = it.getMillis());
    }

    @Override
    protected void doExecute(Task task, StoreCorrelationRequest request, ActionListener<StoreCorrelationResponse> listener) {
        AsyncStoreCorrelationAction asyncAction = new AsyncStoreCorrelationAction(request, listener);

        if (!this.correlationIndices.correlationIndexExists()) {
            try {
                this.correlationIndices.initCorrelationIndex(new ActionListener<>() {
                    @Override
                    public void onResponse(CreateIndexResponse response) {
                        if (response.isAcknowledged()) {
                            IndexUtils.correlationHistoryIndexUpdated();
                            try {
                                correlationIndices.setupCorrelationIndex(setupTimestamp, new ActionListener<>() {
                                    @Override
                                    public void onResponse(BulkResponse response) {
                                        if (!response.hasFailures()) {
                                            asyncAction.start();
                                        } else {
                                            asyncAction.onFailures(
                                                new OpenSearchStatusException(response.toString(), RestStatus.INTERNAL_SERVER_ERROR)
                                            );
                                        }
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        asyncAction.onFailures(e);
                                    }
                                });
                            } catch (IOException e) {
                                asyncAction.onFailures(e);
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        asyncAction.onFailures(e);
                    }
                });
            } catch (IOException e) {
                asyncAction.onFailures(e);
            }
        } else {
            asyncAction.start();
        }
    }

    class AsyncStoreCorrelationAction {
        private final StoreCorrelationRequest request;

        private final ActionListener<StoreCorrelationResponse> listener;

        AsyncStoreCorrelationAction(StoreCorrelationRequest request, ActionListener<StoreCorrelationResponse> listener) {
            this.request = request;
            this.listener = listener;
        }

        void start() {
            prepareCorrelationHistoryIndex();
        }

        private void prepareCorrelationHistoryIndex() {
            try {
                if (!IndexUtils.correlationHistoryIndexUpdated) {
                    IndexUtils.updateIndexMapping(
                        Correlation.CORRELATION_HISTORY_INDEX,
                        CorrelationIndices.correlationMappings(),
                        clusterService.state(),
                        client.admin().indices(),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(AcknowledgedResponse response) {
                                if (response.isAcknowledged()) {
                                    IndexUtils.correlationHistoryIndexUpdated();
                                    generateTimestampFeature();
                                } else {
                                    onFailures(
                                        new OpenSearchStatusException(
                                            "Failed to create correlation Index",
                                            RestStatus.INTERNAL_SERVER_ERROR
                                        )
                                    );
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                onFailures(e);
                            }
                        }
                    );
                } else {
                    generateTimestampFeature();
                }
            } catch (IOException ex) {
                onFailures(ex);
            }
        }

        private void generateTimestampFeature() {
            Long eventTimestamp = request.getTimestamp();

            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("score_timestamp", 0L));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(queryBuilder);
            searchSourceBuilder.fetchSource(true);
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
                        onFailures(new OpenSearchStatusException("Score Root Record not found", RestStatus.INTERNAL_SERVER_ERROR));
                    }

                    SearchHit hit = response.getHits().getHits()[0];
                    String id = hit.getId();
                    Map<String, Object> source = hit.getSourceAsMap();

                    long scoreTimestamp = 0L;
                    if (source != null) {
                        if (source.get("score_timestamp") instanceof Integer) {
                            scoreTimestamp = ((Integer) source.get("score_timestamp")).longValue();
                        } else {
                            scoreTimestamp = (long) source.get("score_timestamp");
                        }
                    }
                    if (eventTimestamp - CorrelationIndices.FIXED_HISTORICAL_INTERVAL > scoreTimestamp) {
                        try {
                            Correlation scoreRootRecord = new Correlation(
                                id,
                                1L,
                                false,
                                0L,
                                "",
                                "",
                                new float[] {},
                                0L,
                                "",
                                "",
                                List.of(),
                                eventTimestamp - CorrelationIndices.FIXED_HISTORICAL_INTERVAL
                            );

                            IndexRequest scoreIndexRequest = new IndexRequest(Correlation.CORRELATION_HISTORY_INDEX).id(id)
                                .source(scoreRootRecord.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                                .timeout(TimeValue.timeValueSeconds(60))
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

                            client.index(scoreIndexRequest, new ActionListener<>() {
                                @Override
                                public void onResponse(IndexResponse response) {
                                    if (response.status().equals(RestStatus.OK)) {
                                        if (request.getEventsAdjacencyList() == null || request.getEventsAdjacencyList().isEmpty()) {
                                            insertOrphanEvents(
                                                Long.valueOf(CorrelationIndices.FIXED_HISTORICAL_INTERVAL / 1000L).floatValue()
                                            );
                                        } else {
                                            insertCorrelatedEvents(
                                                Long.valueOf(CorrelationIndices.FIXED_HISTORICAL_INTERVAL / 1000L).floatValue()
                                            );
                                        }
                                    } else {
                                        onFailures(
                                            new OpenSearchStatusException(
                                                "Failed to update Score Root record",
                                                RestStatus.INTERNAL_SERVER_ERROR
                                            )
                                        );
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    onFailures(e);
                                }
                            });
                        } catch (IOException ex) {
                            onFailures(ex);
                        }
                    } else {
                        float timestampFeature = Long.valueOf((eventTimestamp - scoreTimestamp) / 1000L).floatValue();
                        if (request.getEventsAdjacencyList() == null || request.getEventsAdjacencyList().isEmpty()) {
                            insertOrphanEvents(timestampFeature);
                        } else {
                            insertCorrelatedEvents(timestampFeature);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    onFailures(e);
                }
            });
        }

        private void insertCorrelatedEvents(float timestampFeature) {
            long eventTimestamp = request.getTimestamp();
            Map<String, List<String>> neighborEvents = request.getEventsAdjacencyList();

            MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("root", true);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(queryBuilder);
            searchSourceBuilder.fetchSource(true);
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
                        onFailures(new OpenSearchStatusException("Root Record not found", RestStatus.INTERNAL_SERVER_ERROR));
                    }

                    SearchHit hit = response.getHits().getHits()[0];
                    Map<String, Object> source = hit.getSourceAsMap();

                    assert source != null;
                    long level = Long.parseLong(source.get("level").toString());

                    MultiSearchRequest mSearchRequest = new MultiSearchRequest();
                    for (Map.Entry<String, List<String>> neighborEvent : neighborEvents.entrySet()) {
                        for (String event : neighborEvent.getValue()) {
                            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                                .must(QueryBuilders.matchQuery("event1", event))
                                .must(QueryBuilders.matchQuery("event2", ""));

                            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                            searchSourceBuilder.query(queryBuilder);
                            searchSourceBuilder.fetchSource(true);
                            searchSourceBuilder.size(1);

                            SearchRequest searchRequest = new SearchRequest();
                            searchRequest.indices(Correlation.CORRELATION_HISTORY_INDEX);
                            searchRequest.source(searchSourceBuilder);
                            mSearchRequest.add(searchRequest);
                        }
                    }

                    client.multiSearch(mSearchRequest, new ActionListener<>() {
                        @Override
                        public void onResponse(MultiSearchResponse items) {
                            MultiSearchResponse.Item[] responses = items.getResponses();
                            BulkRequest bulkRequest = new BulkRequest();
                            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

                            long prevLevel = -1L;
                            long totalNeighbors = 0L;
                            for (MultiSearchResponse.Item response : responses) {
                                if (response.isFailure()) {
                                    log.info(response.getFailureMessage());
                                    continue;
                                }

                                if (response.getResponse().getHits().getTotalHits().value == 1) {
                                    ++totalNeighbors;

                                    SearchHit hit = response.getResponse().getHits().getHits()[0];
                                    Map<String, Object> source = hit.getSourceAsMap();

                                    assert source != null;
                                    long neighborLevel = Long.parseLong(source.get("level").toString());
                                    String correlatedEvent = source.get("event1").toString();
                                    String correlatedIndex = source.get("index1").toString();

                                    try {
                                        float[] corrVector = new float[3];
                                        if (level != prevLevel) {
                                            for (int i = 0; i < 2; ++i) {
                                                corrVector[i] = ((float) level) - 50.0f;
                                            }
                                            corrVector[0] = (float) level;
                                            corrVector[2] = timestampFeature;

                                            Correlation event = new Correlation(
                                                false,
                                                level,
                                                request.getEvent(),
                                                "",
                                                corrVector,
                                                eventTimestamp,
                                                request.getIndex(),
                                                "",
                                                request.getTags(),
                                                0L
                                            );

                                            IndexRequest indexRequest = new IndexRequest(Correlation.CORRELATION_HISTORY_INDEX).source(
                                                event.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
                                            ).timeout(TimeValue.timeValueSeconds(60));
                                            bulkRequest.add(indexRequest);
                                        }

                                        corrVector = new float[3];
                                        for (int i = 0; i < 2; ++i) {
                                            corrVector[i] = ((float) level) - 50.0f;
                                        }
                                        corrVector[0] = (2.0f * ((float) level) - 50.0f) / 2.0f;
                                        corrVector[1] = (2.0f * ((float) neighborLevel) - 50.0f) / 2.0f;
                                        corrVector[2] = timestampFeature;

                                        Correlation event = new Correlation(
                                            false,
                                            (long) ((2.0f * ((float) level) - 50.0f) / 2.0f),
                                            request.getEvent(),
                                            correlatedEvent,
                                            corrVector,
                                            eventTimestamp,
                                            request.getIndex(),
                                            correlatedIndex,
                                            request.getTags(),
                                            0L
                                        );

                                        IndexRequest indexRequest = new IndexRequest(Correlation.CORRELATION_HISTORY_INDEX).source(
                                            event.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
                                        ).timeout(TimeValue.timeValueSeconds(60));
                                        bulkRequest.add(indexRequest);
                                    } catch (IOException ex) {
                                        onFailures(ex);
                                    }
                                    prevLevel = level;
                                }
                            }

                            if (totalNeighbors > 0L) {
                                client.bulk(bulkRequest, new ActionListener<>() {
                                    @Override
                                    public void onResponse(BulkResponse response) {
                                        if (response.hasFailures()) {
                                            onFailures(
                                                new OpenSearchStatusException(
                                                    "Correlation of finding failed",
                                                    RestStatus.INTERNAL_SERVER_ERROR
                                                )
                                            );
                                        }
                                        onOperation();
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        onFailures(e);
                                    }
                                });
                            } else {
                                insertOrphanEvents(timestampFeature);
                            }
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

        private void insertOrphanEvents(float timestampFeature) {
            long eventTimestamp = request.getTimestamp();

            MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("root", true);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(queryBuilder);
            searchSourceBuilder.fetchSource(true);
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
                        onFailures(new OpenSearchStatusException("Root Record not found", RestStatus.INTERNAL_SERVER_ERROR));
                    }

                    SearchHit hit = response.getHits().getHits()[0];
                    Map<String, Object> source = hit.getSourceAsMap();
                    String id = hit.getId();

                    assert source != null;
                    long level = Long.parseLong(source.get("level").toString());
                    long timestamp = Long.parseLong(source.get("timestamp").toString());

                    try {
                        if (level == 0L) {
                            updateRootRecord(id, 50L, new ActionListener<>() {
                                @Override
                                public void onResponse(IndexResponse response) {
                                    if (response.status().equals(RestStatus.OK)) {
                                        try {
                                            float[] corrVector = new float[3];
                                            corrVector[0] = 50.0f;
                                            corrVector[2] = timestampFeature;

                                            storeEvents(50L, corrVector);
                                        } catch (IOException ex) {
                                            onFailures(ex);
                                        }
                                    } else {
                                        onFailures(
                                            new OpenSearchStatusException("Root Record not updated", RestStatus.INTERNAL_SERVER_ERROR)
                                        );
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    onFailures(e);
                                }
                            });
                        } else {
                            if (eventTimestamp - timestamp > correlationTimeWindow) {
                                updateRootRecord(id, 50L, new ActionListener<>() {
                                    @Override
                                    public void onResponse(IndexResponse response) {
                                        if (response.status().equals(RestStatus.OK)) {
                                            try {
                                                float[] corrVector = new float[3];
                                                corrVector[0] = 50.0f;
                                                corrVector[2] = timestampFeature;

                                                storeEvents(50L, corrVector);
                                            } catch (IOException ex) {
                                                onFailures(ex);
                                            }
                                        } else {
                                            onFailures(
                                                new OpenSearchStatusException("Root Record not updated", RestStatus.INTERNAL_SERVER_ERROR)
                                            );
                                        }
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        onFailures(e);
                                    }
                                });
                            } else {
                                float[] query = new float[3];
                                for (int i = 0; i < 2; ++i) {
                                    query[i] = (2.0f * ((float) level) - 50.0f) / 2.0f;
                                }
                                query[2] = timestampFeature;

                                CorrelationQueryBuilder correlationQueryBuilder = new CorrelationQueryBuilder(
                                    "corr_vector",
                                    query,
                                    3,
                                    QueryBuilders.boolQuery()
                                        .mustNot(QueryBuilders.matchQuery("event1", ""))
                                        .mustNot(QueryBuilders.matchQuery("event2", ""))
                                        .filter(
                                            QueryBuilders.rangeQuery("timestamp")
                                                .gte(eventTimestamp - correlationTimeWindow)
                                                .lte(eventTimestamp + correlationTimeWindow)
                                        )
                                );
                                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                                searchSourceBuilder.query(correlationQueryBuilder);
                                searchSourceBuilder.fetchSource(true);
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

                                        long totalHits = response.getHits().getTotalHits().value;
                                        SearchHit hit = totalHits > 0 ? response.getHits().getHits()[0] : null;
                                        long existLevel = 0L;

                                        if (hit != null) {
                                            Map<String, Object> source = hit.getSourceAsMap();
                                            assert source != null;
                                            existLevel = Long.parseLong(source.get("level").toString());
                                        }

                                        try {
                                            if (totalHits == 0L || existLevel != ((long) (2.0f * ((float) level) - 50.0f) / 2.0f)) {
                                                float[] corrVector = new float[3];
                                                for (int i = 0; i < 2; ++i) {
                                                    corrVector[i] = ((float) level) - 50.0f;
                                                }
                                                corrVector[0] = (float) level;
                                                corrVector[2] = timestampFeature;

                                                storeEvents(level, corrVector);
                                            } else {
                                                updateRootRecord(id, level + 50L, new ActionListener<>() {
                                                    @Override
                                                    public void onResponse(IndexResponse response) {
                                                        if (response.status().equals(RestStatus.OK)) {
                                                            try {
                                                                float[] corrVector = new float[3];
                                                                for (int i = 0; i < 2; ++i) {
                                                                    corrVector[i] = (float) level;
                                                                }
                                                                corrVector[0] = level + 50.0f;
                                                                corrVector[2] = timestampFeature;

                                                                storeEvents(level + 50L, corrVector);
                                                            } catch (IOException ex) {
                                                                onFailures(ex);
                                                            }
                                                        }
                                                    }

                                                    @Override
                                                    public void onFailure(Exception e) {

                                                    }
                                                });
                                            }
                                        } catch (IOException ex) {
                                            onFailures(ex);
                                        }
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        onFailures(e);
                                    }
                                });
                            }
                        }
                    } catch (IOException ex) {
                        onFailures(ex);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    onFailures(e);
                }
            });
        }

        private void updateRootRecord(String id, Long level, ActionListener<IndexResponse> listener) throws IOException {
            Correlation rootRecord = new Correlation(
                id,
                1L,
                true,
                level,
                "",
                "",
                new float[] { 0.0f, 0.0f, 0.0f },
                request.getTimestamp(),
                "",
                "",
                List.of(),
                0L
            );

            IndexRequest indexRequest = new IndexRequest(Correlation.CORRELATION_HISTORY_INDEX).id(id)
                .source(rootRecord.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .timeout(TimeValue.timeValueSeconds(60))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            client.index(indexRequest, listener);
        }

        private void storeEvents(Long level, float[] correlationVector) throws IOException {
            Correlation event = new Correlation(
                false,
                level,
                request.getEvent(),
                "",
                correlationVector,
                request.getTimestamp(),
                request.getIndex(),
                "",
                request.getTags(),
                0L
            );

            IndexRequest indexRequest = new IndexRequest(Correlation.CORRELATION_HISTORY_INDEX).source(
                event.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            ).timeout(TimeValue.timeValueSeconds(60)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            client.index(indexRequest, new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse response) {
                    if (response.status().equals(RestStatus.CREATED)) {
                        onOperation();
                    } else {
                        onFailures(new OpenSearchStatusException(response.toString(), RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    onFailures(e);
                }
            });
        }

        private void onOperation() {
            finishHim(null);
        }

        private void onFailures(Exception t) {
            finishHim(t);
        }

        private void finishHim(Exception t) {
            if (t != null) {
                listener.onFailure(t);
            } else {
                listener.onResponse(new StoreCorrelationResponse(RestStatus.OK));
            }
        }
    }
}

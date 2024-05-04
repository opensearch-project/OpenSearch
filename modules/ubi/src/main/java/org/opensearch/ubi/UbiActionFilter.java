/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ubi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.Streams;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.tasks.Task;
import org.opensearch.ubi.ext.UbiParameters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An implementation of {@link ActionFilter} that listens for OpenSearch
 * queries and persists the queries to the UBI store.
 */
public class UbiActionFilter implements ActionFilter {

    private static final Logger LOGGER = LogManager.getLogger(UbiActionFilter.class);

    private static final String UBI_QUERIES_INDEX = "ubi_queries";
    private static final String UBI_EVENTS_INDEX = "ubi_events";

    private static final String EVENTS_MAPPING_FILE = "/events-mapping.json";
    private static final String QUERIES_MAPPING_FILE = "/queries-mapping.json";

    private final Client client;

    /**
     * Creates a new filter.
     * @param client An OpenSearch {@link Client}.
     */
    public UbiActionFilter(Client client) {
        this.client = client;
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {

        if (!(request instanceof SearchRequest)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        chain.proceed(task, action, request, new ActionListener<>() {

            @Override
            public void onResponse(Response response) {

                final SearchRequest searchRequest = (SearchRequest) request;

                if (response instanceof SearchResponse) {

                    final UbiParameters ubiParameters = UbiParameters.getUbiParameters(searchRequest);

                    if (ubiParameters != null) {

                        final String queryId = ubiParameters.getQueryId();
                        final String userQuery = ubiParameters.getUserQuery();
                        final String userId = ubiParameters.getClientId();
                        final String objectId = ubiParameters.getObjectId();

                        final List<String> queryResponseHitIds = new LinkedList<>();

                        for (final SearchHit hit : ((SearchResponse) response).getHits()) {

                            if (objectId == null || objectId.isEmpty()) {
                                // Use the result's docId since no object_id was given for the search.
                                queryResponseHitIds.add(String.valueOf(hit.docId()));
                            } else {
                                final Map<String, Object> source = hit.getSourceAsMap();
                                queryResponseHitIds.add((String) source.get(objectId));
                            }

                        }

                        final String queryResponseId = UUID.randomUUID().toString();
                        final QueryResponse queryResponse = new QueryResponse(queryId, queryResponseId, queryResponseHitIds);
                        final QueryRequest queryRequest = new QueryRequest(queryId, userQuery, userId, queryResponse);

                        indexUbiQuery(queryRequest);

                    }

                }

                listener.onResponse(response);

            }

            @Override
            public void onFailure(Exception ex) {
                listener.onFailure(ex);
            }

        });

    }

    private void indexUbiQuery(final QueryRequest queryRequest) {

        final IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(UBI_EVENTS_INDEX, UBI_QUERIES_INDEX);

        client.admin().indices().exists(indicesExistsRequest, new ActionListener<>() {

            @Override
            public void onResponse(IndicesExistsResponse indicesExistsResponse) {

                final Settings indexSettings = Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-2")
                    .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
                    .build();

                // Create the UBI events index.
                final CreateIndexRequest createEventsIndexRequest = new CreateIndexRequest(UBI_EVENTS_INDEX).mapping(
                    getResourceFile(EVENTS_MAPPING_FILE)
                ).settings(indexSettings);

                client.admin().indices().create(createEventsIndexRequest);

                // Create the UBI queries index.
                final CreateIndexRequest createQueriesIndexRequest = new CreateIndexRequest(UBI_QUERIES_INDEX).mapping(
                    getResourceFile(QUERIES_MAPPING_FILE)
                ).settings(indexSettings);

                client.admin().indices().create(createQueriesIndexRequest);

            }

            @Override
            public void onFailure(Exception ex) {
                LOGGER.error("Error creating UBI indexes.", ex);
            }

        });

        LOGGER.debug(
            "Indexing query ID {} with response ID {}",
            queryRequest.getQueryId(),
            queryRequest.getQueryResponse().getQueryResponseId()
        );

        // What will be indexed - adheres to the queries-mapping.json
        final Map<String, Object> source = new HashMap<>();
        source.put("timestamp", queryRequest.getTimestamp());
        source.put("query_id", queryRequest.getQueryId());
        source.put("query_response_id", queryRequest.getQueryResponse().getQueryResponseId());
        source.put("query_response_object_ids", queryRequest.getQueryResponse().getQueryResponseObjectIds());
        source.put("user_id", queryRequest.getUserId());
        source.put("user_query", queryRequest.getUserQuery());

        // Build the index request.
        final IndexRequest indexRequest = new IndexRequest(UBI_QUERIES_INDEX).source(source, XContentType.JSON);

        client.index(indexRequest, new ActionListener<>() {

            @Override
            public void onResponse(IndexResponse indexResponse) {}

            @Override
            public void onFailure(Exception e) {
                LOGGER.error("Unable to index query into UBI index.", e);
            }

        });

    }

    private String getResourceFile(final String fileName) {
        try (InputStream is = UbiActionFilter.class.getResourceAsStream(fileName)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get mapping from resource [" + fileName + "]", e);
        }
    }

}

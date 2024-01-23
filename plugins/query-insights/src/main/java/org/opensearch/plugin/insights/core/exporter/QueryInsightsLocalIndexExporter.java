/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Class to export data collected by search query analyzers to a local OpenSearch index
 * <p>
 * Internal used within the Query Insight framework
 *
 * @opensearch.internal
 */
public class QueryInsightsLocalIndexExporter<T extends SearchQueryRecord<?>> extends QueryInsightsExporter<T> {
    private static final Logger log = LogManager.getLogger(QueryInsightsLocalIndexExporter.class);
    private static final int INDEX_TIMEOUT = 60;

    private final ClusterService clusterService;
    private final Client client;

    /** The mapping for the local index that holds the data */
    private final InputStream localIndexMapping;

    /**
     * Create a QueryInsightsLocalIndexExporter Object
     * @param clusterService The clusterService of the node
     * @param client The OpenSearch Client to support index operations
     * @param localIndexName The local index name to export the data to
     * @param localIndexMapping The mapping for the local index
     */
    public QueryInsightsLocalIndexExporter(
        ClusterService clusterService,
        Client client,
        String localIndexName,
        InputStream localIndexMapping
    ) {
        super(localIndexName);
        this.clusterService = clusterService;
        this.client = client;
        this.localIndexMapping = localIndexMapping;
    }

    /**
     * Export the data to the predefined local OpenSearch Index
     *
     * @param records the data to export
     * @throws IOException if an error occurs
     */
    @Override
    public void export(List<T> records) throws IOException {
        if (records.size() == 0) {
            return;
        }
        boolean indexExists = checkAndInitLocalIndex(new ActionListener<>() {
            @Override
            public void onResponse(CreateIndexResponse response) {
                if (response.isAcknowledged()) {
                    log.debug(String.format(Locale.ROOT, "successfully initialized local index %s for query insight.", getIdentifier()));
                    try {
                        bulkRecord(records);
                    } catch (IOException e) {
                        log.error(String.format(Locale.ROOT, "fail to ingest query insight data to local index, error: %s", e));
                    }
                } else {
                    log.error(
                        String.format(Locale.ROOT, "request to created local index %s for query insight not acknowledged.", getIdentifier())
                    );
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error(String.format(Locale.ROOT, "error creating local index for query insight: %s", e));
            }
        });

        if (indexExists) {
            bulkRecord(records);
        }
    }

    /**
     * Util function to check if a local OpenSearch Index exists
     *
     * @return boolean
     */
    private boolean checkIfIndexExists() {
        ClusterState clusterState = clusterService.state();
        return clusterState.getRoutingTable().hasIndex(this.getIdentifier());
    }

    /**
     * Check and initialize the local OpenSearch Index for the exporter
     *
     * @param listener the listener to be notified upon completion
     * @return boolean to represent if the index has already been created before calling this function
     * @throws IOException if an error occurs
     */
    private synchronized boolean checkAndInitLocalIndex(ActionListener<CreateIndexResponse> listener) throws IOException {
        if (!checkIfIndexExists()) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(this.getIdentifier()).mapping(getIndexMappings())
                .settings(Settings.builder().put("index.hidden", false).build());
            client.admin().indices().create(createIndexRequest, listener);
            return false;
        } else {
            return true;
        }
    }

    /**
     * Get the index mapping of the local index
     *
     * @return String to represent the index mapping
     * @throws IOException if an error occurs
     */
    private String getIndexMappings() throws IOException {
        return new String(Objects.requireNonNull(this.localIndexMapping).readAllBytes(), Charset.defaultCharset());
    }

    /**
     * Bulk ingest the data into to the predefined local OpenSearch Index
     *
     * @param records the data to export
     * @throws IOException if an error occurs
     */
    private void bulkRecord(List<T> records) throws IOException {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .timeout(TimeValue.timeValueSeconds(INDEX_TIMEOUT));
        for (T record : records) {
            bulkRequest.add(
                new IndexRequest(getIdentifier()).source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }
        client.bulk(bulkRequest, new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse response) {
                if (response.status().equals(RestStatus.CREATED) || response.status().equals(RestStatus.OK)) {
                    log.debug(String.format(Locale.ROOT, "successfully ingest data for %s! ", getIdentifier()));
                } else {
                    log.error(
                        String.format(
                            Locale.ROOT,
                            "error when ingesting data for %s, error: %s",
                            getIdentifier(),
                            response.buildFailureMessage()
                        )
                    );
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error(String.format(Locale.ROOT, "failed to ingest data for %s, %s", getIdentifier(), e));
            }
        });
    }
}

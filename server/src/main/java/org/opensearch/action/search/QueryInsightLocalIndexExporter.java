/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

/**
 * Class to export data collected by search query analyzers to a local OpenSearch index
 * <p>
 * Mainly for use within the Query Insight framework
 *
 * @opensearch.internal
 */
public class QueryInsightLocalIndexExporter<T extends SearchQueryRecord<?>> extends QueryInsightExporter<T>{

    private static final Logger log = LogManager.getLogger(QueryInsightLocalIndexExporter.class);

    private final ClusterService clusterService;
    private final Client client;

    /** The OpenSearch index name to export the data to */
    private final String localIndexName;

    /** The mapping for the local index that holds the data */
    private final InputStream localIndexMapping;

    public QueryInsightLocalIndexExporter(
        boolean enabled,
        ClusterService clusterService,
        Client client,
        String localIndexName,
        InputStream localIndexMapping) {
        this.setEnabled(enabled);
        this.clusterService = clusterService;
        this.client = client;
        this.localIndexName = localIndexName;
        this.localIndexMapping = localIndexMapping;
        try {
            setup();
        } catch (IOException e) {
            log.error(String.format("failed to set up with error %s.", e));
        }
    }

    @Override
    public void setup() throws IOException {}


    /**
     * Export the data to the predefined local OpenSearch Index
     *
     * @param records the data to export
     * @throws IOException if an error occurs
     */
    @Override
    public synchronized void export(List<T> records) throws IOException {
        if (records.size() == 0) {
            return;
        }
        if (checkIfIndexExists()) {
            bulkRecord(records);
        } else {
            // local index not exist
            initLocalIndex(new ActionListener<>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    if (response.isAcknowledged()) {
                        log.debug(String.format("successfully initialized local index %s for query insight.", localIndexName));
                        try {
                            bulkRecord(records);
                        } catch (IOException e) {
                            log.error(String.format("fail to ingest query insight data to local index, error: %s", e));
                        }
                    }
                    else {
                        log.error(String.format("request to created local index %s for query insight not acknowledged.", localIndexName));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(String.format("error creating local index for query insight: %s", e));
                }
            });
        }
    }

    /**
     * Util function to check if a local OpenSearch Index exists
     *
     * @return boolean
     */
    private boolean checkIfIndexExists() {
        ClusterState clusterState = clusterService.state();
        return clusterState.getRoutingTable().hasIndex(this.localIndexName);
    }

    /**
     * Initialize the local OpenSearch Index for the exporter
     *
     * @param listener the listener to be notified upon completion
     * @throws IOException if an error occurs
     */
    private synchronized void initLocalIndex(ActionListener<CreateIndexResponse> listener) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(this.localIndexName).mapping(
            getIndexMappings()
        ).settings(Settings.builder().put("index.hidden", false).build());
        client.admin().indices().create(createIndexRequest, listener);
    }

    /**
     * Drop the local OpenSearch Index created by the exporter
     *
     * @param listener the listener to be notified upon completion
     * @throws IOException if an error occurs
     */
    private synchronized void dropLocalIndex(ActionListener<AcknowledgedResponse> listener) throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(this.localIndexName);
        client.admin().indices().delete(deleteIndexRequest, listener);
    }

    /**
     * Get the index mapping of the local index
     *
     * @return String to represent the index mapping
     * @throws IOException if an error occurs
     */
    private String getIndexMappings() throws IOException {
        return new String(
            Objects.requireNonNull(this.localIndexMapping)
                .readAllBytes(),
            Charset.defaultCharset()
        );
    }

    /**
     * Bulk ingest the data into to the predefined local OpenSearch Index
     *
     * @param records the data to export
     * @throws IOException if an error occurs
     */
    private synchronized void bulkRecord(List<T> records) throws IOException {
        BulkRequest bulkRequest = new BulkRequest()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).timeout(TimeValue.timeValueSeconds(60));
        for (T record : records) {
            bulkRequest.add(
                new IndexRequest(localIndexName)
                    .source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }
        client.bulk(bulkRequest, new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse response) {
                if (response.status().equals(RestStatus.CREATED) || response.status().equals(RestStatus.OK)) {
                    log.debug(String.format("successfully ingest data for %s! ", localIndexName));
                }
                else {
                    log.error(String.format("error when ingesting data for %s", localIndexName));
                }
            }
            @Override
            public void onFailure(Exception e) {
                log.error(String.format("failed to ingest data for %s, %s", localIndexName, e));
            }
        });
    }

    /**
     * Index one document to the predefined local OpenSearch Index
     *
     * @param record the document to export
     * @throws IOException if an error occurs
     */
    private synchronized void indexRecord(T record) throws IOException {
        IndexRequest indexRequest = new IndexRequest(localIndexName)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            .timeout(TimeValue.timeValueSeconds(60));

        client.index(indexRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse response) {
                if (response.status().equals(RestStatus.CREATED) || response.status().equals(RestStatus.OK)) {
                    log.debug(String.format("successfully indexed data for %s ", localIndexName));
                }
                else {
                    log.error(String.format("failed to index data for %s", localIndexName));
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error(String.format("failed to index data for %s, error: %s", localIndexName, e));
            }
        });
    }
}

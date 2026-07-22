/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.catalog.PublishShardAction;
import org.opensearch.action.admin.cluster.catalog.PublishShardRequest;
import org.opensearch.action.admin.cluster.catalog.PublishShardResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.client.Client;

import java.io.IOException;

/**
 * Orchestrates publishing index data to an external catalog. Runs on the cluster manager.
 * <p>
 * Constructed at node startup with a {@link MetadataClient} produced by the installed
 * {@code CatalogPlugin}. If no catalog is configured, {@code metadataClient} is {@code null}
 * and {@link #publishIndex} rejects requests with a clear error.
 * <p>
 * Flow: validate → initialize (capture pre-publish snapshot id) → broadcast publish to
 * data nodes → finalizePublish (threads the snapshot id back for rollback on failure)
 * → respond.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RemoteCatalogService {

    private static final Logger logger = LogManager.getLogger(RemoteCatalogService.class);

    private final ClusterService clusterService;
    private final Client client;
    private final MetadataClient metadataClient;

    /**
     * Creates a new service.
     *
     * @param clusterService  cluster service for reading cluster state
     * @param client          node client used to dispatch the broadcast publish action
     * @param metadataClient  client bound to the node's catalog repository, or {@code null}
     *                        if no catalog is configured on this node
     */
    public RemoteCatalogService(ClusterService clusterService, Client client, MetadataClient metadataClient) {
        this.clusterService = clusterService;
        this.client = client;
        this.metadataClient = metadataClient;
    }

    /**
     * Publishes an index to the catalog. Entry point for ISM and REST callers.
     *
     * @param indexName  name of the index to publish
     * @param listener   callback with the broadcast response
     */
    public void publishIndex(String indexName, ActionListener<PublishShardResponse> listener) {
        try {
            // 1. Check MetadataClient is available.
            if (metadataClient == null) {
                listener.onFailure(new IllegalStateException("catalog is not configured on this node"));
                return;
            }

            // 2. Validate the index exists and read metadata.
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
            if (indexMetadata == null) {
                listener.onFailure(new IllegalArgumentException("index [" + indexName + "] not found in cluster state"));
                return;
            }

            // 3. Initialize catalog state (once, on cluster manager) and capture the
            //    pre-publish snapshot id. Rollback on failure targets this snapshot.
            logger.info("Initializing catalog for index [{}]", indexName);
            final String savedSnapshotId = metadataClient.initialize(indexName, indexMetadata);

            // 4. Dispatch broadcast action to data nodes.
            logger.info("Publishing shards for index [{}]", indexName);
            PublishShardRequest broadcastRequest = new PublishShardRequest(indexName);
            client.execute(PublishShardAction.INSTANCE, broadcastRequest, ActionListener.wrap(
                response -> {
                    // 5. Always finalize — plugin stamps completion or rolls back based on success flag.
                    boolean allSucceeded = response.getFailedShards() == 0;
                    try {
                        logger.info(
                            "Finalizing publish for index [{}] (success={}, {}/{} shards, savedSnapshotId={})",
                            indexName, allSucceeded, response.getSuccessfulShards(), response.getTotalShards(), savedSnapshotId
                        );
                        metadataClient.finalizePublish(indexName, allSucceeded, savedSnapshotId);
                    } catch (IOException e) {
                        logger.error("Failed to finalize publish for index [{}]", indexName, e);
                        listener.onFailure(e);
                        return;
                    }
                    listener.onResponse(response);
                },
                listener::onFailure
            ));

        } catch (Exception e) {
            logger.error("Failed to publish index [{}] to catalog", indexName, e);
            listener.onFailure(e);
        }
    }
}

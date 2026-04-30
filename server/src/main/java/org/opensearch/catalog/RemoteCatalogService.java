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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/**
 * Orchestrates publishing index data to an external catalog. Runs on the cluster manager.
 * <p>
 * Constructed at node startup with a {@link CatalogMetadataClient} produced by the installed
 * {@code CatalogPlugin}. If no catalog is configured, {@code metadataClient} is {@code null}
 * and {@link #publishIndex} rejects requests with a clear error.
 * <p>
 * Flow: validate → startPublishForIndex → broadcast copyShard to data nodes → finalizePublish → respond.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RemoteCatalogService {

    private static final Logger logger = LogManager.getLogger(RemoteCatalogService.class);

    private final ClusterService clusterService;
    private final Client client;
    private final CatalogMetadataClient metadataClient;
    private final TimeValue publishTimeout;

    /**
     * Creates a new service.
     *
     * @param clusterService  cluster service for reading cluster state
     * @param client          node client used to dispatch the broadcast publish action
     * @param metadataClient  client bound to the node's catalog repository, or {@code null}
     *                        if no catalog is configured on this node
     * @param publishTimeout  upper bound on the end-to-end broadcast publish. Propagated to
     *                        {@link PublishShardRequest#timeout(TimeValue)} so the broadcast
     *                        framework enforces it at the per-shard RPC layer. When the
     *                        timeout fires, the orchestrator invokes
     *                        {@code finalizePublish(..., false)} for rollback and reports the
     *                        error to the caller. Must not be {@code null}; see
     *                        {@code Node.CATALOG_PUBLISH_TIMEOUT_SETTING}.
     */
    public RemoteCatalogService(
        ClusterService clusterService,
        Client client,
        CatalogMetadataClient metadataClient,
        TimeValue publishTimeout
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.metadataClient = metadataClient;
        this.publishTimeout = Objects.requireNonNull(publishTimeout, "publishTimeout must not be null");
    }

    /**
     * Publishes an index to the catalog. Entry point for ISM and REST callers.
     *
     * @param indexName  name of the index to publish
     * @param listener   callback with the broadcast response
     */
    public void publishIndex(String indexName, ActionListener<PublishShardResponse> listener) {
        try {
            // 1. Check CatalogMetadataClient is available.
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

            // 3. Initialize catalog state (once, on cluster manager) and mint a publishId
            //    so every per-shard invocation can correlate back to this logical publish.
            String publishId = UUID.randomUUID().toString();
            logger.info("Starting catalog publish [{}] for index [{}]", publishId, indexName);
            metadataClient.startPublishForIndex(indexName, indexMetadata);

            // 4. Dispatch broadcast action to data nodes.
            logger.info("Copying shards for publish [{}] on index [{}]", publishId, indexName);
            PublishShardRequest broadcastRequest = new PublishShardRequest(publishId, indexName);
            broadcastRequest.timeout(publishTimeout);
            client.execute(PublishShardAction.INSTANCE, broadcastRequest, ActionListener.wrap(
                response -> {
                    // 5. Always finalize — plugin stamps completion or rolls back based on success flag.
                    boolean allSucceeded = response.getFailedShards() == 0;
                    try {
                        logger.info(
                            "Finalizing publish [{}] for index [{}] (success={}, {}/{} shards)",
                            publishId, indexName, allSucceeded,
                            response.getSuccessfulShards(), response.getTotalShards()
                        );
                        metadataClient.finalizePublish(indexName, allSucceeded);
                    } catch (IOException e) {
                        logger.error("Failed to finalize publish [{}] for index [{}]", publishId, indexName, e);
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

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
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Orchestrates publishing index data to an external catalog. Runs on the cluster manager.
 * <p>
 * Flow: validate → initialize → broadcast publish to data nodes → finalizePublish → respond.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RemoteCatalogService {

    private static final Logger logger = LogManager.getLogger(RemoteCatalogService.class);

    private final MetadataClient metadataClient;
    private final ClusterService clusterService;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Client client;

    public RemoteCatalogService(
        MetadataClient metadataClient,
        ClusterService clusterService,
        Supplier<RepositoriesService> repositoriesService,
        Client client
    ) {
        this.metadataClient = metadataClient;
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.client = client;
    }

    /**
     * Publishes an index to the catalog. Entry point for ISM and REST callers.
     *
     * @param indexName       name of the index to publish
     * @param catalogRepoName name of the registered catalog repository
     * @param listener        callback with the broadcast response
     */
    public void publishIndex(String indexName, String catalogRepoName, ActionListener<PublishShardResponse> listener) {
        try {
            // 1. Validate the catalog repository exists
            Repository repository = repositoriesService.get().repository(catalogRepoName);
            if (!(repository instanceof CatalogRepository)) {
                listener.onFailure(
                    new IllegalArgumentException("repository [" + catalogRepoName + "] is not a catalog repository")
                );
                return;
            }

            // 2. Validate the index exists and read metadata
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
            if (indexMetadata == null) {
                listener.onFailure(new IllegalArgumentException("index [" + indexName + "] not found in cluster state"));
                return;
            }

            // 3. Check MetadataClient is available
            if (metadataClient == null) {
                listener.onFailure(new IllegalStateException("no CatalogPlugin installed — MetadataClient is not available"));
                return;
            }

            // 4. Initialize catalog state (once, on cluster manager)
            logger.info("Initializing catalog for index [{}] in repository [{}]", indexName, catalogRepoName);
            metadataClient.initialize(indexName, indexMetadata);

            // 5. Dispatch broadcast action to data nodes
            logger.info("Publishing shards for index [{}]", indexName);
            PublishShardRequest broadcastRequest = new PublishShardRequest(indexName, catalogRepoName);
            client.execute(PublishShardAction.INSTANCE, broadcastRequest, ActionListener.wrap(
                response -> {
                    // 6. Always finalize — plugin stamps completion or rolls back based on success flag
                    boolean allSucceeded = response.getFailedShards() == 0;
                    try {
                        logger.info(
                            "Finalizing publish for index [{}] (success={}, {}/{} shards)",
                            indexName, allSucceeded, response.getSuccessfulShards(), response.getTotalShards()
                        );
                        metadataClient.finalizePublish(indexName, allSucceeded);
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
            logger.error("Failed to publish index [{}] to catalog [{}]", indexName, catalogRepoName, e);
            listener.onFailure(e);
        }
    }
}

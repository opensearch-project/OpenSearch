/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.catalog.CatalogPublishesInProgress;
import org.opensearch.catalog.PublishEntry;
import org.opensearch.catalog.PublishPhase;
import org.opensearch.catalog.RemoteCatalogService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

/**
 * Cluster-manager-node transport for submitting an async catalog publish. Appends a new
 * {@link PublishEntry} to {@link CatalogPublishesInProgress} and returns its correlation id.
 *
 * @opensearch.experimental
 */
public class TransportPublishIndexAction extends TransportClusterManagerNodeAction<PublishIndexRequest, PublishIndexResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPublishIndexAction.class);

    private final RemoteCatalogService remoteCatalogService;
    private final int maxConcurrentPublishes;

    @Inject
    public TransportPublishIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteCatalogService remoteCatalogService
    ) {
        super(
            PublishIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PublishIndexRequest::new,
            indexNameExpressionResolver
        );
        this.remoteCatalogService = remoteCatalogService;
        this.maxConcurrentPublishes = remoteCatalogService.maxConcurrentPublishes();
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PublishIndexResponse read(StreamInput in) throws IOException {
        return new PublishIndexResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PublishIndexRequest request, ClusterState state) {
        // Snapshot-style block checks: global READ + index METADATA_READ. Mirrors
        // TransportCreateSnapshotAction — the cluster must be writable, the index itself
        // only needs metadata read access.
        ClusterBlockException global = state.blocks().globalBlockedException(ClusterBlockLevel.READ);
        if (global != null) return global;
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, new String[] { request.indexName() });
    }

    @Override
    protected void clusterManagerOperation(
        PublishIndexRequest request,
        ClusterState state,
        ActionListener<PublishIndexResponse> listener
    ) {
        if (!remoteCatalogService.isEnabled()) {
            listener.onFailure(new IllegalStateException("catalog is not configured on this node"));
            return;
        }

        IndexMetadata indexMetadata = state.metadata().index(request.indexName());
        if (indexMetadata == null) {
            listener.onFailure(new IllegalArgumentException("index [" + request.indexName() + "] not found"));
            return;
        }

        String publishId = UUID.randomUUID().toString();
        String indexName = request.indexName();
        String indexUUID = indexMetadata.getIndexUUID();

        clusterService.submitStateUpdateTask(
            "catalog-publish-submit[" + publishId + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return addEntry(currentState, publishId, indexName, indexUUID, maxConcurrentPublishes);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("[publish-{}] submit failed", publishId, e);
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("[publish-{}] submitted for index [{}]", publishId, indexName);
                    listener.onResponse(new PublishIndexResponse(true, publishId));
                }
            }
        );
    }

    /** Pure helper for unit tests; same body as the submit update task. */
    static ClusterState addEntry(
        ClusterState currentState,
        String publishId,
        String indexName,
        String indexUUID,
        int maxConcurrentPublishes
    ) {
        CatalogPublishesInProgress current = currentState.metadata().custom(CatalogPublishesInProgress.TYPE);
        if (current == null) current = CatalogPublishesInProgress.EMPTY;

        if (current.entryForIndex(indexName) != null) {
            throw new IllegalStateException("publish already in progress for index [" + indexName + "]");
        }
        if (current.entries().size() >= maxConcurrentPublishes) {
            throw new OpenSearchRejectedExecutionException(
                "max concurrent catalog publishes reached ["
                    + maxConcurrentPublishes + "]; retry later"
            );
        }
        IndexMetadata im = currentState.metadata().index(indexName);
        if (im == null) {
            throw new IllegalArgumentException("index [" + indexName + "] not found");
        }

        PublishEntry entry = PublishEntry.builder()
            .publishId(publishId)
            .indexName(indexName)
            .indexUUID(indexUUID)
            .phase(PublishPhase.INITIALIZED)
            .shardStatuses(Collections.emptyMap())
            .startedAt(System.currentTimeMillis())
            .retryCount(0)
            .build();

        CatalogPublishesInProgress next = current.withAddedEntry(entry);
        Metadata.Builder mb = Metadata.builder(currentState.metadata())
            .putCustom(CatalogPublishesInProgress.TYPE, next);
        return ClusterState.builder(currentState).metadata(mb).build();
    }
}

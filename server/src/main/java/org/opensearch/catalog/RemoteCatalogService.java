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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Cluster-manager-side orchestrator. Implements {@link ClusterStateListener}; on every
 * cluster-state update dispatches each {@link PublishEntry} to its phase processor.
 * Handles node-departure and index-deletion coordination on top.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RemoteCatalogService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(RemoteCatalogService.class);

    private final ClusterService clusterService;
    private final CatalogMetadataClient metadataClient;
    private final TimeValue publishTimeout;
    private final int maxConcurrentPublishes;

    private final InitializedProcessor initializedProcessor;
    private final PublishingProcessor publishingProcessor;
    private final FinalizingSuccessProcessor finalizingSuccessProcessor;
    private final FinalizingFailureProcessor finalizingFailureProcessor;

    public RemoteCatalogService(
        ClusterService clusterService,
        Client client,
        CatalogMetadataClient metadataClient,
        ThreadPool threadPool,
        TimeValue publishTimeout,
        int maxRetries,
        TimeValue baseBackoff,
        int maxConcurrentPublishes
    ) {
        this.clusterService = clusterService;
        this.metadataClient = metadataClient;
        this.publishTimeout = publishTimeout;
        this.maxConcurrentPublishes = maxConcurrentPublishes;

        if (metadataClient != null) {
            this.initializedProcessor = new InitializedProcessor(
                clusterService, metadataClient, threadPool, maxRetries, baseBackoff
            );
            this.publishingProcessor = new PublishingProcessor(
                clusterService, metadataClient, threadPool, client, maxRetries, baseBackoff, publishTimeout
            );
            this.finalizingSuccessProcessor = new FinalizingSuccessProcessor(
                clusterService, metadataClient, threadPool, maxRetries, baseBackoff
            );
            this.finalizingFailureProcessor = new FinalizingFailureProcessor(
                clusterService, metadataClient, threadPool, maxRetries, baseBackoff
            );
        } else {
            this.initializedProcessor = null;
            this.publishingProcessor = null;
            this.finalizingSuccessProcessor = null;
            this.finalizingFailureProcessor = null;
        }
    }

    public boolean isEnabled() {
        return metadataClient != null;
    }

    public TimeValue publishTimeout() {
        return publishTimeout;
    }

    public int maxConcurrentPublishes() {
        return maxConcurrentPublishes;
    }

    public CatalogMetadataClient metadataClient() {
        return metadataClient;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        boolean wasClusterManager = event.previousState().nodes().isLocalNodeElectedClusterManager();
        boolean isClusterManager = event.localNodeClusterManager();
        if (!isClusterManager) {
            if (wasClusterManager && publishingProcessor != null) {
                publishingProcessor.forgetAllDispatches();
            }
            return;
        }
        if (!isEnabled()) {
            return;
        }

        CatalogPublishesInProgress custom = AbstractPublishStateProcessor.currentCustom(event.state());
        if (custom.isEmpty()) {
            return;
        }

        if (!wasClusterManager) {
            logger.info("adopted {} in-flight catalog publishes after becoming cluster manager", custom.entries().size());
        }

        handleNodeDepartures(event, custom);
        handleIndexDeletions(event.state(), custom);

        for (PublishEntry entry : custom.entries()) {
            dispatch(entry);
        }
    }

    private void dispatch(PublishEntry entry) {
        switch (entry.phase()) {
            case INITIALIZED:
                initializedProcessor.process(entry);
                break;
            case PUBLISHING:
                publishingProcessor.process(entry);
                break;
            case FINALIZING_SUCCESS:
                finalizingSuccessProcessor.process(entry);
                break;
            case FINALIZING_FAILURE:
                finalizingFailureProcessor.process(entry);
                break;
            case FAILED:
                // terminal; operator cleanup pending
                break;
            default:
                logger.warn("[publish-{}] unknown phase [{}]", entry.publishId(), entry.phase());
        }
    }

    private void handleNodeDepartures(ClusterChangedEvent event, CatalogPublishesInProgress custom) {
        if (!event.nodesRemoved()) return;

        Set<String> removedNodeIds = new HashSet<>();
        event.nodesDelta().removedNodes().forEach(n -> removedNodeIds.add(n.getId()));
        if (removedNodeIds.isEmpty()) return;

        for (PublishEntry entry : custom.entries()) {
            if (entry.phase() != PublishPhase.PUBLISHING) continue;
            if (entry.shardStatuses().isEmpty()) continue;

            boolean anyAffected = entry.shardStatuses().values().stream().anyMatch(s ->
                s.state() == PerShardStatus.State.PENDING && s.nodeId() != null && removedNodeIds.contains(s.nodeId())
            );
            if (!anyAffected) continue;

            submitShardFailures(
                entry.publishId(),
                "catalog-publish-node-departed[" + entry.publishId() + "]",
                (shardId, status) -> status.state() == PerShardStatus.State.PENDING
                    && status.nodeId() != null
                    && removedNodeIds.contains(status.nodeId()),
                status -> "node left cluster [" + status.nodeId() + "]"
            );
        }
    }

    private void handleIndexDeletions(ClusterState state, CatalogPublishesInProgress custom) {
        for (PublishEntry entry : custom.entries()) {
            if (entry.phase() != PublishPhase.PUBLISHING) continue;
            if (state.metadata().hasIndex(entry.indexName())) continue;
            if (entry.shardStatuses().isEmpty()) continue;

            boolean anyPending = entry.shardStatuses().values().stream()
                .anyMatch(s -> s.state() == PerShardStatus.State.PENDING);
            if (!anyPending) continue;

            submitShardFailures(
                entry.publishId(),
                "catalog-publish-index-deleted[" + entry.publishId() + "]",
                (shardId, status) -> status.state() == PerShardStatus.State.PENDING,
                status -> "index deleted during publish"
            );
        }
    }

    private void submitShardFailures(
        String publishId,
        String source,
        java.util.function.BiPredicate<ShardId, PerShardStatus> predicate,
        java.util.function.Function<PerShardStatus, String> reasonBuilder
    ) {
        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                CatalogPublishesInProgress current = AbstractPublishStateProcessor.currentCustom(currentState);
                PublishEntry entry = current.entry(publishId);
                if (entry == null || entry.phase() != PublishPhase.PUBLISHING) {
                    return currentState;
                }
                Map<ShardId, PerShardStatus> next = new LinkedHashMap<>();
                boolean changed = false;
                for (Map.Entry<ShardId, PerShardStatus> s : entry.shardStatuses().entrySet()) {
                    PerShardStatus status = s.getValue();
                    if (predicate.test(s.getKey(), status)) {
                        next.put(s.getKey(), status.withFailure(reasonBuilder.apply(status)));
                        changed = true;
                    } else {
                        next.put(s.getKey(), status);
                    }
                }
                if (!changed) return currentState;
                PublishEntry updated = entry.withShardStatuses(next);
                return AbstractPublishStateProcessor.withCustom(currentState, current.withUpdatedEntry(updated));
            }

            @Override
            public void onFailure(String src, Exception e) {
                logger.warn("[publish-{}] coordinator update [{}] failed", publishId, src, e);
            }
        });
    }
}

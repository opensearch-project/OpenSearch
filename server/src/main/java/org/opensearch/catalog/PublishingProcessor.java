/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.action.admin.cluster.catalog.PublishShardAction;
import org.opensearch.action.admin.cluster.catalog.PublishShardRequest;
import org.opensearch.action.admin.cluster.catalog.PublishShardResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link PublishPhase#PUBLISHING} → {@link PublishPhase#FINALIZING_SUCCESS} or
 * {@link PublishPhase#FINALIZING_FAILURE}.
 * <p>
 * Seeds PENDING shard statuses on first entry, dispatches {@link PublishShardAction} once
 * per publishId, and flips shard statuses based on the broadcast response. Node-departure
 * and index-deletion are handled by {@link RemoteCatalogService}; this processor observes
 * the resulting terminal statuses on the next tick.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class PublishingProcessor extends AbstractPublishStateProcessor {

    private final Client client;

    /** Per-node bookkeeping. Cleared on master-change so a new master re-dispatches. */
    private final ConcurrentHashMap<String, Boolean> dispatchedPublishIds = new ConcurrentHashMap<>();

    private final TimeValue publishTimeout;

    public PublishingProcessor(
        ClusterService clusterService,
        CatalogMetadataClient metadataClient,
        ThreadPool threadPool,
        Client client,
        int maxRetries,
        TimeValue baseBackoff,
        TimeValue publishTimeout
    ) {
        super(clusterService, metadataClient, threadPool, maxRetries, baseBackoff);
        this.client = client;
        this.publishTimeout = publishTimeout;
    }

    @Override
    protected PublishPhase expectedPhase() {
        return PublishPhase.PUBLISHING;
    }

    @Override
    public void process(PublishEntry entry) {
        // On first entry to this phase, seed PENDING shard statuses from the routing table.
        if (entry.shardStatuses().isEmpty()) {
            populatePendingShardStatuses(entry);
            return;
        }

        boolean anyFailed = false;
        boolean allTerminal = true;
        for (PerShardStatus status : entry.shardStatuses().values()) {
            if (!status.isTerminal()) {
                allTerminal = false;
                break;
            }
            if (status.state() == PerShardStatus.State.FAILED) {
                anyFailed = true;
            }
        }

        if (allTerminal) {
            PublishPhase next = anyFailed ? PublishPhase.FINALIZING_FAILURE : PublishPhase.FINALIZING_SUCCESS;
            moveToNextPhase(entry, next);
            return;
        }

        // Dispatch the broadcast at most once per publishId from this node.
        if (dispatchedPublishIds.putIfAbsent(entry.publishId(), Boolean.TRUE) == null) {
            dispatchBroadcast(entry);
        }
    }

    private void populatePendingShardStatuses(PublishEntry entry) {
        ClusterState state = clusterService.state();
        IndexMetadata indexMetadata = state.metadata().index(entry.indexName());
        if (indexMetadata == null) {
            handleRetryOnError(entry, "index [" + entry.indexName() + "] not found",
                new IllegalStateException("index metadata missing"));
            return;
        }
        IndexRoutingTable routingTable = state.routingTable().index(indexMetadata.getIndex());
        if (routingTable == null) {
            handleRetryOnError(entry, "routing table missing for index [" + entry.indexName() + "]",
                new IllegalStateException("routing table missing"));
            return;
        }

        Map<ShardId, PerShardStatus> statuses = new LinkedHashMap<>();
        for (IndexShardRoutingTable shardRouting : routingTable) {
            ShardRouting primary = shardRouting.primaryShard();
            if (primary == null) {
                statuses.put(
                    shardRouting.shardId(),
                    new PerShardStatus(PerShardStatus.State.FAILED, null, "no primary shard assigned")
                );
                continue;
            }
            String nodeId = primary.assignedToNode() ? primary.currentNodeId() : null;
            statuses.put(shardRouting.shardId(), PerShardStatus.pending(nodeId));
        }

        logger.info(
            "[publish-{}] seeding {} PENDING shard statuses for index [{}]",
            entry.publishId(), statuses.size(), entry.indexName()
        );
        submitEntryUpdate(
            "catalog-publish-seed-shards[" + entry.publishId() + "]",
            entry.publishId(),
            e -> e.withShardStatuses(statuses)
        );
    }

    private void dispatchBroadcast(PublishEntry entry) {
        PublishShardRequest request = new PublishShardRequest(entry.publishId(), entry.indexName());
        request.timeout(publishTimeout);
        logger.info(
            "[publish-{}] dispatching PublishShardAction for index [{}] with timeout {}",
            entry.publishId(), entry.indexName(), publishTimeout
        );
        client.execute(PublishShardAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(PublishShardResponse response) {
                onBroadcastComplete(entry, response);
            }

            @Override
            public void onFailure(Exception e) {
                onBroadcastFailed(entry, e);
            }
        });
    }

    private void onBroadcastComplete(PublishEntry entry, PublishShardResponse response) {
        Map<ShardId, String> failures = new LinkedHashMap<>();
        for (DefaultShardOperationFailedException f : response.getShardFailures()) {
            ShardId shardId = new ShardId(new org.opensearch.core.index.Index(f.index(), lookupIndexUUID(f.index())), f.shardId());
            failures.put(shardId, f.reason() == null ? f.getCause().toString() : f.reason());
        }

        submitEntryUpdate(
            "catalog-publish-shards-complete[" + entry.publishId() + "]",
            entry.publishId(),
            e -> {
                Map<ShardId, PerShardStatus> next = new LinkedHashMap<>();
                for (Map.Entry<ShardId, PerShardStatus> s : e.shardStatuses().entrySet()) {
                    PerShardStatus existing = s.getValue();
                    if (existing.state() != PerShardStatus.State.PENDING) {
                        next.put(s.getKey(), existing);
                        continue;
                    }
                    String failReason = failures.get(s.getKey());
                    if (failReason != null) {
                        next.put(s.getKey(), existing.withFailure(failReason));
                    } else {
                        next.put(s.getKey(), existing.withState(PerShardStatus.State.SUCCESS));
                    }
                }
                return e.withShardStatuses(next);
            }
        );
    }

    private void onBroadcastFailed(PublishEntry entry, Exception cause) {
        // Re-arm dispatch so the retry actually sends a new broadcast.
        dispatchedPublishIds.remove(entry.publishId());
        handleRetryOnError(entry, "broadcast failed: " + cause.getMessage(), cause);
    }

    private String lookupIndexUUID(String indexName) {
        IndexMetadata im = clusterService.state().metadata().index(indexName);
        return im != null ? im.getIndexUUID() : "";
    }

    void forgetDispatch(String publishId) {
        dispatchedPublishIds.remove(publishId);
    }

    void forgetAllDispatches() {
        dispatchedPublishIds.clear();
    }
}

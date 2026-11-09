/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.action.shard;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * A local implementation of {@link ShardStateAction} that applies shard state changes directly to the
 * local cluster state. This is used in clusterless mode, where there is no cluster manager.
 *
 * <p>Both changes look the copy up by allocation id in the state being updated, not by equality with the routing they
 * were given, which may be out of date. A message about an allocation that is no longer in the state, or no longer in
 * the state the message expects, changes nothing.
 *
 * <p>A routing table holds a relocation as one entry, the relocating source. The initializing target is derived from it:
 * {@link IndexShardRoutingTable#getByAllocationId} finds it, but there is no entry to remove. So a change to either end
 * of a relocation is a change to the source entry.
 */
public class LocalShardStateAction extends ShardStateAction {
    @Inject
    public LocalShardStateAction(
        ClusterService clusterService,
        TransportService transportService,
        AllocationService allocationService,
        RerouteService rerouteService,
        ThreadPool threadPool
    ) {
        super(clusterService, transportService, allocationService, rerouteService, threadPool);
    }

    @Override
    public void shardStarted(
        ShardRouting shardRouting,
        long primaryTerm,
        String message,
        ActionListener<Void> listener,
        ClusterState currentState
    ) {
        apply("shard-started " + shardRouting.shardId(), clusterState -> startShard(clusterState, shardRouting, primaryTerm), listener);
    }

    /**
     * Fails the copy in the local cluster state, as {@code RoutingNodes#failShard} would on a cluster manager: it
     * becomes unassigned, with the reason and a count of failed allocations, or, for either end of a relocation, the
     * relocation is undone the way that method undoes it.
     *
     * <p>Leaving the routing entry as it was is not a neutral choice. {@code IndicesClusterStateService} keeps a failed
     * shard in its failed-shards cache, and so never creates it again, until the shard's routing entry names a different
     * allocation. With the entry left in place, whatever next builds the local cluster state reuses it, and the copy stays
     * absent while the state still claims it is there.
     *
     * <p>Only the routing table changes. One thing {@code RoutingNodes#failShard} does for a failed primary is
     * deliberately not done: promoting an active replica. A promotion comes with a new primary term, and both are for
     * whatever builds the local cluster state to decide, as are primary terms, in-sync allocation ids, and whether and
     * where to allocate the copy again.
     */
    @Override
    public void localShardFailed(
        ShardRouting shardRouting,
        String message,
        Exception failure,
        ActionListener<Void> listener,
        ClusterState currentState
    ) {
        apply("shard-failed " + shardRouting.shardId(), clusterState -> failShard(clusterState, shardRouting, message, failure), listener);
    }

    private void apply(String source, Function<ClusterState, ClusterState> update, ActionListener<Void> listener) {
        clusterService.getClusterApplierService().updateClusterState(source, update, new ClusterApplier.ClusterApplyListener() {
            @Override
            public void onSuccess(String source) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * The state with {@code startedShard}'s copy started, or {@code clusterState} itself if the message is stale, as
     * {@code ShardStartedClusterStateTaskExecutor} judges it: the copy is gone, is no longer initializing, or is a
     * primary whose term has moved on since it recovered.
     */
    static ClusterState startShard(ClusterState clusterState, ShardRouting startedShard, long primaryTerm) {
        IndexShardRoutingTable shardRoutingTable = shardRoutingTable(clusterState, startedShard);
        ShardRouting current = shardRoutingTable == null ? null : shardRoutingTable.getByAllocationId(startedShard.allocationId().getId());
        if (current == null || current.initializing() == false) {
            return clusterState;
        }
        if (current.primary() && primaryTerm > 0) {
            IndexMetadata indexMetadata = clusterState.metadata().index(current.index());
            if (indexMetadata == null || indexMetadata.primaryTerm(current.id()) != primaryTerm) {
                return clusterState;
            }
        }

        List<ShardRouting> copies = new ArrayList<>(shardRoutingTable.shards());
        // A started relocation target replaces its source; moveToStarted finishes the relocation.
        replace(
            copies,
            current.isRelocationTarget() ? find(copies, current.allocationId().getRelocationId()) : current,
            current.moveToStarted()
        );
        return withShard(clusterState, current.shardId(), copies);
    }

    /**
     * The state with {@code failedShard}'s copy failed, or {@code clusterState} itself if that copy is no longer in it.
     */
    static ClusterState failShard(ClusterState clusterState, ShardRouting failedShard, String message, @Nullable Exception failure) {
        IndexShardRoutingTable shardRoutingTable = shardRoutingTable(clusterState, failedShard);
        ShardRouting current = shardRoutingTable == null ? null : shardRoutingTable.getByAllocationId(failedShard.allocationId().getId());
        if (current == null) {
            return clusterState;
        }
        List<ShardRouting> copies = new ArrayList<>(shardRoutingTable.shards());
        fail(copies, current, UnassignedInfo.failedShard(current, message, failure, System.nanoTime(), System.currentTimeMillis()));
        return withShard(clusterState, current.shardId(), copies);
    }

    /** As {@code RoutingNodes#failShard}, less the promotion, on a shard's entries. */
    private static void fail(List<ShardRouting> copies, ShardRouting failed, UnassignedInfo unassignedInfo) {
        if (failed.primary() && failed.isRelocationTarget() == false) {
            // The primary is about to be unassigned, and RoutingNodes asserts that a copy recovering from a primary has
            // one assigned to a node. So the replicas recovering from it fail with it, as they do on a cluster manager.
            // Re-resolved each time, because failing one may have changed the others.
            for (String recovering : recoveringReplicas(copies)) {
                ShardRouting replica = find(copies, recovering);
                if (replica != null && replica.initializing()) {
                    fail(
                        copies,
                        replica,
                        new UnassignedInfo(
                            UnassignedInfo.Reason.PRIMARY_FAILED,
                            "primary failed while replica initializing",
                            null,
                            0,
                            unassignedInfo.getUnassignedTimeInNanos(),
                            unassignedInfo.getUnassignedTimeInMillis(),
                            false,
                            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                            Collections.emptySet()
                        )
                    );
                }
            }
        }

        if (failed.isRelocationTarget()) {
            // Cancels the relocation and leaves nothing unassigned.
            ShardRouting source = find(copies, failed.allocationId().getRelocationId());
            if (source != null) {
                replace(copies, source, source.cancelRelocation());
            }
        } else if (failed.relocating() && failed.primary() == false) {
            // The target is a replica recovering like any other, and has no need of the source.
            replace(copies, failed, failed.getTargetRelocatingShard().removeRelocationSource());
        } else {
            // A relocating primary included: unassigning the source drops its target with it.
            replace(copies, failed, failed.moveToUnassigned(unassignedInfo));
        }
    }

    /** The allocation ids of the replicas initializing on this shard, relocation targets included. */
    private static List<String> recoveringReplicas(List<ShardRouting> copies) {
        List<String> recovering = new ArrayList<>();
        for (ShardRouting copy : copies) {
            if (copy.primary()) {
                continue;
            }
            if (copy.initializing()) {
                recovering.add(copy.allocationId().getId());
            } else if (copy.relocating()) {
                recovering.add(copy.getTargetRelocatingShard().allocationId().getId());
            }
        }
        return recovering;
    }

    /** The entry with this allocation id, or the relocation target derived from one, or null. */
    @Nullable
    private static ShardRouting find(List<ShardRouting> copies, String allocationId) {
        for (ShardRouting copy : copies) {
            if (copy.allocationId() == null) {
                continue;
            }
            if (copy.allocationId().getId().equals(allocationId)) {
                return copy;
            }
            if (copy.relocating() && copy.allocationId().getRelocationId().equals(allocationId)) {
                return copy.getTargetRelocatingShard();
            }
        }
        return null;
    }

    /** Replaces an entry, or, for a derived relocation target, the source entry it is derived from. */
    private static void replace(List<ShardRouting> copies, ShardRouting existing, ShardRouting replacement) {
        int index = copies.indexOf(existing);
        if (index < 0 && existing.isRelocationTarget()) {
            index = copies.indexOf(find(copies, existing.allocationId().getRelocationId()));
        }
        assert index >= 0 : existing + " is not in " + copies;
        copies.set(index, replacement);
    }

    @Nullable
    private static IndexShardRoutingTable shardRoutingTable(ClusterState clusterState, ShardRouting shard) {
        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(shard.index());
        if (indexRoutingTable == null || shard.allocationId() == null) {
            return null;
        }
        return indexRoutingTable.shard(shard.id());
    }

    private static ClusterState withShard(ClusterState clusterState, ShardId shardId, List<ShardRouting> copies) {
        IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(shardId);
        copies.forEach(shardBuilder::addShard);
        IndexShardRoutingTable changed = shardBuilder.build();

        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(shardId.getIndex());
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(shardId.getIndex());
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingTable.shardId().equals(shardId) ? changed : indexShardRoutingTable);
        }
        return ClusterState.builder(clusterState)
            .routingTable(RoutingTable.builder(clusterState.routingTable()).add(indexRoutingTableBuilder).build())
            .build();
    }
}

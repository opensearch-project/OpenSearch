/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.repositories.IndexId;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An abstract class that implements basic functionality for allocating
 * shards to nodes based on shard copies that already exist in the cluster.
 * <p>
 * Individual implementations of this class are responsible for providing
 * the logic to determine to which nodes (if any) those shards are allocated.
 *
 * @opensearch.internal
 */
public abstract class BaseGatewayShardAllocator {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * Allocate an unassigned shard to nodes (if any) where valid copies of the shard already exist.
     * It is up to the individual implementations of {@link #makeAllocationDecision(ShardRouting, RoutingAllocation, Logger)}
     * to make decisions on assigning shards to nodes.
     *
     * @param shardRouting                the shard to allocate
     * @param allocation                  the allocation state container object
     * @param unassignedAllocationHandler handles the allocation of the current shard
     */
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        final AllocateUnassignedDecision allocateUnassignedDecision = makeAllocationDecision(shardRouting, allocation, logger);
        executeDecision(shardRouting, allocateUnassignedDecision, allocation, unassignedAllocationHandler);
    }

    protected void allocateUnassignedBatchOnTimeout(Set<ShardId> shardIds, RoutingAllocation allocation, boolean primary) {
        if (shardIds.isEmpty()) {
            return;
        }
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            ShardRouting unassignedShard = iterator.next();
            AllocateUnassignedDecision allocationDecision;
            if (unassignedShard.primary() == primary && shardIds.contains(unassignedShard.shardId())) {
                if (isResponsibleFor(unassignedShard) == false) {
                    continue;
                }
                allocationDecision = AllocateUnassignedDecision.throttle(null);
                executeDecision(unassignedShard, allocationDecision, allocation, iterator);
            }
        }
    }

    /**
     * Is the allocator responsible for allocating the given {@link ShardRouting}?
     */
    protected abstract boolean isResponsibleFor(ShardRouting shardRouting);

    protected void executeDecision(
        ShardRouting shardRouting,
        AllocateUnassignedDecision allocateUnassignedDecision,
        RoutingAllocation allocation,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        if (allocateUnassignedDecision.isDecisionTaken() == false) {
            // no decision was taken by this allocator
            return;
        }

        if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
            unassignedAllocationHandler.initialize(
                allocateUnassignedDecision.getTargetNode().getId(),
                allocateUnassignedDecision.getAllocationId(),
                getExpectedShardSize(shardRouting, allocation),
                allocation.changes()
            );
        } else if (maybeAutoRestoreFromRemoteStore(shardRouting, allocateUnassignedDecision, allocation, unassignedAllocationHandler)) {
            // converted to a remote-store recovery; the shard stays unassigned and the shards allocator
            // places it in this same allocation round
        } else {
            unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
        }
    }

    /**
     * The auto-restore trigger (the allocation half of the remote store fence work): converts a primary this
     * allocator has just proven unrecoverable from any live node's local store into a remote-store recovery, instead
     * of parking it at {@link AllocationStatus#NO_VALID_SHARD_COPY} (RED) to wait for a node that may never return.
     * <p>
     * Everything here happens inside the current cluster-state computation, so the recovery-source re-point and the
     * later in-sync reset ({@code IndexMetadataUpdater}) are atomic with the allocator's no-valid-copy proof - there
     * is no second, racing restore task. The converted shard remains in the unassigned list; this allocator is no
     * longer responsible for a {@code REMOTE_STORE}-source shard, so the balanced shards allocator places it later in
     * the same round. Recovery on the target then follows the fenced sequence that already ships: seal the fence at a
     * strictly higher term, read the translog restore point after the seal, hydrate, replay.
     * <p>
     * Eligibility is deliberately narrow, and each condition is load-bearing:
     * <ul>
     * <li><b>{@code NO_VALID_SHARD_COPY} only.</b> {@code DECIDERS_NO} means valid copies exist and allocation is
     * blocked by policy - converting would abandon real data over a transient decider.</li>
     * <li><b>{@code EXISTING_STORE} source only.</b> A shard that previously STARTED and owns a remote lineage. This
     * is what excludes a resize (shrink/split/clone) target mid-{@code LOCAL_SHARDS}-recovery, whose remote store is
     * still empty and whose restore would lose the resize data, and a snapshot-restore target, which carries a
     * {@code SNAPSHOT} source and its own retry semantics.</li>
     * <li><b>Index {@code OPEN} only.</b> A closed index acknowledges nothing and may be mid-in-place-snapshot-restore;
     * its lifecycle operations own the shard.</li>
     * <li><b>Fencing required.</b> The trigger fires on the cluster manager's membership view; the departed primary
     * is typically still alive behind a partition and can still reach the object store. The fence's
     * strictly-greater-term takeover is the only thing that stops it acknowledging writes the restored copy will
     * never see. {@code FenceAutoRestore.tla} verifies the fenced trigger and refutes the unfenced one, so the
     * setting validator enforces the coupling and this check restates it defensively.</li>
     * </ul>
     * The operator's manual {@code _remotestore/_restore} performs the same mutation under a weaker guard (any
     * unassigned primary); whichever cluster-state task runs first wins and the other no-ops, since both are enabled
     * only while the primary is unassigned.
     */
    private boolean maybeAutoRestoreFromRemoteStore(
        ShardRouting shardRouting,
        AllocateUnassignedDecision decision,
        RoutingAllocation allocation,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        if (decision.getAllocationStatus() != AllocationStatus.NO_VALID_SHARD_COPY) {
            return false;
        }
        if (shardRouting.primary() == false || shardRouting.recoverySource().getType() != RecoverySource.Type.EXISTING_STORE) {
            return false;
        }
        final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        if (indexMetadata.getState() != IndexMetadata.State.OPEN) {
            return false;
        }
        // Raw VALUE reads rather than Setting#get: get() re-runs the setting validators, and this guard must DECLINE
        // on inconsistent metadata (e.g. auto_restore without fencing), never throw inside the reroute path. The KEYS
        // come from the Setting constants themselves, so a setting rename cannot silently detach this guard from the
        // settings the operator actually toggles.
        final Settings indexSettings = indexMetadata.getSettings();
        if (indexSettings.getAsBoolean(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), false) == false
            || indexSettings.getAsBoolean(IndexMetadata.INDEX_REMOTE_STORE_FENCING_ENABLED_SETTING.getKey(), false) == false
            || indexSettings.getAsBoolean(IndexMetadata.INDEX_REMOTE_STORE_AUTO_RESTORE_ENABLED_SETTING.getKey(), false) == false) {
            return false;
        }
        // As on the manual restore path (RemoteStoreRestoreService), this IndexId is unrelated to snapshot restore,
        // so the ctor without a pathType is used: the remote path layout is resolved from the index's remote store
        // custom metadata by the directory factories, never from the recovery source's IndexId.
        final RecoverySource.RemoteStoreRecoverySource recoverySource = new RecoverySource.RemoteStoreRecoverySource(
            UUIDs.randomBase64UUID(),
            indexMetadata.getCreationVersion(),
            new IndexId(shardRouting.getIndexName(), indexMetadata.getIndexUUID(), IndexId.DEFAULT_SHARD_PATH_TYPE)
        );
        // A fresh UnassignedInfo with the original reason and a zero failure count: the conversion is not a failed
        // allocation. Health stays RED while the restore hydrates (a hydrating primary cannot serve queries; see
        // ClusterShardHealth#getInactivePrimaryHealth) and converges to GREEN without operator action.
        final UnassignedInfo unassignedInfo = new UnassignedInfo(
            shardRouting.unassignedInfo().getReason(),
            "auto-restoring from remote store: no valid local copy on any live node [" + shardRouting.unassignedInfo().getMessage() + "]",
            null,
            0,
            shardRouting.unassignedInfo().getUnassignedTimeInNanos(),
            shardRouting.unassignedInfo().getUnassignedTimeInMillis(),
            false,
            AllocationStatus.NO_ATTEMPT,
            shardRouting.unassignedInfo().getFailedNodeIds()
        );
        logger.info(
            "[{}][{}] no valid shard copy on any live node; auto-restoring primary from remote store",
            shardRouting.getIndexName(),
            shardRouting.shardId().id()
        );
        unassignedAllocationHandler.updateUnassigned(unassignedInfo, recoverySource, allocation.changes());
        return true;
    }

    protected long getExpectedShardSize(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            if (shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                return allocation.snapshotShardSizeInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            } else {
                return ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;
            }
        } else {
            return allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        }
    }

    /**
     * Make a decision on the allocation of an unassigned shard.  This method is used by
     * {@link #allocateUnassigned(ShardRouting, RoutingAllocation, ExistingShardsAllocator.UnassignedAllocationHandler)} to make decisions
     * about whether or not the shard can be allocated by this allocator and if so, to which node it will be allocated.
     *
     * @param unassignedShard the unassigned shard to allocate
     * @param allocation      the current routing state
     * @param logger          the logger
     * @return an {@link AllocateUnassignedDecision} with the final decision of whether to allocate and details of the decision
     */
    public abstract AllocateUnassignedDecision makeAllocationDecision(
        ShardRouting unassignedShard,
        RoutingAllocation allocation,
        Logger logger
    );

    /**
     * Builds decisions for all nodes in the cluster, so that the explain API can provide information on
     * allocation decisions for each node, while still waiting to allocate the shard (e.g. due to fetching shard data).
     */
    protected static List<NodeAllocationResult> buildDecisionsForAllNodes(ShardRouting shard, RoutingAllocation allocation) {
        List<NodeAllocationResult> results = new ArrayList<>();
        for (RoutingNode node : allocation.routingNodes()) {
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            results.add(new NodeAllocationResult(node.node(), null, decision));
        }
        return results;
    }
}

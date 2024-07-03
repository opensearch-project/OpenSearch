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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode;
import org.opensearch.node.remotestore.RemoteStoreNodeService.Direction;

/**
 * A new allocation decider for migration of document replication clusters to remote store backed clusters:
 * - For STRICT compatibility mode, the decision is always YES
 * - For remote store backed indices, relocation or allocation/relocation can only be towards a remote node
 * - For "REMOTE_STORE" migration direction:
 *      - New primary shards can only be allocated to a remote node
 *      - New replica shards can be allocated to a remote node iff the primary has been migrated/allocated to a remote node
 * - For other directions ("DOCREP", "NONE"), the decision is always YES
 *
 * @opensearch.internal
 */
public class RemoteStoreMigrationAllocationDecider extends AllocationDecider {

    public static final String NAME = "remote_store_migration";

    volatile private Direction migrationDirection;
    volatile private CompatibilityMode compatibilityMode;

    public RemoteStoreMigrationAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.migrationDirection = RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING.get(settings);
        this.compatibilityMode = RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING, this::setMigrationDirection);
        clusterSettings.addSettingsUpdateConsumer(
            RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING,
            this::setCompatibilityMode
        );
    }

    private void setMigrationDirection(Direction migrationDirection) {
        this.migrationDirection = migrationDirection;
    }

    private void setCompatibilityMode(CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        DiscoveryNode targetNode = node.node();

        if (compatibilityMode.equals(CompatibilityMode.STRICT)) {
            // assuming all nodes are of the same type (all remote or all non-remote)
            return allocation.decision(
                Decision.YES,
                NAME,
                getDecisionDetails(true, shardRouting, targetNode, " for strict compatibility mode")
            );
        }

        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        boolean remoteSettingsBackedIndex = IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexMetadata.getSettings());

        if (migrationDirection.equals(Direction.NONE)) {
            // remote backed indices on docrep nodes and non remote backed indices on remote nodes are not allowed
            boolean isNoDecision = remoteSettingsBackedIndex ^ targetNode.isRemoteStoreNode();
            String reason = " for " + (remoteSettingsBackedIndex ? "" : "non ") + "remote store backed index";
            return allocation.decision(
                isNoDecision ? Decision.NO : Decision.YES,
                NAME,
                getDecisionDetails(!isNoDecision, shardRouting, targetNode, reason)
            );
        } else if (migrationDirection.equals(Direction.DOCREP)) {
            // docrep migration direction is currently not supported
            return allocation.decision(Decision.YES, NAME, getDecisionDetails(true, shardRouting, targetNode, " for DOCREP direction"));
        } else {
            // check for remote store backed indices
            if (remoteSettingsBackedIndex && targetNode.isRemoteStoreNode() == false) {
                // allocations and relocations must be to a remote node
                String reason = new StringBuilder(" because a remote store backed index's shard copy can only be ").append(
                    (shardRouting.assignedToNode() == false) ? "allocated" : "relocated"
                ).append(" to a remote node").toString();
                return allocation.decision(Decision.NO, NAME, getDecisionDetails(false, shardRouting, targetNode, reason));
            }

            if (shardRouting.primary()) {
                return primaryShardDecision(shardRouting, targetNode, allocation);
            }
            return replicaShardDecision(shardRouting, targetNode, allocation);
        }
    }

    // handle scenarios for allocation of a new shard's primary copy
    private Decision primaryShardDecision(ShardRouting primaryShardRouting, DiscoveryNode targetNode, RoutingAllocation allocation) {
        if (targetNode.isRemoteStoreNode() == false) {
            return allocation.decision(Decision.NO, NAME, getDecisionDetails(false, primaryShardRouting, targetNode, ""));
        }
        return allocation.decision(Decision.YES, NAME, getDecisionDetails(true, primaryShardRouting, targetNode, ""));
    }

    // Checks if primary shard is on a remote node.
    static boolean isPrimaryOnRemote(ShardId shardId, RoutingAllocation allocation) {
        ShardRouting primaryShardRouting = allocation.routingNodes().activePrimary(shardId);
        if (primaryShardRouting != null) {
            DiscoveryNode primaryShardNode = allocation.nodes().getNodes().get(primaryShardRouting.currentNodeId());
            return primaryShardNode.isRemoteStoreNode();
        }
        return false;
    }

    private Decision replicaShardDecision(ShardRouting replicaShardRouting, DiscoveryNode targetNode, RoutingAllocation allocation) {
        if (targetNode.isRemoteStoreNode()) {
            boolean primaryOnRemote = RemoteStoreMigrationAllocationDecider.isPrimaryOnRemote(replicaShardRouting.shardId(), allocation);
            if (primaryOnRemote == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    getDecisionDetails(false, replicaShardRouting, targetNode, " since primary shard copy is not yet migrated to remote")
                );
            }
            return allocation.decision(
                Decision.YES,
                NAME,
                getDecisionDetails(true, replicaShardRouting, targetNode, " since primary shard copy has been migrated to remote")
            );
        }
        return allocation.decision(Decision.YES, NAME, getDecisionDetails(true, replicaShardRouting, targetNode, ""));
    }

    // get detailed reason for the decision
    private String getDecisionDetails(boolean isYes, ShardRouting shardRouting, DiscoveryNode targetNode, String reason) {
        return new StringBuilder("[").append(migrationDirection.direction)
            .append(" migration_direction]: ")
            .append(shardRouting.primary() ? "primary" : "replica")
            .append(" shard copy ")
            .append(isYes ? "can" : "can not")
            .append(" be ")
            .append((shardRouting.assignedToNode() == false) ? "allocated" : "relocated")
            .append(" to a ")
            .append(targetNode.isRemoteStoreNode() ? "remote" : "non-remote")
            .append(" node")
            .append(reason)
            .toString();
    }

}

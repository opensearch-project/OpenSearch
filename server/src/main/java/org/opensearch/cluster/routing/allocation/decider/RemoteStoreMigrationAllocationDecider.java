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
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.node.remotestore.RemoteStoreNodeService.Direction;
import org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode;

/**
 * An allocation decider to oversee shard allocation or relocation to facilitate remote-store migration:
 * - For STRICT compatibility mode, the decision is always YES
 *  * - For remote_store_enabled indices, relocation or allocation/relocation can only be towards a remote node
 * - For "REMOTE_STORE" migration direction:
 *      - New primary shards can only be allocated to a remote node
 *      - New replica shards can be allocated to a remote node iff the primary has been migrated/allocated to a remote node
 * - For other directions ("DOCREP", "NONE"), the decision is always YES
 *
 * @opensearch.internal
 */
public class RemoteStoreMigrationAllocationDecider extends AllocationDecider {

    public static final String NAME = "remote_store_migration";

    private Direction migrationDirection;
    private CompatibilityMode compatibilityMode;
    private boolean remoteStoreEnabledIndex = false;

    public RemoteStoreMigrationAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.migrationDirection = RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING.get(settings);
        this.compatibilityMode = RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING.get(settings);
        this.remoteStoreEnabledIndex = IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(settings);
        if (IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.exists(settings)) {
            remoteStoreEnabledIndex = IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(settings);
        }
        clusterSettings.addSettingsUpdateConsumer(
            RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING,
            this::setMigrationDirection
        );
        clusterSettings.addSettingsUpdateConsumer(
            RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING,
            this::setCompatibilityMode
        );
    }

    // listen for changes in migration direction of cluster
    private void setMigrationDirection(Direction migrationDirection) {
        this.migrationDirection = migrationDirection;
    }

    // listen for changes in compatibility mode of cluster
    private void setCompatibilityMode (CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        DiscoveryNode targetNode = node.node();

        if (compatibilityMode.equals(CompatibilityMode.STRICT)) {
            return allocation.decision(Decision.YES, NAME,
                getDecisionDetails(true, shardRouting, targetNode, " for STRICT compatibility mode"));
        }

        if (!migrationDirection.equals(Direction.REMOTE_STORE)) {
            return allocation.decision(Decision.YES, NAME,
                getDecisionDetails(true, shardRouting, targetNode, " for non remote_store direction"));
        }

        String reasonToDecline = checkIndexMetadata(shardRouting, targetNode, allocation);
        if (reasonToDecline != null) {
            return allocation.decision(Decision.NO, NAME,
                getDecisionDetails(false, shardRouting, targetNode, reasonToDecline));
        }

        if (shardRouting.primary()) {
            return primaryShardDecision(shardRouting, targetNode, allocation);
        }
        return replicaShardDecision(shardRouting, targetNode, allocation);
    }

    // handle scenarios for allocation of a new shard's primary copy
    private Decision primaryShardDecision(
        ShardRouting primaryShardRouting,
        DiscoveryNode targetNode,
        RoutingAllocation allocation
    ) {
        if (!targetNode.isRemoteStoreNode()) {
            return allocation.decision(Decision.NO, NAME,
                getDecisionDetails(false, primaryShardRouting, targetNode, ""));
        }
        return allocation.decision(Decision.YES, NAME,
            getDecisionDetails(true, primaryShardRouting, targetNode, ""));
    }

    private Decision replicaShardDecision(
        ShardRouting replicaShardRouting,
        DiscoveryNode targetNode,
        RoutingAllocation allocation
    ) {
        if (targetNode.isRemoteStoreNode()) {
            ShardRouting primaryShardRouting = allocation.routingNodes().activePrimary(replicaShardRouting.shardId());
            DiscoveryNode primaryShardNode = allocation.nodes().getNodes().get(primaryShardRouting.currentNodeId());
            if (!primaryShardNode.isRemoteStoreNode()) {
                return allocation.decision(Decision.NO, NAME,
                    getDecisionDetails(false, replicaShardRouting, targetNode,
                        " since primary shard copy is not yet migrated to remote"));
            }
            return allocation.decision(Decision.YES, NAME,
                getDecisionDetails(true, replicaShardRouting, targetNode,
                    " since primary shard copy has been migrated to remote"));
        }
        return allocation.decision(Decision.YES, NAME,
            getDecisionDetails(true, replicaShardRouting, targetNode, ""));
    }

    // check for remote_store_enabled indices
    private String checkIndexMetadata(ShardRouting shardRouting, DiscoveryNode targetNode, RoutingAllocation allocation) {
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        if (IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.exists(indexMetadata.getSettings())) {
            remoteStoreEnabledIndex = IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexMetadata.getSettings());
        }
        // allocations and relocations must be towards a remote node
        if (remoteStoreEnabledIndex && !targetNode.isRemoteStoreNode()) {
            return " because a remote_store_enabled index's shard copy can only move towards a remote_store node";
        }
        return null;
    }

    // get given node's type
    private String getNodeType(DiscoveryNode node) {
        return(node.isRemoteStoreNode() ? "remote_store" : "non_remote_store");
    }

    // get detailed reason for the decision
    private String getDecisionDetails(
        boolean isYes,
        ShardRouting shardRouting,
        DiscoveryNode targetNode,
        String reason
    ) {
        return String.format(
            "[%s migration_direction]: %s shard copy %s be %s to a %s node%s",
            migrationDirection,
            (shardRouting.primary() ? "primary" : "replica"),
            (isYes ? "can" : "can not"),
            (!shardRouting.assignedToNode() ? "allocated" : "relocated"),
            getNodeType(targetNode),
            reason
        );
    }

}

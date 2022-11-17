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

package org.opensearch.index.shard;

import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Replication group for a shard. Used by a primary shard to coordinate replication and recoveries.
 *
 * @opensearch.internal
 */
public class ReplicationGroup {
    private final IndexShardRoutingTable routingTable;
    private final Set<String> inSyncAllocationIds;
    private final Set<String> trackedAllocationIds;
    private final Set<String> localTranslogAllocationIds;
    private final long version;
    private final boolean remoteTranslogEnabled;

    private final Set<String> unavailableInSyncShards; // derived from the other fields
    private final List<ReplicationAwareShardRouting> replicationTargets; // derived from the other fields
    private final List<ShardRouting> skippedShards; // derived from the other fields

    public ReplicationGroup(
        IndexShardRoutingTable routingTable,
        Set<String> inSyncAllocationIds,
        Set<String> trackedAllocationIds,
        Set<String> localTranslogAllocationIds,
        long version,
        boolean remoteTranslogEnabled
    ) {
        if (!remoteTranslogEnabled) {
            assert trackedAllocationIds.equals(localTranslogAllocationIds)
                : "In absence of remote translog store, all tracked shards must have local translog store";
        }
        this.routingTable = routingTable;
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.trackedAllocationIds = trackedAllocationIds;
        this.localTranslogAllocationIds = localTranslogAllocationIds;
        this.version = version;
        this.remoteTranslogEnabled = remoteTranslogEnabled;

        this.unavailableInSyncShards = Sets.difference(inSyncAllocationIds, routingTable.getAllAllocationIds());
        this.replicationTargets = new ArrayList<>();
        this.skippedShards = new ArrayList<>();
        for (final ShardRouting shard : routingTable) {
            if (shard.unassigned()) {
                assert shard.primary() == false : "primary shard should not be unassigned in a replication group: " + shard;
                skippedShards.add(shard);
            } else {
                if (trackedAllocationIds.contains(shard.allocationId().getId())) {
                    replicationTargets.add(
                        new ReplicationAwareShardRouting(
                            remoteTranslogEnabled,
                            localTranslogAllocationIds.contains(shard.allocationId().getId()),
                            shard
                        )
                    );
                } else {
                    assert inSyncAllocationIds.contains(shard.allocationId().getId()) == false : "in-sync shard copy but not tracked: "
                        + shard;
                    skippedShards.add(shard);
                }
                if (shard.relocating()) {
                    ShardRouting relocationTarget = shard.getTargetRelocatingShard();
                    if (trackedAllocationIds.contains(relocationTarget.allocationId().getId())) {
                        replicationTargets.add(
                            new ReplicationAwareShardRouting(
                                remoteTranslogEnabled,
                                localTranslogAllocationIds.contains(relocationTarget.allocationId().getId()),
                                relocationTarget
                            )
                        );
                    } else {
                        skippedShards.add(relocationTarget);
                        assert inSyncAllocationIds.contains(relocationTarget.allocationId().getId()) == false
                            : "without remote translog, in-sync shard copy but not tracked: " + shard;
                    }
                }
            }
        }
    }

    public ReplicationGroup(
        IndexShardRoutingTable routingTable,
        Set<String> inSyncAllocationIds,
        Set<String> trackedAllocationIds,
        long version
    ) {
        this(routingTable, inSyncAllocationIds, trackedAllocationIds, Collections.emptySet(), version, false);
    }

    public long getVersion() {
        return version;
    }

    public IndexShardRoutingTable getRoutingTable() {
        return routingTable;
    }

    public Set<String> getInSyncAllocationIds() {
        return inSyncAllocationIds;
    }

    public Set<String> getTrackedAllocationIds() {
        return trackedAllocationIds;
    }

    /**
     * Returns the set of shard allocation ids that are in the in-sync set but have no assigned routing entry
     */
    public Set<String> getUnavailableInSyncShards() {
        return unavailableInSyncShards;
    }

    /**
     * Returns the subset of shards in the routing table that should be replicated to basis the remoteTranslogEnabled and
     * replicated flag. Includes relocation targets.
     */
    public List<ReplicationAwareShardRouting> getReplicationTargets() {
        return replicationTargets;
    }

    /**
     * Returns the subset of shards in the routing table that are unassigned or initializing and not ready yet to receive operations
     * (i.e. engine not opened yet). Includes relocation targets.
     */
    public List<ShardRouting> getSkippedShards() {
        return skippedShards;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicationGroup that = (ReplicationGroup) o;

        if (!routingTable.equals(that.routingTable)) return false;
        if (!inSyncAllocationIds.equals(that.inSyncAllocationIds)) return false;
        if (!trackedAllocationIds.equals(that.trackedAllocationIds)) return false;
        if (!localTranslogAllocationIds.equals(that.localTranslogAllocationIds)) return false;
        return remoteTranslogEnabled == that.remoteTranslogEnabled;
    }

    @Override
    public int hashCode() {
        int result = routingTable.hashCode();
        result = 31 * result + inSyncAllocationIds.hashCode();
        result = 31 * result + trackedAllocationIds.hashCode();
        result = 31 * result + localTranslogAllocationIds.hashCode();
        result = 31 * result + Boolean.hashCode(remoteTranslogEnabled);
        return result;
    }

    @Override
    public String toString() {
        return "ReplicationGroup{"
            + "routingTable="
            + routingTable
            + ", inSyncAllocationIds="
            + inSyncAllocationIds
            + ", trackedAllocationIds="
            + trackedAllocationIds
            + ", localTranslogAllocationIds="
            + localTranslogAllocationIds
            + ", remoteTranslogEnabled="
            + remoteTranslogEnabled
            + '}';
    }

    /**
     * Replication aware ShardRouting used for fanning out replication requests smartly.
     */
    public static final class ReplicationAwareShardRouting {

        private final boolean remoteTranslogEnabled;

        private final boolean replicated;

        private final ShardRouting shardRouting;

        public boolean isRemoteTranslogEnabled() {
            return remoteTranslogEnabled;
        }

        public boolean isReplicated() {
            return replicated;
        }

        public ShardRouting getShardRouting() {
            return shardRouting;
        }

        public ReplicationAwareShardRouting(
            final boolean remoteTranslogEnabled,
            final boolean replicated,
            final ShardRouting shardRouting
        ) {
            // Either remoteTranslogEnabled or replicated is true. It is not possible that a shard is nether having remoteTranslogEnabled as
            // true nor replicated as true.
            assert remoteTranslogEnabled || replicated;
            // ShardRouting has to be non-null always.
            assert Objects.nonNull(shardRouting);
            this.remoteTranslogEnabled = remoteTranslogEnabled;
            this.replicated = replicated;
            this.shardRouting = shardRouting;
        }
    }

}

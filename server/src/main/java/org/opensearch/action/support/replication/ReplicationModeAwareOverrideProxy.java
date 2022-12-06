/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.action.support.replication.ReplicationOperation.ReplicationOverridePolicy;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.shard.ReplicationGroup.ReplicationModeAwareShardRouting;

import java.util.Objects;

/**
 * This implementation of {@link ReplicationProxy} fans out the replication request to current shard routing basis
 * the shard routing's replication mode and replication override policy.
 *
 * @opensearch.internal
 */
public class ReplicationModeAwareOverrideProxy<ReplicaRequest> extends ReplicationProxy<ReplicaRequest> {

    private final ReplicationOverridePolicy overridePolicy;

    public ReplicationModeAwareOverrideProxy(ReplicationOverridePolicy overridePolicy) {
        assert Objects.nonNull(overridePolicy);
        this.overridePolicy = overridePolicy;
    }

    @Override
    ReplicationTracker.ReplicationMode determineReplicationMode(
        ReplicationModeAwareShardRouting shardRouting,
        ShardRouting primaryRouting
    ) {
        ShardRouting currentRouting = shardRouting.getShardRouting();

        // If the current routing is the primary, then it does not need to be replicated
        if (currentRouting.isSameAllocation(primaryRouting)) {
            return ReplicationTracker.ReplicationMode.NO_REPLICATION;
        }

        // If the current routing's replication mode is not NONE, then we return the original replication mode.
        if (shardRouting.getReplicationMode() != ReplicationTracker.ReplicationMode.NO_REPLICATION) {
            return shardRouting.getReplicationMode();
        }

        // If the current routing's replication mode is none, then we check for override and return overridden mode.
        if (Objects.nonNull(overridePolicy)) {
            return overridePolicy.getOverriddenMode();
        }

        // At the end, return NONE.
        return ReplicationTracker.ReplicationMode.NO_REPLICATION;
    }
}

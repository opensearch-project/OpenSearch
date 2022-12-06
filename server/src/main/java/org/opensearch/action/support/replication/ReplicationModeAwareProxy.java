/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.seqno.ReplicationTracker.ReplicationMode;
import org.opensearch.index.shard.ReplicationGroup.ReplicationModeAwareShardRouting;

import java.util.Objects;

/**
 * This implementation of {@link ReplicationProxy} fans out the replication request to current shard routing basis
 * the shard routing's replication mode and replication override policy.
 *
 * @opensearch.internal
 */
public class ReplicationModeAwareProxy<ReplicaRequest> extends ReplicationProxy<ReplicaRequest> {

    private final ReplicationMode replicationModeOverride;

    public ReplicationModeAwareProxy(ReplicationMode replicationModeOverride) {
        assert Objects.nonNull(replicationModeOverride);
        this.replicationModeOverride = replicationModeOverride;
    }

    @Override
    ReplicationMode determineReplicationMode(ReplicationModeAwareShardRouting shardRouting, ShardRouting primaryRouting) {
        ShardRouting currentRouting = shardRouting.getShardRouting();

        // If the current routing is the primary, then it does not need to be replicated
        if (currentRouting.isSameAllocation(primaryRouting)) {
            return ReplicationMode.NO_REPLICATION;
        }

        // If the current routing's replication mode is full replication or primary term validation, then we return the original mode
        if (shardRouting.getReplicationMode() == ReplicationMode.FULL_REPLICATION
            || shardRouting.getReplicationMode() == ReplicationMode.PRIMARY_TERM_VALIDATION) {
            return shardRouting.getReplicationMode();
        }
        return replicationModeOverride;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.shard.ReplicationGroup;

import static org.opensearch.index.seqno.ReplicationTracker.ReplicationMode;

/**
 * This implementation of {@link ReplicationProxy} fans out the replication request to current shard routing if
 * it is not the primary and has replication mode as {@link ReplicationMode#FULL_REPLICATION}.
 *
 * @opensearch.internal
 */
public class FanoutReplicationProxy<ReplicaRequest> extends ReplicationProxy<ReplicaRequest> {

    @Override
    ReplicationMode determineReplicationMode(ReplicationGroup.ReplicationModeAwareShardRouting shardRouting, ShardRouting primaryRouting) {
        return shardRouting.getShardRouting().isSameAllocation(primaryRouting) == false
            ? ReplicationMode.FULL_REPLICATION
            : ReplicationMode.NO_REPLICATION;
    }
}

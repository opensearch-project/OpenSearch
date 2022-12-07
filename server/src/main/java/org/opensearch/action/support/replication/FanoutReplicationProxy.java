/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.cluster.routing.ShardRouting;

/**
 * This implementation of {@link ReplicationProxy} fans out the replication request to current shard routing if
 * it is not the primary and has replication mode as {@link ReplicationMode#FULL_REPLICATION}.
 *
 * @opensearch.internal
 */
public class FanoutReplicationProxy<ReplicaRequest> extends ReplicationProxy<ReplicaRequest> {

    @Override
    ReplicationMode determineReplicationMode(ShardRouting shardRouting, ShardRouting primaryRouting) {
        return shardRouting.isSameAllocation(primaryRouting) == false ? ReplicationMode.FULL_REPLICATION : ReplicationMode.NO_REPLICATION;
    }
}

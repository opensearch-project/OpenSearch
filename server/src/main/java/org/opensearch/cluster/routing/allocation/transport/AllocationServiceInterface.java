/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.transport;

import java.io.IOException;
import java.util.List;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AutoExpandReplicas;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.routing.allocation.StaleShard;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.common.unit.TimeValue;

public interface AllocationServiceInterface {
    ClusterState applyStartedShards(ClusterState clusterState, List<ShardRouting> startedShards);

    // Used for testing
    ClusterState applyFailedShard(ClusterState clusterState, ShardRouting failedShard, boolean markAsStale);

    // Used for testing
    ClusterState applyFailedShards(ClusterState clusterState, List<FailedShard> failedShards);

    ClusterState applyFailedShards(
        ClusterState clusterState, List<FailedShard> failedShards, List<StaleShard> staleShards
    );

    ClusterState disassociateDeadNodes(ClusterState clusterState, boolean reroute, String reason);

    AllocationService.CommandsResult reroute(ClusterState clusterState, AllocationCommands commands, boolean explain, boolean retryFailed);

    ClusterState reroute(ClusterState clusterState, String reason) ;


}

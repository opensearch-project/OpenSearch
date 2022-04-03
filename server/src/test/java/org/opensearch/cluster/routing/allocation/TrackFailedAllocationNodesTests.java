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

package org.opensearch.cluster.routing.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.common.settings.Settings;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class TrackFailedAllocationNodesTests extends OpenSearchAllocationTestCase {

    public void testTrackFailedNodes() {
        int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        AllocationService allocationService = createAllocationService();
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
        for (int i = 0; i < 5; i++) {
            discoNodes.add(newNode("node-" + i));
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(discoNodes)
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("idx")).build())
            .build();
        clusterState = allocationService.reroute(clusterState, "reroute");
        Set<String> failedNodeIds = new HashSet<>();

        // track the failed nodes if shard is not started
        for (int i = 0; i < maxRetries; i++) {
            failedNodeIds.add(clusterState.routingTable().index("idx").shard(0).shards().get(0).currentNodeId());
            clusterState = allocationService.applyFailedShard(
                clusterState,
                clusterState.routingTable().index("idx").shard(0).shards().get(0),
                randomBoolean()
            );
            assertThat(
                clusterState.routingTable().index("idx").shard(0).shards().get(0).unassignedInfo().getFailedNodeIds(),
                equalTo(failedNodeIds)
            );
        }

        // reroute with retryFailed=true should discard the failedNodes
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).state(), equalTo(ShardRoutingState.UNASSIGNED));
        clusterState = allocationService.reroute(clusterState, new AllocationCommands(), false, true).getClusterState();
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).unassignedInfo().getFailedNodeIds(), empty());

        // do not track the failed nodes while shard is started
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).state(), equalTo(ShardRoutingState.STARTED));
        clusterState = allocationService.applyFailedShard(
            clusterState,
            clusterState.routingTable().index("idx").shard(0).shards().get(0),
            false
        );
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).unassignedInfo().getFailedNodeIds(), empty());
    }
}

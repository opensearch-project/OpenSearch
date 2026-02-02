/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.Nodes;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodeRoleGenerator;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CatNodesResponseTests extends OpenSearchTestCase {

    public void testGetAndSetCatNodesResponse() {
        ClusterName clusterName = new ClusterName("cluster-1");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();

        Set<DiscoveryNodeRole> roles = new HashSet<>();
        String roleName = "test_role";
        DiscoveryNodeRole testRole = DiscoveryNodeRoleGenerator.createDynamicRole(roleName);
        roles.add(testRole);

        builder.add(new DiscoveryNode("node-1", buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT));
        DiscoveryNodes discoveryNodes = builder.build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(clusterName, clusterState, false);
        NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(clusterName, Collections.emptyList(), Collections.emptyList());
        NodesStatsResponse nodesStatsResponse = new NodesStatsResponse(clusterName, Collections.emptyList(), Collections.emptyList());
        CatNodesResponse catShardsResponse = new CatNodesResponse(clusterStateResponse, nodesInfoResponse, nodesStatsResponse);

        assertEquals(nodesStatsResponse, catShardsResponse.getNodesStatsResponse());
        assertEquals(nodesInfoResponse, catShardsResponse.getNodesInfoResponse());
        assertEquals(clusterStateResponse, catShardsResponse.getClusterStateResponse());

    }
}

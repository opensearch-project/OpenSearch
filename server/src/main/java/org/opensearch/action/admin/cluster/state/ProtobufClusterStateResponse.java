/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.action.admin.cluster.state;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.server.proto.ClusterStateResponseProto;
import org.opensearch.server.proto.ClusterStateResponseProto.ClusterStateRes;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The response for getting the cluster state.
*
* @opensearch.internal
*/
public class ProtobufClusterStateResponse extends ProtobufActionResponse {

    private ClusterStateResponseProto.ClusterStateRes clusterStateRes;

    public ProtobufClusterStateResponse(String clusterName, DiscoveryNodes nodes, long version, String stateUUID, boolean waitForTimedOut) {
        ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Builder discoveryNodesBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.newBuilder();

        List<ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> allNodes = convertNodes(nodes);
        discoveryNodesBuilder.addAllAllNodes(allNodes).setClusterManagerNodeId(nodes.getClusterManagerNodeId()).setLocalNodeId(nodes.getLocalNodeId()).setMinNonClientNodeVersion(nodes.getSmallestNonClientNodeVersion().toString()).setMaxNonClientNodeVersion(nodes.getLargestNonClientNodeVersion().toString()).setMinNodeVersion(nodes.getMinNodeVersion().toString()).setMaxNodeVersion(nodes.getMaxNodeVersion().toString());
        ClusterStateResponseProto.ClusterStateRes.ClusterState.Builder clusterStateBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.newBuilder();
        clusterStateBuilder.setClusterName(clusterName)
            .setVersion(version)
            .setStateUUID(stateUUID)
            .setNodes(discoveryNodesBuilder.build());
        this.clusterStateRes = ClusterStateResponseProto.ClusterStateRes.newBuilder()
                                            .setClusterName(clusterName)
                                            .setClusterState(clusterStateBuilder.build())
                                            .setWaitForTimedOut(waitForTimedOut)
                                            .build();
    }

    private List<ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> convertNodes(DiscoveryNodes nodes) {
        List<ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> convertedNodes = new ArrayList<>();
        if (nodes.getNodes().isEmpty()) {
            return convertedNodes;
        }
        for (ObjectCursor<DiscoveryNode> node : nodes.getNodes().values()) {
            List<ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.NodeRole> nodeRoles = new ArrayList<>();
            node.value.getRoles().forEach(role -> {
                ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.NodeRole.Builder nodeRoleBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.NodeRole.newBuilder();
                nodeRoleBuilder.setIsKnownRole(role.isKnownRole()).setIsDynamicRole(role.isDynamicRole()).setRoleName(role.roleName()).setRoleNameAbbreviation(role.roleNameAbbreviation()).setCanContainData(role.canContainData()).build();
                nodeRoles.add(nodeRoleBuilder.build());
            });
            ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.Builder nodeBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.newBuilder();
            nodeBuilder.setNodeName(node.value.getName()).setNodeId(node.value.getId()).setEphemeralId(node.value.getEphemeralId()).setHostName(node.value.getHostName())
            .setHostAddress(node.value.getHostAddress()).setTransportAddress(node.value.getAddress().toString()).putAllAttributes(node.value.getAttributes()).addAllRoles(nodeRoles).setVersion(node.value.getVersion().toString()).build();
            convertedNodes.add(nodeBuilder.build());
        }
        return convertedNodes;
    }

    @Override
    public String toString() {
        return "ProtobufClusterStateResponse{" + "clusterState=" + this.clusterStateRes.getClusterState() + '}';
    }

    public ClusterStateRes response() {
        return this.clusterStateRes;
    }

    public ProtobufClusterStateResponse(byte[] data) throws IOException {
        this.clusterStateRes = ClusterStateResponseProto.ClusterStateRes.parseFrom(data);
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(this.clusterStateRes.toByteArray());
    }
}

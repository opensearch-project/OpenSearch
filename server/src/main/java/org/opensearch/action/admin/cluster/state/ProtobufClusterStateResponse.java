/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.state;

import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.server.proto.ClusterStateResponseProto;
import org.opensearch.server.proto.ClusterStateResponseProto.ClusterStateResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * The response for getting the cluster state.
*
* @opensearch.internal
*/
public class ProtobufClusterStateResponse extends ProtobufActionResponse {

    private ClusterStateResponseProto.ClusterStateResponse clusterStateRes;

    public ProtobufClusterStateResponse(String clusterName, DiscoveryNodes nodes, long version, String stateUUID, boolean waitForTimedOut) {
        ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Builder discoveryNodesBuilder =
            ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.newBuilder();

        List<ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node> allNodes = convertNodes(nodes);
        discoveryNodesBuilder.addAllAllNodes(allNodes)
            .setClusterManagerNodeId(nodes.getClusterManagerNodeId())
            .setLocalNodeId(nodes.getLocalNodeId())
            .setMinNonClientNodeVersion(nodes.getSmallestNonClientNodeVersion().toString())
            .setMaxNonClientNodeVersion(nodes.getLargestNonClientNodeVersion().toString())
            .setMinNodeVersion(nodes.getMinNodeVersion().toString())
            .setMaxNodeVersion(nodes.getMaxNodeVersion().toString());
        ClusterStateResponseProto.ClusterStateResponse.ClusterState.Builder clusterStateBuilder =
            ClusterStateResponseProto.ClusterStateResponse.ClusterState.newBuilder();
        clusterStateBuilder.setClusterName(clusterName).setVersion(version).setStateUUID(stateUUID).setNodes(discoveryNodesBuilder.build());
        this.clusterStateRes = ClusterStateResponseProto.ClusterStateResponse.newBuilder()
            .setClusterName(clusterName)
            .setClusterState(clusterStateBuilder.build())
            .setWaitForTimedOut(waitForTimedOut)
            .build();
    }

    private List<ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node> convertNodes(DiscoveryNodes nodes) {
        List<ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node> convertedNodes = new ArrayList<>();
        if (nodes.getNodes().isEmpty()) {
            return convertedNodes;
        }
        for (DiscoveryNode node : nodes.getNodes().values()) {
            List<ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node.NodeRole> nodeRoles = new ArrayList<>();
            node.getRoles().forEach(role -> {
                ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node.NodeRole.Builder nodeRoleBuilder =
                    ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node.NodeRole.newBuilder();
                nodeRoleBuilder.setIsKnownRole(role.isKnownRole())
                    .setIsDynamicRole(role.isDynamicRole())
                    .setRoleName(role.roleName())
                    .setRoleNameAbbreviation(role.roleNameAbbreviation())
                    .setCanContainData(role.canContainData())
                    .build();
                nodeRoles.add(nodeRoleBuilder.build());
            });
            ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node.Builder nodeBuilder =
                ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node.newBuilder();
            nodeBuilder.setNodeName(node.getName())
                .setNodeId(node.getId())
                .setEphemeralId(node.getEphemeralId())
                .setHostName(node.getHostName())
                .setHostAddress(node.getHostAddress())
                .setTransportAddress(node.getAddress().toString())
                .putAllAttributes(node.getAttributes())
                .addAllRoles(nodeRoles)
                .setVersion(node.getVersion().toString())
                .build();
            convertedNodes.add(nodeBuilder.build());
        }
        return convertedNodes;
    }

    @Override
    public String toString() {
        return "ProtobufClusterStateResponse{" + "clusterState=" + this.clusterStateRes.getClusterState() + '}';
    }

    public ClusterStateResponse response() {
        return this.clusterStateRes;
    }

    public ProtobufClusterStateResponse(byte[] data) throws IOException {
        this.clusterStateRes = ClusterStateResponseProto.ClusterStateResponse.parseFrom(data);
    }

    public ProtobufClusterStateResponse(ClusterStateResponseProto.ClusterStateResponse clusterStateRes) {
        this.clusterStateRes = clusterStateRes;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(this.clusterStateRes.toByteArray());
    }
}

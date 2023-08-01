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

import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.io.stream.TryWriteable;
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
public class ProtobufClusterStateResponse extends ProtobufActionResponse implements TryWriteable {

    private ClusterStateResponseProto.ClusterStateRes clusterStateRes;

    // public ProtobufClusterStateResponse(ClusterName clusterName, ClusterState clusterState, boolean waitForTimedOut) {
    //     ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Builder discoveryNodesBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.newBuilder();
    //     DiscoveryNodes nodes = clusterState.getNodes();
    //     ImmutableOpenMap<String, DiscoveryNode> allNodes = nodes.getNodes();

    //     Map<String, ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> allNodesMap = convertNodes(allNodes);
    //     discoveryNodesBuilder.putAllAllNodes(allNodesMap).setClusterManagerNodeId(nodes.getClusterManagerNodeId()).setLocalNodeId(nodes.getLocalNodeId()).setMinNonClientNodeVersion(nodes.getSmallestNonClientNodeVersion().toString()).setMaxNonClientNodeVersion(nodes.getLargestNonClientNodeVersion().toString()).setMinNodeVersion(nodes.getMinNodeVersion().toString()).setMaxNodeVersion(nodes.getMaxNodeVersion().toString());

    //     ClusterStateResponseProto.ClusterStateRes.ClusterState.Builder clusterStateBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.newBuilder();
    //     clusterStateBuilder.setClusterName(clusterState.getClusterName().value())
    //         .setVersion(clusterState.version())
    //         .setStateUUID(clusterState.stateUUID())
    //         .setNodes(discoveryNodesBuilder.build());
    //     this.clusterStateRes = ClusterStateResponseProto.ClusterStateRes.newBuilder()
    //                                         .setClusterName(clusterName.value())
    //                                         .setClusterState(clusterStateBuilder.build())
    //                                         .setWaitForTimedOut(waitForTimedOut)
    //                                         .build();
    // }

    public ProtobufClusterStateResponse(String clusterName, DiscoveryNodes nodes, long version, String stateUUID, boolean waitForTimedOut) {
        long startTime = System.nanoTime();
        ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Builder discoveryNodesBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.newBuilder();
        // ImmutableOpenMap<String, DiscoveryNode> allNodes = nodes.getNodes();

        long startTime1 = System.nanoTime();
        Map<String, ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> allNodesMap = convertNodes(nodes.getNodes());
        long endTime1 = System.nanoTime();
        System.out.println("Time taken to convert nodes: " + (endTime1 - startTime1) + " ns");
        long startTime2 = System.nanoTime();
        discoveryNodesBuilder.putAllAllNodes(allNodesMap).setClusterManagerNodeId(nodes.getClusterManagerNodeId()).setLocalNodeId(nodes.getLocalNodeId()).setMinNonClientNodeVersion(nodes.getSmallestNonClientNodeVersion().toString()).setMaxNonClientNodeVersion(nodes.getLargestNonClientNodeVersion().toString()).setMinNodeVersion(nodes.getMinNodeVersion().toString()).setMaxNodeVersion(nodes.getMaxNodeVersion().toString());
        long endTime2 = System.nanoTime();
        System.out.println("Time taken to discover nodes: " + (endTime2 - startTime2) + " ns");

        long startTime3 = System.nanoTime();
        ClusterStateResponseProto.ClusterStateRes.ClusterState.Builder clusterStateBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.newBuilder();
        clusterStateBuilder.setClusterName(clusterName)
            .setVersion(version)
            .setStateUUID(stateUUID)
            .setNodes(discoveryNodesBuilder.build());
        long endTime3 = System.nanoTime();
        System.out.println("Time taken to build cluster state: " + (endTime3 - startTime3) + " ns");

        long startTime4 = System.nanoTime();
        this.clusterStateRes = ClusterStateResponseProto.ClusterStateRes.newBuilder()
                                            .setClusterName(clusterName)
                                            .setClusterState(clusterStateBuilder.build())
                                            .setWaitForTimedOut(waitForTimedOut)
                                            .build();
        long endTime4 = System.nanoTime();
        System.out.println("Time taken to build protobuf response: " + (endTime4 - startTime4) + " ns");
        long endTime = System.nanoTime();
        System.out.println("Time taken to build protobuf response: " + (endTime - startTime) + " ns");
    }

    private Map<String, ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> convertNodes(ImmutableOpenMap<String, DiscoveryNode> nodes) {
        Map<String, ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> convertedNodes = new HashMap<>();
        if (nodes.isEmpty()) {
            return convertedNodes;
        }
        long startTime = System.nanoTime();
        Iterator<String> keysIt = nodes.keysIt();
        while(keysIt.hasNext()) {
            String key = keysIt.next();
            DiscoveryNode node = nodes.get(key);
            List<ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.NodeRole> nodeRoles = new ArrayList<>();
            node.getRoles().forEach(role -> {
                ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.NodeRole.Builder nodeRoleBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.NodeRole.newBuilder();
                nodeRoleBuilder.setIsKnownRole(role.isKnownRole()).setIsDynamicRole(role.isDynamicRole()).setRoleName(role.roleName()).setRoleNameAbbreviation(role.roleNameAbbreviation()).setCanContainData(role.canContainData()).build();
                nodeRoles.add(nodeRoleBuilder.build());
            });
            ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.Builder nodeBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.newBuilder();
            nodeBuilder.setNodeName(node.getName()).setNodeId(node.getId()).setEphemeralId(node.getEphemeralId()).setHostName(node.getHostName())
            .setHostAddress(node.getHostAddress()).setTransportAddress(node.getAddress().toString()).putAllAttributes(node.getAttributes()).addAllRoles(nodeRoles).setVersion(node.getVersion().toString()).build();
            convertedNodes.put(key, nodeBuilder.build());
        }
        long endTime = System.nanoTime();
        System.out.println("Time taken to convert nodes in method: " + (endTime - startTime) + " ns");
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

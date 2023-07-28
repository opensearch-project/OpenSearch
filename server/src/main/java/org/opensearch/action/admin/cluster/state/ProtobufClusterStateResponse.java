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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import org.opensearch.Version;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.TryWriteable;
import org.opensearch.server.proto.ClusterStateRequestProto;
import org.opensearch.server.proto.ClusterStateResponseProto;
import org.opensearch.server.proto.ClusterStateResponseProto.ClusterStateRes;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * The response for getting the cluster state.
*
* @opensearch.internal
*/
public class ProtobufClusterStateResponse extends ProtobufActionResponse implements TryWriteable {

    private ClusterName clusterName;
    private ClusterState clusterState;
    private boolean waitForTimedOut = false;
    private ClusterStateResponseProto.ClusterStateRes clusterStateRes;

    public ProtobufClusterStateResponse(CodedInputStream in) throws IOException {
        super(in);
        System.out.println("Inside ProtobufClusterStateResponse constructor");
        System.out.println("CodedInputStream in: " + in.readTag());
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        String cluster_name = in.readString();
        System.out.println("cluster_name: " + cluster_name);
        clusterName = new ClusterName(cluster_name);
        clusterState = protobufStreamInput.readOptionalWriteable(innerIn -> ClusterState.readFrom(innerIn, null));
        waitForTimedOut = in.readBool();
    }

    public ProtobufClusterStateResponse(ClusterName clusterName, ClusterState clusterState, boolean waitForTimedOut) {
        System.out.println("Inside ProtobufClusterStateResponse constructor");
        this.clusterName = clusterName;
        this.clusterState = clusterState;
        this.waitForTimedOut = waitForTimedOut;
        //convert clusterState to ClusterStateResponseProto.ClusterStateRes.ClusterState
        
        ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Builder discoveryNodesBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.newBuilder();
        DiscoveryNodes nodes = clusterState.getNodes();
        ImmutableOpenMap<String, DiscoveryNode> allNodes = nodes.getNodes();

        Map<String, ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> allNodesMap = convertNodes(allNodes);
        System.out.println("All nodes map:" + allNodes);
        discoveryNodesBuilder.putAllAllNodes(allNodesMap).setClusterManagerNodeId(nodes.getClusterManagerNodeId()).setLocalNodeId(nodes.getLocalNodeId()).setMinNonClientNodeVersion(nodes.getSmallestNonClientNodeVersion().toString()).setMaxNonClientNodeVersion(nodes.getLargestNonClientNodeVersion().toString()).setMinNodeVersion(nodes.getMinNodeVersion().toString()).setMaxNodeVersion(nodes.getMaxNodeVersion().toString());

        ClusterStateResponseProto.ClusterStateRes.ClusterState.Builder clusterStateBuilder = ClusterStateResponseProto.ClusterStateRes.ClusterState.newBuilder();
        clusterStateBuilder.setClusterName(clusterState.getClusterName().value())
            .setVersion(clusterState.version())
            .setStateUUID(clusterState.stateUUID())
            .setNodes(discoveryNodesBuilder.build());
        this.clusterStateRes = ClusterStateResponseProto.ClusterStateRes.newBuilder()
                                            .setClusterName(clusterName.value())
                                            .setClusterState(clusterStateBuilder.build())
                                            .setWaitForTimedOut(waitForTimedOut)
                                            .build();
        System.out.println("Proto cluster state response: " + this.clusterStateRes);
    }

    private Map<String, ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> convertNodes(ImmutableOpenMap<String, DiscoveryNode> nodes) {
        Map<String, ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> convertedNodes = new HashMap<>();
        if (nodes.isEmpty()) {
            return convertedNodes;
        }
        System.out.println("In convertNodes");
        System.out.println("Nodes: " + nodes);
        Iterator<String> keysIt = nodes.keysIt();
        while(keysIt.hasNext()) {
            System.out.println("Inside while loop");
            String key = keysIt.next();
            System.out.println("Key: " + key);
            DiscoveryNode node = nodes.get(key);
            Set<ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node.NodeRole> nodeRoles = new HashSet<>();
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
        return convertedNodes;
    }

    /**
     * The requested cluster state.  Only the parts of the cluster state that were
    * requested are included in the returned {@link ClusterState} instance.
    */
    public ClusterState getState() {
        return this.clusterState;
    }

    /**
     * The name of the cluster.
    */
    public ClusterName getClusterName() {
        return this.clusterName;
    }

    /**
     * Returns whether the request timed out waiting for a cluster state with a metadata version equal or
    * higher than the specified metadata.
    */
    public boolean isWaitForTimedOut() {
        return waitForTimedOut;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        // System.out.println("Inside writeTo of ProtobufClusterStateResponse");
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        clusterName.writeTo(out);
        protobufStreamOutput.writeOptionalWriteable(clusterState);
        out.writeBoolNoTag(waitForTimedOut);
    }

    @Override
    public String toString() {
        return "ProtobufClusterStateResponse{" + "clusterState=" + clusterState + '}';
    }

    public ClusterStateRes response() {
        return this.clusterStateRes;
    }

    public ProtobufClusterStateResponse(byte[] data) throws IOException {
        this.clusterStateRes = ClusterStateResponseProto.ClusterStateRes.parseFrom(data);
        // this.clusterName = new ClusterName(this.clusterStateRes.getClusterName());
        // org.opensearch.server.proto.ClusterStateResponseProto.ClusterStateRes.ClusterState clusterState = this.clusterStateRes.getClusterState();
        // org.opensearch.server.proto.ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes discoveryNodesProto = clusterState.getNodes();
        // discoveryNodesProto.getAllNodesMap().forEach((key, value) -> {
        //     System.out.println("Key: " + key);
        //     System.out.println("Value: " + value);
        // });
        // Map<String, ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> nodeMap = discoveryNodesProto.getAllNodesMap();
        // ImmutableOpenMap<String, DiscoveryNode> allNodes = new ImmutableOpenMap.Builder<String, DiscoveryNode>().build();
        // for (Map.Entry<String, ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node> entry : nodeMap.entrySet()) {
        //     String key = entry.getKey();
        //     ClusterStateResponseProto.ClusterStateRes.ClusterState.DiscoveryNodes.Node value = entry.getValue();
        //     Set<DiscoveryNodeRole> roles = new HashSet<>();
        //     value.getRolesList().forEach(role -> {
        //         String roleName = role.getRoleName();
        //         String roleNameAbbreviation = role.getRoleNameAbbreviation();
        //         boolean canContainData = role.getCanContainData();
        //         DiscoveryNodeRole discoveryNodeRole = DiscoveryNode.roleMap.get(roleName);
        //         roles.add(discoveryNodeRole);
        //     });
        //     DiscoveryNode node = new DiscoveryNode();
        //     allNodes = allNodes.copyAndPut(key, node);
        // }
        // allNodes.putAll(nodeMap);
        // DiscoveryNodes discoveryNodes = new DiscoveryNodes(discoveryNodesProto.getClusterManagerNodeId(), discoveryNodesProto.getLocalNodeId(), discoveryNodesProto.getMinNonClientNodeVersion(), discoveryNodesProto.getMaxNonClientNodeVersion(), discoveryNodesProto.getMinNodeVersion(), discoveryNodesProto.getMaxNodeVersion());
        // this.clusterState = new ClusterState(clusterName, clusterState.getVersion(), clusterState.getStateUUID(), null, null, null, null, null, 0, waitForTimedOut);
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(this.clusterStateRes.toByteArray());
    }
}

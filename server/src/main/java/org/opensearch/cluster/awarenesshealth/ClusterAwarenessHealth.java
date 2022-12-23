/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.awarenesshealth;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Cluster state Awareness health information
 *
 */
public class ClusterAwarenessHealth implements Writeable {

    private int numberOfNodes;
    private int numberOfDataNodes;
    private int activeShards;
    private int unassignedShards;
    private int initializingShards;
    private int relocatingShards;
    private int activePrimaryShards;
    private double activeShardsPercent;
    private boolean hasDiscoveredClusterManager;
    private ClusterHealthStatus status;
    private final ClusterAwarenessAttributeHealth clusterAwarenessAttributeHealth;

    /**
     * Creates cluster awareness health from cluster state.
     *
     * @param clusterState The current cluster state. Must not be null.
     * @param clusterSettings the current cluster settings.
     * @param awarenessAttributeName Name of awareness attribute for which we need to see the health
     */
    public ClusterAwarenessHealth(ClusterState clusterState, ClusterSettings clusterSettings, String awarenessAttributeName) {

        setClusterHealth(clusterState);

        clusterAwarenessAttributeHealth = generateAwarenessAttributeHealthMap(clusterState, clusterSettings, awarenessAttributeName);
    }

    private void setClusterHealth(ClusterState clusterState) {
        ClusterStateHealth clusterStateHealth = new ClusterStateHealth(clusterState);
        numberOfDataNodes = clusterStateHealth.getNumberOfDataNodes();
        numberOfNodes = clusterStateHealth.getNumberOfNodes();
        activeShards = clusterStateHealth.getActiveShards();
        unassignedShards = clusterStateHealth.getUnassignedShards();
        initializingShards = clusterStateHealth.getInitializingShards();
        relocatingShards = clusterStateHealth.getRelocatingShards();
        status = clusterStateHealth.getStatus();
        activePrimaryShards = clusterStateHealth.getActivePrimaryShards();
        activeShardsPercent = clusterStateHealth.getActiveShardsPercent();
        hasDiscoveredClusterManager = clusterStateHealth.hasDiscoveredClusterManager();
    }

    private Map<String, NodeShardInfo> generateShardAllocationPerNode(ClusterState clusterState) {

        boolean activeShard;
        boolean relocatingShard;
        boolean initializingShard;
        String nodeId = "";
        String relocatingNodeId = "";
        NodeShardInfo nodeShardInfo;

        Map<String, NodeShardInfo> shardAllocationPerNode = new HashMap<>();

        for (ShardRouting shard : clusterState.routingTable().allShards()) {
            activeShard = false;
            relocatingShard = false;
            initializingShard = false;

            if (shard.assignedToNode()) {
                nodeId = shard.currentNodeId();
                if (shard.active()) {
                    if (shard.started()) {
                        activeShard = true;
                    } else if (shard.relocating()) {
                        relocatingShard = true;
                        relocatingNodeId = shard.relocatingNodeId();
                    }
                } else if (shard.initializing()) {
                    initializingShard = true;
                }

                // This will handle the scenario when shard is in relocating phase
                if (relocatingShard) {
                    if (shardAllocationPerNode.containsKey(relocatingNodeId)) {
                        nodeShardInfo = shardAllocationPerNode.get(relocatingNodeId);
                        nodeShardInfo.setInitializingShards(nodeShardInfo.getInitializingShards() + 1);
                    } else {
                        nodeShardInfo = new NodeShardInfo(relocatingNodeId, 0, 0, 1);
                    }
                    shardAllocationPerNode.put(relocatingNodeId, nodeShardInfo);

                    if (shardAllocationPerNode.containsKey(nodeId)) {
                        nodeShardInfo = shardAllocationPerNode.get(nodeId);
                        nodeShardInfo.setRelocatingShards(nodeShardInfo.getRelocatingShards() + 1);
                    } else {
                        nodeShardInfo = new NodeShardInfo(relocatingNodeId, 0, 1, 0);
                    }

                } else { // This will handle the non-relocating shard scenario
                    if (shardAllocationPerNode.containsKey(nodeId)) {
                        nodeShardInfo = shardAllocationPerNode.get(nodeId);
                        if (activeShard) {
                            nodeShardInfo.setActiveShards(nodeShardInfo.getActiveShards() + 1);
                        } else if (initializingShard) {
                            nodeShardInfo.setInitializingShards(nodeShardInfo.getInitializingShards() + 1);
                        }
                    } else {
                        nodeShardInfo = new NodeShardInfo(relocatingNodeId, 0, 0, 0);
                        if (activeShard) {
                            nodeShardInfo.setActiveShards(1);
                        } else if (initializingShard) {
                            nodeShardInfo.setInitializingShards(1);
                        }
                    }

                }
                shardAllocationPerNode.put(nodeId, nodeShardInfo);
            }
        }

        return shardAllocationPerNode;
    }

    private ClusterAwarenessAttributeHealth generateAwarenessAttributeHealthMap(
        ClusterState clusterState,
        ClusterSettings clusterSettings,
        String awarenessAttributeName
    ) {

        int totalShards = this.activeShards + this.initializingShards + this.unassignedShards;

        // Constructing shard allocation per node for awareness level shard distribution
        Map<String, NodeShardInfo> shardAllocationPerNode = generateShardAllocationPerNode(clusterState);

        // Get Weights if those are set
        WeightedRoutingMetadata weightedRoutingMetadata = clusterState.getMetadata().weightedRoutingMetadata();
        Map<String, Double> weightedRoutingWeights = null;
        if (weightedRoutingMetadata != null) {
            WeightedRouting weightedRouting = weightedRoutingMetadata.getWeightedRouting();
            weightedRoutingWeights = weightedRouting.weights();
        }

        // This property will govern if we need to show unassigned shard info or not
        boolean displayUnassignedShardLevelInfo = canCalcUnassignedShards(clusterSettings, awarenessAttributeName);

        ImmutableOpenMap<String, DiscoveryNode> dataNodesMap = clusterState.nodes().getDataNodes();

        return new ClusterAwarenessAttributeHealth(
            awarenessAttributeName,
            weightedRoutingWeights,
            displayUnassignedShardLevelInfo,
            shardAllocationPerNode,
            dataNodesMap,
            totalShards
        );
    }

    private boolean canCalcUnassignedShards(ClusterSettings clusterSettings, String awarenessAttributeName) {

        // Getting the replicaEnforcement settings as both are necessary for replica enforcement.
        boolean allocationAwarenessBalance = clusterSettings.get(
            AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING
        );
        Settings forcedAwarenessSettings = clusterSettings.get(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING
        );

        boolean forcedZoneSettingsExists = false;

        if (!forcedAwarenessSettings.isEmpty()) {
            // We will only mark true if particular awareness attribute exists
            if (forcedAwarenessSettings.hasValue(awarenessAttributeName + ".values")) {
                forcedZoneSettingsExists = true;
            }
        }

        return allocationAwarenessBalance && forcedZoneSettingsExists;
    }

    public ClusterAwarenessHealth(final StreamInput in) throws IOException {
        numberOfNodes = in.readVInt();
        numberOfDataNodes = in.readVInt();
        activeShards = in.readVInt();
        initializingShards = in.readVInt();
        relocatingShards = in.readVInt();
        unassignedShards = in.readVInt();
        activePrimaryShards = in.readVInt();
        activeShardsPercent = in.readDouble();
        hasDiscoveredClusterManager = in.readBoolean();
        status = ClusterHealthStatus.fromValue(in.readByte());
        clusterAwarenessAttributeHealth = new ClusterAwarenessAttributeHealth(in);
    }

    public ClusterAwarenessHealth(
        int activeShards,
        int unassignedShards,
        int initializingShards,
        int relocatingShards,
        int numberOfDataNodes,
        int numberOfNodes,
        int activePrimaryShards,
        double activeShardsPercent,
        boolean hasDiscoveredClusterManager,
        String awarenessAttribute,
        ClusterHealthStatus clusterHealthStatus
    ) {
        this.activeShards = activeShards;
        this.unassignedShards = unassignedShards;
        this.initializingShards = initializingShards;
        this.relocatingShards = relocatingShards;
        this.numberOfDataNodes = numberOfDataNodes;
        this.numberOfNodes = numberOfNodes;
        this.status = clusterHealthStatus;
        this.activePrimaryShards = activePrimaryShards;
        this.activeShardsPercent = activeShardsPercent;
        this.hasDiscoveredClusterManager = hasDiscoveredClusterManager;
        this.clusterAwarenessAttributeHealth = new ClusterAwarenessAttributeHealth(awarenessAttribute, Collections.emptyMap());
    }

    public int getNumberOfNodes() {
        return this.numberOfNodes;
    }

    public int getNumberOfDataNodes() {
        return this.numberOfDataNodes;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public int getActivePrimaryShards() {
        return activePrimaryShards;
    }

    public double getActiveShardsPercent() {
        return activeShardsPercent;
    }

    public boolean isHasDiscoveredClusterManager() {
        return hasDiscoveredClusterManager;
    }

    public ClusterAwarenessAttributeHealth getAwarenessAttributeHealth() {
        return this.clusterAwarenessAttributeHealth;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(numberOfNodes);
        out.writeVInt(numberOfDataNodes);
        out.writeVInt(activeShards);
        out.writeVInt(initializingShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(unassignedShards);
        out.writeVInt(activePrimaryShards);
        out.writeDouble(activeShardsPercent);
        out.writeBoolean(hasDiscoveredClusterManager);
        out.writeByte(status.value());
        clusterAwarenessAttributeHealth.writeTo(out);
    }

    @Override
    public String toString() {
        return "ClusterStateHealth{"
            + "numberOfNodes="
            + numberOfNodes
            + ", numberOfDataNodes="
            + numberOfDataNodes
            + ", activeShards="
            + activeShards
            + ", initializingShards="
            + initializingShards
            + ", unassignedShards="
            + unassignedShards
            + ", activePrimaryShards="
            + activePrimaryShards
            + ", activeShardsPercent="
            + activeShardsPercent
            + ", hasDiscoveredClusterManager="
            + hasDiscoveredClusterManager
            + ", status="
            + status
            + ", clusterAwarenessAttributeHealth.awarenessAttributeName"
            + (clusterAwarenessAttributeHealth == null ? "null" : clusterAwarenessAttributeHealth.getAwarenessAttributeName())
            + ", clusterAwarenessAttributeHealth.size="
            + (clusterAwarenessAttributeHealth == null ? "null" : clusterAwarenessAttributeHealth.getAwarenessAttributeHealthMap().size())
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterAwarenessHealth that = (ClusterAwarenessHealth) o;
        return numberOfNodes == that.numberOfNodes
            && numberOfDataNodes == that.numberOfDataNodes
            && activeShards == that.activeShards
            && initializingShards == that.initializingShards
            && unassignedShards == that.unassignedShards
            && activePrimaryShards == that.activePrimaryShards
            && activeShardsPercent == that.activeShardsPercent
            && hasDiscoveredClusterManager == that.hasDiscoveredClusterManager
            && status == that.status
            && Objects.equals(clusterAwarenessAttributeHealth, that.clusterAwarenessAttributeHealth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            numberOfNodes,
            numberOfDataNodes,
            activeShards,
            initializingShards,
            unassignedShards,
            activePrimaryShards,
            activeShardsPercent,
            hasDiscoveredClusterManager,
            status,
            clusterAwarenessAttributeHealth
        );
    }
}

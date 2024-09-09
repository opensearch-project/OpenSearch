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

package org.opensearch.cluster.health;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Cluster state health information
 *
 * @opensearch.internal
 */
public final class ClusterStateHealth implements Iterable<ClusterIndexHealth>, Writeable {

    private final int numberOfNodes;
    private final int numberOfDataNodes;
    private final boolean hasDiscoveredClusterManager;
    private final int activeShards;
    private final int relocatingShards;
    private final int activePrimaryShards;
    private final int initializingShards;
    private final int unassignedShards;
    private int delayedUnassignedShards;
    private final double activeShardsPercent;
    private final ClusterHealthStatus status;
    private final Map<String, ClusterIndexHealth> indices;

    /**
     * Creates a new <code>ClusterStateHealth</code> instance considering the current cluster state and all indices in the cluster.
     *
     * @param clusterState The current cluster state. Must not be null.
     */
    public ClusterStateHealth(final ClusterState clusterState) {
        this(clusterState, clusterState.metadata().getConcreteAllIndices());
    }

    public ClusterStateHealth(final ClusterState clusterState, final ClusterHealthRequest.Level clusterHealthLevel) {
        this(clusterState, clusterState.metadata().getConcreteAllIndices(), clusterHealthLevel);
    }

    /**
     * Creates a new <code>ClusterStateHealth</code> instance considering the current cluster state and the provided index names.
     *
     * @param clusterState    The current cluster state. Must not be null.
     * @param concreteIndices An array of index names to consider. Must not be null but may be empty.
     */
    public ClusterStateHealth(final ClusterState clusterState, final String[] concreteIndices) {
        numberOfNodes = clusterState.nodes().getSize();
        numberOfDataNodes = clusterState.nodes().getDataNodes().size();
        hasDiscoveredClusterManager = clusterState.nodes().getClusterManagerNodeId() != null;
        indices = new HashMap<>();
        for (String index : concreteIndices) {
            IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(index);
            IndexMetadata indexMetadata = clusterState.metadata().index(index);
            if (indexRoutingTable == null) {
                continue;
            }

            ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetadata, indexRoutingTable);

            indices.put(indexHealth.getIndex(), indexHealth);
        }

        ClusterHealthStatus computeStatus = ClusterHealthStatus.GREEN;
        int computeActivePrimaryShards = 0;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;
        int computeDelayedUnassignedShards = 0;

        for (ClusterIndexHealth indexHealth : indices.values()) {
            computeActivePrimaryShards += indexHealth.getActivePrimaryShards();
            computeActiveShards += indexHealth.getActiveShards();
            computeRelocatingShards += indexHealth.getRelocatingShards();
            computeInitializingShards += indexHealth.getInitializingShards();
            computeUnassignedShards += indexHealth.getUnassignedShards();
            computeDelayedUnassignedShards += indexHealth.getDelayedUnassignedShards();
            computeStatus = getClusterHealthStatus(indexHealth, computeStatus);
        }

        if (clusterState.blocks().hasGlobalBlockWithStatus(RestStatus.SERVICE_UNAVAILABLE)) {
            computeStatus = ClusterHealthStatus.RED;
        }

        this.status = computeStatus;
        this.activePrimaryShards = computeActivePrimaryShards;
        this.activeShards = computeActiveShards;
        this.relocatingShards = computeRelocatingShards;
        this.initializingShards = computeInitializingShards;
        this.unassignedShards = computeUnassignedShards;
        this.delayedUnassignedShards = computeDelayedUnassignedShards;

        // shortcut on green
        if (ClusterHealthStatus.GREEN.equals(computeStatus)) {
            this.activeShardsPercent = 100;
        } else {
            List<ShardRouting> shardRoutings = clusterState.getRoutingTable().allShards();
            int activeShardCount = 0;
            int totalShardCount = 0;
            for (ShardRouting shardRouting : shardRoutings) {
                if (shardRouting.active()) activeShardCount++;
                totalShardCount++;
            }
            this.activeShardsPercent = (((double) activeShardCount) / totalShardCount) * 100;
        }
    }

    public ClusterStateHealth(
        final ClusterState clusterState,
        final String[] concreteIndices,
        final ClusterHealthRequest.Level healthLevel
    ) {
        numberOfNodes = clusterState.nodes().getSize();
        numberOfDataNodes = clusterState.nodes().getDataNodes().size();
        hasDiscoveredClusterManager = clusterState.nodes().getClusterManagerNodeId() != null;
        indices = new HashMap<>();
        boolean isIndexOrShardLevelHealthRequired = healthLevel == ClusterHealthRequest.Level.INDICES
            || healthLevel == ClusterHealthRequest.Level.SHARDS;

        ClusterHealthStatus computeStatus = ClusterHealthStatus.GREEN;
        int computeActivePrimaryShards = 0;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;
        int computeDelayedUnassignedShards = 0;

        for (String index : concreteIndices) {
            IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(index);
            IndexMetadata indexMetadata = clusterState.metadata().index(index);
            if (indexRoutingTable == null) {
                continue;
            }

            ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetadata, indexRoutingTable, healthLevel);
            computeActivePrimaryShards += indexHealth.getActivePrimaryShards();
            computeActiveShards += indexHealth.getActiveShards();
            computeRelocatingShards += indexHealth.getRelocatingShards();
            computeInitializingShards += indexHealth.getInitializingShards();
            computeUnassignedShards += indexHealth.getUnassignedShards();
            computeDelayedUnassignedShards += indexHealth.getDelayedUnassignedShards();
            computeStatus = getClusterHealthStatus(indexHealth, computeStatus);

            if (isIndexOrShardLevelHealthRequired) {
                // Store ClusterIndexHealth only when the health is requested at Index or Shard level
                indices.put(indexHealth.getIndex(), indexHealth);
            }
        }

        if (clusterState.blocks().hasGlobalBlockWithStatus(RestStatus.SERVICE_UNAVAILABLE)) {
            computeStatus = ClusterHealthStatus.RED;
        }

        this.status = computeStatus;
        this.activePrimaryShards = computeActivePrimaryShards;
        this.activeShards = computeActiveShards;
        this.relocatingShards = computeRelocatingShards;
        this.initializingShards = computeInitializingShards;
        this.unassignedShards = computeUnassignedShards;
        this.delayedUnassignedShards = computeDelayedUnassignedShards;

        // shortcut on green
        if (ClusterHealthStatus.GREEN.equals(computeStatus)) {
            this.activeShardsPercent = 100;
        } else {
            List<ShardRouting> shardRoutings = clusterState.getRoutingTable().allShards();
            int activeShardCount = 0;
            int totalShardCount = 0;
            for (ShardRouting shardRouting : shardRoutings) {
                if (shardRouting.active()) activeShardCount++;
                totalShardCount++;
            }
            this.activeShardsPercent = (((double) activeShardCount) / totalShardCount) * 100;
        }
    }

    private static ClusterHealthStatus getClusterHealthStatus(ClusterIndexHealth indexHealth, ClusterHealthStatus computeStatus) {
        switch (indexHealth.getStatus()) {
            case RED:
                return ClusterHealthStatus.RED;
            case YELLOW:
                // do not override an existing red
                if (computeStatus != ClusterHealthStatus.RED) {
                    return ClusterHealthStatus.YELLOW;
                } else {
                    return ClusterHealthStatus.RED;
                }
            default:
                return computeStatus;
        }
    }

    public ClusterStateHealth(final StreamInput in) throws IOException {
        activePrimaryShards = in.readVInt();
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        numberOfNodes = in.readVInt();
        numberOfDataNodes = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_1_0_0)) {
            hasDiscoveredClusterManager = in.readBoolean();
        } else {
            hasDiscoveredClusterManager = true;
        }
        status = ClusterHealthStatus.fromValue(in.readByte());
        int size = in.readVInt();
        indices = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            ClusterIndexHealth indexHealth = new ClusterIndexHealth(in);
            indices.put(indexHealth.getIndex(), indexHealth);
        }
        activeShardsPercent = in.readDouble();
    }

    /**
     * For ClusterHealthResponse's XContent Parser
     */
    public ClusterStateHealth(
        int activePrimaryShards,
        int activeShards,
        int relocatingShards,
        int initializingShards,
        int unassignedShards,
        int numberOfNodes,
        int numberOfDataNodes,
        boolean hasDiscoveredClusterManager,
        double activeShardsPercent,
        ClusterHealthStatus status,
        Map<String, ClusterIndexHealth> indices
    ) {
        this.activePrimaryShards = activePrimaryShards;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.unassignedShards = unassignedShards;
        this.numberOfNodes = numberOfNodes;
        this.numberOfDataNodes = numberOfDataNodes;
        this.hasDiscoveredClusterManager = hasDiscoveredClusterManager;
        this.activeShardsPercent = activeShardsPercent;
        this.status = status;
        this.indices = indices;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public int getActivePrimaryShards() {
        return activePrimaryShards;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    public int getDelayedUnassignedShards() {
        return delayedUnassignedShards;
    }

    public int getNumberOfNodes() {
        return this.numberOfNodes;
    }

    public int getNumberOfDataNodes() {
        return this.numberOfDataNodes;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public Map<String, ClusterIndexHealth> getIndices() {
        return Collections.unmodifiableMap(indices);
    }

    public double getActiveShardsPercent() {
        return activeShardsPercent;
    }

    public boolean hasDiscoveredClusterManager() {
        return hasDiscoveredClusterManager;
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #hasDiscoveredClusterManager()} */
    @Deprecated
    public boolean hasDiscoveredMaster() {
        return hasDiscoveredClusterManager();
    }

    @Override
    public Iterator<ClusterIndexHealth> iterator() {
        return indices.values().iterator();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(activePrimaryShards);
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeVInt(numberOfNodes);
        out.writeVInt(numberOfDataNodes);
        if (out.getVersion().onOrAfter(Version.V_1_0_0)) {
            out.writeBoolean(hasDiscoveredClusterManager);
        }
        out.writeByte(status.value());
        out.writeVInt(indices.size());
        for (ClusterIndexHealth indexHealth : this) {
            indexHealth.writeTo(out);
        }
        out.writeDouble(activeShardsPercent);
    }

    @Override
    public String toString() {
        return "ClusterStateHealth{"
            + "numberOfNodes="
            + numberOfNodes
            + ", numberOfDataNodes="
            + numberOfDataNodes
            + ", hasDiscoveredClusterManager="
            + hasDiscoveredClusterManager
            + ", activeShards="
            + activeShards
            + ", relocatingShards="
            + relocatingShards
            + ", activePrimaryShards="
            + activePrimaryShards
            + ", initializingShards="
            + initializingShards
            + ", unassignedShards="
            + unassignedShards
            + ", activeShardsPercent="
            + activeShardsPercent
            + ", status="
            + status
            + ", indices.size="
            + (indices == null ? "null" : indices.size())
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateHealth that = (ClusterStateHealth) o;
        return numberOfNodes == that.numberOfNodes
            && numberOfDataNodes == that.numberOfDataNodes
            && hasDiscoveredClusterManager == that.hasDiscoveredClusterManager
            && activeShards == that.activeShards
            && relocatingShards == that.relocatingShards
            && activePrimaryShards == that.activePrimaryShards
            && initializingShards == that.initializingShards
            && unassignedShards == that.unassignedShards
            && Double.compare(that.activeShardsPercent, activeShardsPercent) == 0
            && status == that.status
            && Objects.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            numberOfNodes,
            numberOfDataNodes,
            hasDiscoveredClusterManager,
            activeShards,
            relocatingShards,
            activePrimaryShards,
            initializingShards,
            unassignedShards,
            activeShardsPercent,
            status,
            indices
        );
    }
}

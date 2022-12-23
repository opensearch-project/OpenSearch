/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.awarenesshealth;

/**
 * Stores shard level information for a node
 *
 */
public class NodeShardInfo {

    private String nodeID;
    private int activeShards;
    private int relocatingShards;
    private int initializingShards;

    /**
     * Stores shard level information for a node
     *
     * @param nodeID node id in the cluster
     * @param activeShards active shards present on the node
     * @param relocatingShards relocating shards present on the node
     * @param initializingShards initialising shards present on the node
     */
    public NodeShardInfo(String nodeID, int activeShards, int relocatingShards, int initializingShards) {
        this.nodeID = nodeID;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
    }

    public String getNodeID() {
        return nodeID;
    }

    public void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public void setActiveShards(int activeShards) {
        this.activeShards = activeShards;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public void setRelocatingShards(int relocatingShards) {
        this.relocatingShards = relocatingShards;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public void setInitializingShards(int initializingShards) {
        this.initializingShards = initializingShards;
    }
}

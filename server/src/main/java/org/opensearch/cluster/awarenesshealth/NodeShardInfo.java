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

    private final String nodeID;
    private int activeShards;
    private int relocatingShards;
    private int initializingShards;

    /**
     * Stores shard level information for a node
     *
     * @param nodeID node id in the cluster
     */
    public NodeShardInfo(String nodeID) {
        this.nodeID = nodeID;
    }

    public String getNodeID() {
        return nodeID;
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

    public void incrementActiveShards() {
        activeShards += 1;
    }

    public void incrementInitializingShards() {
        initializingShards += 1;
    }

    public void incrementRelocatingShards() {
        relocatingShards += 1;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.awarenesshealth;

public class NodeShardInfo {

    private String nodeID;
    private int activeShards;
    private int relocatingShards;
    private int initialisingShards;

    public NodeShardInfo(String nodeID, int activeShards, int relocatingShards, int initialisingShards) {
        this.nodeID = nodeID;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initialisingShards = initialisingShards;
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

    public int getInitialisingShards() {
        return initialisingShards;
    }

    public void setInitialisingShards(int initialisingShards) {
        this.initialisingShards = initialisingShards;
    }
}

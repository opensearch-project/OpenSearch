/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.indices.replication.common.ReplicationType;

import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;

public class SegmentReplicationAllocationIT extends SegmentReplicationIT {

    /**
     * This test verifies shard allocation when segment replication is enabled. The test verifies that primary shard
     * distribution is even.
     */
    public void testShardAllocation() {
        // Start 3 node cluster
        internalCluster().startNodes(3, featureFlagSettings());
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(BalancedShardsAllocator.PRIMARY_BALANCE_FACTOR_SETTING.getKey(), 0.40f).build())
            .get();
        int numberOfIndices = 10;
        ShardAllocations shardAllocations = new ShardAllocations();
        ClusterState state;
        for (int i = 0; i < numberOfIndices; i++) {
            int shardCount = 5;// randomIntBetween(1, 5);
            int replicaCount = 1;// randomIntBetween(0, 2);
            createIndex("test" + i, shardCount, replicaCount, i % 2 == 0);
            ensureGreen();
            state = client().admin().cluster().prepareState().execute().actionGet().getState();
            shardAllocations.setState(state);
            if (logger.isTraceEnabled()) {
                logger.info("{}", shardAllocations.toString());
            }
        }
        // Wait for all shards to be STARTED and evaluate final allocation.
        ensureGreen();
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        shardAllocations.setState(state);
        logger.info("{}", shardAllocations.toString());
    }

    /**
     * ===================================================
     * NODE_T0              TOTAL                DOCREP               SEGREP
     * P                    20                   10                   10
     * R                    13                   6                    7
     * NODE_T1              TOTAL                DOCREP               SEGREP
     * P                    10                   5                    5
     * R                    24                   12                   12
     * NODE_T2              TOTAL                DOCREP               SEGREP
     * P                    20                   10                   10
     * R                    13                   7                    6
     *
     * Unassigned           0                    0
     *
     * Total Shards         50                   50
     */

    class ShardAllocations {
        ClusterState state;

        public static final String separator = "===================================================";
        public static final String ONE_LINE_RETURN = "\n";
        public static final String TWO_LINE_RETURN = "\n\n";

        /**
         Use treemap so that each iteration shows same ordering of nodes.
         String: NodeId
         int[]: tuple storing primary shard count in 0 index and replica's in 1 for segrep enabled indices
         */
        TreeMap<String, int[]> nodeToSegRepCountMap = new TreeMap<>();

        TreeMap<String, int[]> nodeToDocRepCountMap = new TreeMap<>();

        /**
         * Helper map containing NodeName to Node Id
         */
        TreeMap<String, String> nameToNodeId = new TreeMap<>();

        /*
        Unassigned array containing primary at 0, replica at 1
         */
        int[] unassigned;

        int[] totalShards;

        public final String printShardAllocationWithHeader(String nodeName, int[] docrep, int[] segrep) {
            StringBuffer sb = new StringBuffer();
            Formatter formatter = new Formatter(sb);
            // formatter.format("%-10s\n", nodeName);
            formatter.format("%-20s %-20s %-20s %-20s\n", "P", docrep[0] + segrep[0], docrep[0], segrep[0]);
            formatter.format("%-20s %-20s %-20s %-20s\n", "R", docrep[1] + segrep[1], docrep[1], segrep[1]);
            // formatter.format("%-19s %-20s %-20s\n", "", "---", "---");
            // formatter.format("%-20s %-20s %-20s\n", "", docrep[0] + docrep[1], segrep[1] + segrep[0]);
            return sb.toString();
        }

        public void reset() {
            nodeToSegRepCountMap.clear();
            nodeToDocRepCountMap.clear();
            nameToNodeId = new TreeMap<>();
            totalShards = new int[] { 0, 0 };
            unassigned = new int[] { 0, 0 };
        }

        public void setState(ClusterState state) {
            this.reset();
            this.state = state;
            buildMap();
        }

        private void buildMap() {
            for (RoutingNode node : state.getRoutingNodes()) {
                nameToNodeId.putIfAbsent(node.node().getName(), node.nodeId());
                nodeToSegRepCountMap.putIfAbsent(node.nodeId(), new int[] { 0, 0 });
                nodeToDocRepCountMap.putIfAbsent(node.nodeId(), new int[] { 0, 0 });
            }
            for (ShardRouting shardRouting : state.routingTable().allShards()) {
                // Fetch shard to update. Initialize local array
                if (isIndexSegRep(shardRouting.getIndexName())) {
                    updateMap(nodeToSegRepCountMap, shardRouting);
                } else {
                    updateMap(nodeToDocRepCountMap, shardRouting);
                }
            }
        }

        void updateMap(TreeMap<String, int[]> mapToUpdate, ShardRouting shardRouting) {
            int[] shard;
            shard = shardRouting.assignedToNode() ? mapToUpdate.get(shardRouting.currentNodeId()) : unassigned;
            // Update shard type count
            if (shardRouting.primary()) {
                shard[0]++;
                totalShards[0]++;
            } else {
                shard[1]++;
                totalShards[1]++;
            }
            // For assigned shards, put back counter
            if (shardRouting.assignedToNode()) mapToUpdate.put(shardRouting.currentNodeId(), shard);
        }

        boolean isIndexSegRep(String indexName) {
            return state.metadata()
                .index(indexName)
                .getSettings()
                .get(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey())
                .equals(ReplicationType.SEGMENT.toString());
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(TWO_LINE_RETURN + separator + ONE_LINE_RETURN);
            Formatter formatter = new Formatter(sb);
            // formatter.format("%-20s %-20s %-20s\n", "", "DOCREP", "SEGREP");
            for (Map.Entry<String, String> entry : nameToNodeId.entrySet()) {
                String nodeId = nameToNodeId.get(entry.getKey());
                formatter.format("%-20s %-20s %-20s %-20s\n", entry.getKey().toUpperCase(), "TOTAL", "DOCREP", "SEGREP");
                sb.append(
                    printShardAllocationWithHeader(
                        entry.getKey().toUpperCase(),
                        nodeToDocRepCountMap.get(nodeId),
                        nodeToSegRepCountMap.get(nodeId)
                    )
                );
            }
            sb.append(ONE_LINE_RETURN);
            formatter.format("%-20s %-20s %-20s\n\n", "Unassigned ", unassigned[0], unassigned[1]);
            formatter.format("%-20s %-20s %-20s\n\n", "Total Shards", totalShards[0], totalShards[1]);
            return sb.toString();
        }
    }

}

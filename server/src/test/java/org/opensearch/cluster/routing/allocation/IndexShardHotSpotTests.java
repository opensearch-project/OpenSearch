/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.OpenSearchAllocationWithConstraintsTestCase;

public class IndexShardHotSpotTests extends OpenSearchAllocationWithConstraintsTestCase {

    /**
     * Test single node replacement without active indexing.
     */
    public void testNodeReplacement() {
        setupInitialCluster(5, 1, 5, 1);
        terminateNodes("node_1");
        assertForIndexShardHotSpots(false, 4);
        addNodesWithoutIndexing(1, "new_node_");
        allocateAndCheckIndexShardHotSpots(false, 5, "new_node_0");
    }

    /**
     * Test single node replacement with active indexing.
     */
    public void testNodeReplacementWithIndexing() {
        setupInitialCluster(5, 30, 5, 1);
        buildAllocationService();
        terminateNodes("node_1");
        assertForIndexShardHotSpots(false, 4);
        addNodesWithIndexing(1, "new_node_", 3, 20, 1);
        allocateAndCheckIndexShardHotSpots(false, 5, "new_node_0");
    }

    /**
     * Test skewed cluster scale out via single node addition during active indexing.
     */
    public void testSkewedClusterScaleOut() {
        setupInitialCluster(3, 30, 10, 1);
        buildAllocationService();
        addNodesWithIndexing(1, "new_node_", 8, 10, 1);
        allocateAndCheckIndexShardHotSpots(false, 4, "new_node_0");
    }

    /**
     * Test under replicated yellow cluster scale out to green.
     */
    public void testUnderReplicatedClusterScaleOut() {
        setupInitialCluster(3, 30, 10, 3);
        buildAllocationService();
        addNodesWithoutIndexing(1, "new_node_");
        allocateAndCheckIndexShardHotSpots(false, 4, "new_node_0");
    }

    /**
     * Test cluster scale in scenario, when nodes are gracefully excluded from
     * cluster before termination.
     * <p>
     * During moveShards(), shards are picked from across indexes in an interleaved manner.
     * This prevents hot spots by evenly picking up shards. Since shard order can change
     * in subsequent runs.
     */
    public void testClusterScaleIn() {
        setupInitialCluster(4, 30, 10, 1);
        buildAllocationService("node_0,node_1");
        allocateAndCheckIndexShardHotSpots(false, 2, "node_2", "node_3");
    }

    /**
     * Test cluster scale in scenario with skewed shard distribution in remaining nodes.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/2063")
    public void testClusterScaleInWithSkew() {
        setupInitialCluster(4, 100, 5, 1);
        buildAllocationService("node_0,node_1");
        addNodesWithoutIndexing(1, "new_node_");
        allocateAndCheckIndexShardHotSpots(false, 3, "node_2", "node_3", "new_node_0");
    }
}

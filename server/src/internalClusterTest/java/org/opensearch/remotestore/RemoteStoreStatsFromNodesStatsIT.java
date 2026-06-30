/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteSegmentStats;
import org.opensearch.index.translog.RemoteTranslogStats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreStatsFromNodesStatsIT extends RemoteStoreBaseIntegTestCase {
    private static final String INDEX_NAME = "remote-index-1";
    private static final int DATA_NODE_COUNT = 2;
    private static final int CLUSTER_MANAGER_NODE_COUNT = 3;

    @Before
    public void setup() {
        setupCustomCluster();
    }

    private void setupCustomCluster() {
        internalCluster().startClusterManagerOnlyNodes(CLUSTER_MANAGER_NODE_COUNT);
        internalCluster().startDataOnlyNodes(DATA_NODE_COUNT);
        ensureStableCluster(DATA_NODE_COUNT + CLUSTER_MANAGER_NODE_COUNT);
    }

    /**
     * - Creates two indices with single primary shard, pinned to a single node.
     * - Index documents in both of them and forces a fresh for both
     * - Polls the _remotestore/stats API for individual index level stats
     * - Adds up requisite fields from the API output, repeats this for the 2nd index
     * - Polls _nodes/stats and verifies that the total values at node level adds up
     * to the values capture in the previous step
     */
    public void testNodesStatsParityWithOnlyPrimaryShards() {
        String[] dataNodes = internalCluster().getDataNodeNames().toArray(String[]::new);
        String randomDataNode = dataNodes[randomIntBetween(0, dataNodes.length - 1)];
        String firstIndex = INDEX_NAME + "1";
        String secondIndex = INDEX_NAME + "2";

        // Create first index
        createIndex(
            firstIndex,
            Settings.builder().put(remoteStoreIndexSettings(0, 1)).put("index.routing.allocation.require._name", randomDataNode).build()
        );
        ensureGreen(firstIndex);
        indexSingleDoc(firstIndex, true);

        // Create second index
        createIndex(
            secondIndex,
            Settings.builder().put(remoteStoreIndexSettings(0, 1)).put("index.routing.allocation.require._name", randomDataNode).build()
        );
        ensureGreen(secondIndex);
        indexSingleDoc(secondIndex, true);

        assertNodeStatsParityOnNode(randomDataNode, firstIndex, secondIndex);
    }

    /**
     * - Creates two indices with single primary shard and single replica
     * - Index documents in both of them and forces a fresh for both
     * - Polls the _remotestore/stats API for individual index level stats
     * - Adds up requisite fields from the API output for both indices
     * - Polls _nodes/stats and verifies that the total values at node level adds up
     * to the values capture in the previous step
     * - Repeats the above 3 steps for the second node
     */
    public void testNodesStatsParityWithReplicaShards() throws Exception {
        String firstIndex = INDEX_NAME + "1";
        String secondIndex = INDEX_NAME + "2";

        createIndex(firstIndex, Settings.builder().put(remoteStoreIndexSettings(1, 1)).build());
        ensureGreen(firstIndex);
        indexSingleDoc(firstIndex, true);

        // Create second index
        createIndex(secondIndex, Settings.builder().put(remoteStoreIndexSettings(1, 1)).build());
        ensureGreen(secondIndex);
        indexSingleDoc(secondIndex, true);

        assertBusy(() -> assertNodeStatsParityAcrossNodes(firstIndex, secondIndex), 15, TimeUnit.SECONDS);
    }

    /**
     * Ensures that node stats shows 0 values for dedicated cluster manager nodes
     * since cluster manager nodes does not participate in indexing
     */
    public void testZeroRemoteStatsOnNodesStatsForClusterManager() {
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureGreen(INDEX_NAME);
        indexSingleDoc(INDEX_NAME);
        refresh(INDEX_NAME);

        NodesStatsResponse nodesStatsResponseForClusterManager = client().admin()
            .cluster()
            .prepareNodesStats(internalCluster().getClusterManagerName())
            .setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.Segments, true).set(CommonStatsFlags.Flag.Translog, true))
            .get();

        assertTrue(
            nodesStatsResponseForClusterManager.getNodes().get(0).getNode().isClusterManagerNode()
                && !nodesStatsResponseForClusterManager.getNodes().get(0).getNode().isDataNode()
        );
        assertZeroRemoteSegmentStats(
            nodesStatsResponseForClusterManager.getNodes().get(0).getIndices().getSegments().getRemoteSegmentStats()
        );
        assertZeroRemoteTranslogStats(
            nodesStatsResponseForClusterManager.getNodes().get(0).getIndices().getTranslog().getRemoteTranslogStats()
        );

        NodesStatsResponse nodesStatsResponseForDataNode = client().admin()
            .cluster()
            .prepareNodesStats(primaryNodeName(INDEX_NAME))
            .setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.Segments, true).set(CommonStatsFlags.Flag.Translog, true))
            .get();

        assertTrue(nodesStatsResponseForDataNode.getNodes().get(0).getNode().isDataNode());
        RemoteSegmentStats remoteSegmentStats = nodesStatsResponseForDataNode.getNodes()
            .get(0)
            .getIndices()
            .getSegments()
            .getRemoteSegmentStats();
        assertTrue(remoteSegmentStats.getUploadBytesStarted() > 0);
        assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);

        RemoteTranslogStats remoteTranslogStats = nodesStatsResponseForDataNode.getNodes()
            .get(0)
            .getIndices()
            .getTranslog()
            .getRemoteTranslogStats();
        assertTrue(remoteTranslogStats.getUploadBytesStarted() > 0);
        assertTrue(remoteTranslogStats.getUploadBytesSucceeded() > 0);
    }

    private void assertZeroRemoteSegmentStats(RemoteSegmentStats remoteSegmentStats) {
        // Compare with fresh object because all values default to 0 in default fresh object
        assertEquals(new RemoteSegmentStats(), remoteSegmentStats);
    }

    private void assertZeroRemoteTranslogStats(RemoteTranslogStats remoteTranslogStats) {
        // Compare with fresh object because all values default to 0 in default fresh object
        assertEquals(new RemoteTranslogStats(), remoteTranslogStats);
    }

    private static void assertNodeStatsParityAcrossNodes(String... indices) {
        for (String dataNode : internalCluster().getDataNodeNames()) {
            assertNodeStatsParityOnNode(dataNode, indices);
        }
    }

    private static void assertNodeStatsParityOnNode(String dataNode, String... indices) {
        RemoteSegmentStats remoteSegmentStatsCumulative = new RemoteSegmentStats();
        RemoteTranslogStats remoteTranslogStatsCumulative = new RemoteTranslogStats();
        for (String index : indices) {
            // Fetch _remotestore/stats
            RemoteStoreStatsResponse remoteStoreStats = client(dataNode).admin()
                .cluster()
                .prepareRemoteStoreStats(index, "0")
                .setLocal(true)
                .get();
            remoteSegmentStatsCumulative.add(new RemoteSegmentStats(remoteStoreStats.getRemoteStoreStats()[0].getSegmentStats()));
            remoteTranslogStatsCumulative.add(new RemoteTranslogStats(remoteStoreStats.getRemoteStoreStats()[0].getTranslogStats()));
        }

        // Fetch _nodes/stats
        NodesStatsResponse nodesStatsResponse = client().admin()
            .cluster()
            .prepareNodesStats(dataNode)
            .setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.Segments, true).set(CommonStatsFlags.Flag.Translog, true))
            .get();

        // assert segment stats
        RemoteSegmentStats remoteSegmentStatsFromNodesStats = nodesStatsResponse.getNodes()
            .get(0)
            .getIndices()
            .getSegments()
            .getRemoteSegmentStats();
        assertEquals(remoteSegmentStatsCumulative, remoteSegmentStatsFromNodesStats);
        // Ensure that total upload time has non-zero value if there has been segments uploaded from the node
        if (remoteSegmentStatsCumulative.getUploadBytesStarted() > 0) {
            assertTrue(remoteSegmentStatsCumulative.getTotalUploadTime() > 0);
        }
        // Ensure that total download time has non-zero value if there has been segments downloaded to the node
        if (remoteSegmentStatsCumulative.getDownloadBytesStarted() > 0) {
            assertTrue(remoteSegmentStatsCumulative.getTotalDownloadTime() > 0);
        }

        // assert translog stats
        RemoteTranslogStats remoteTranslogStatsFromNodesStats = nodesStatsResponse.getNodes()
            .get(0)
            .getIndices()
            .getTranslog()
            .getRemoteTranslogStats();
        assertEquals(remoteTranslogStatsCumulative, remoteTranslogStatsFromNodesStats);
    }
}

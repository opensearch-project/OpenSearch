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
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteSegmentStatsFromNodesStatsIT extends RemoteStoreBaseIntegTestCase {
    private static final String INDEX_NAME = "remote-index-1";
    private static final int DATA_NODE_COUNT = 2;
    private static final int CLUSTER_MANAGER_NODE_COUNT = 3;

    @Before
    public void setup() {
        setupCustomCluster();
        setupRepo(false);
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

        long cumulativeUploadsSucceeded = 0, cumulativeUploadsStarted = 0, cumulativeUploadsFailed = 0;
        long totalBytesLag = 0, maxBytesLag = 0, maxTimeLag = 0;
        long totalUploadTime = 0;
        // Fetch upload stats
        RemoteStoreStatsResponse remoteStoreStatsFirstIndex = client(randomDataNode).admin()
            .cluster()
            .prepareRemoteStoreStats(firstIndex, "0")
            .setLocal(true)
            .get();
        cumulativeUploadsSucceeded += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesSucceeded;
        cumulativeUploadsStarted += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesStarted;
        cumulativeUploadsFailed += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesFailed;
        totalBytesLag += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().bytesLag;
        maxBytesLag = Math.max(maxBytesLag, remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().bytesLag);
        maxTimeLag = Math.max(maxTimeLag, remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().refreshTimeLagMs);
        totalUploadTime += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().totalUploadTimeInMs;

        RemoteStoreStatsResponse remoteStoreStatsSecondIndex = client(randomDataNode).admin()
            .cluster()
            .prepareRemoteStoreStats(secondIndex, "0")
            .setLocal(true)
            .get();

        cumulativeUploadsSucceeded += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesSucceeded;
        cumulativeUploadsStarted += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesStarted;
        cumulativeUploadsFailed += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesFailed;
        totalBytesLag += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().bytesLag;
        maxBytesLag = Math.max(maxBytesLag, remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().bytesLag);
        maxTimeLag = Math.max(maxTimeLag, remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().refreshTimeLagMs);
        totalUploadTime += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().totalUploadTimeInMs;

        // Fetch nodes stats
        NodesStatsResponse nodesStatsResponse = client().admin()
            .cluster()
            .prepareNodesStats(randomDataNode)
            .setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.Segments, true))
            .get();
        RemoteSegmentStats remoteSegmentStats = nodesStatsResponse.getNodes().get(0).getIndices().getSegments().getRemoteSegmentStats();
        assertTrue(cumulativeUploadsSucceeded > 0 && cumulativeUploadsSucceeded == remoteSegmentStats.getUploadBytesSucceeded());
        assertTrue(cumulativeUploadsStarted > 0 && cumulativeUploadsStarted == remoteSegmentStats.getUploadBytesStarted());
        assertEquals(cumulativeUploadsFailed, remoteSegmentStats.getUploadBytesFailed());
        assertEquals(totalBytesLag, remoteSegmentStats.getTotalRefreshBytesLag());
        assertEquals(maxBytesLag, remoteSegmentStats.getMaxRefreshBytesLag());
        assertEquals(maxTimeLag, remoteSegmentStats.getMaxRefreshTimeLag());
        assertTrue(totalUploadTime > 0 && totalUploadTime == remoteSegmentStats.getTotalUploadTime());
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
            .setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.Segments, true))
            .get();
        assertTrue(
            nodesStatsResponseForClusterManager.getNodes().get(0).getNode().isClusterManagerNode()
                && !nodesStatsResponseForClusterManager.getNodes().get(0).getNode().isDataNode()
        );
        assertZeroRemoteSegmentStats(
            nodesStatsResponseForClusterManager.getNodes().get(0).getIndices().getSegments().getRemoteSegmentStats()
        );
        NodesStatsResponse nodesStatsResponseForDataNode = client().admin()
            .cluster()
            .prepareNodesStats(primaryNodeName(INDEX_NAME))
            .setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.Segments, true))
            .get();
        assertTrue(nodesStatsResponseForDataNode.getNodes().get(0).getNode().isDataNode());
        RemoteSegmentStats remoteSegmentStats = nodesStatsResponseForDataNode.getNodes()
            .get(0)
            .getIndices()
            .getSegments()
            .getRemoteSegmentStats();
        assertTrue(remoteSegmentStats.getUploadBytesStarted() > 0);
        assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
    }

    private void assertZeroRemoteSegmentStats(RemoteSegmentStats remoteSegmentStats) {
        assertEquals(0, remoteSegmentStats.getUploadBytesStarted());
        assertEquals(0, remoteSegmentStats.getUploadBytesSucceeded());
        assertEquals(0, remoteSegmentStats.getUploadBytesFailed());
        assertEquals(0, remoteSegmentStats.getDownloadBytesStarted());
        assertEquals(0, remoteSegmentStats.getDownloadBytesSucceeded());
        assertEquals(0, remoteSegmentStats.getDownloadBytesFailed());
        assertEquals(0, remoteSegmentStats.getTotalRefreshBytesLag());
        assertEquals(0, remoteSegmentStats.getMaxRefreshBytesLag());
        assertEquals(0, remoteSegmentStats.getMaxRefreshTimeLag());
        assertEquals(0, remoteSegmentStats.getTotalUploadTime());
        assertEquals(0, remoteSegmentStats.getTotalDownloadTime());
    }

    private static void assertNodeStatsParityAcrossNodes(String firstIndex, String secondIndex) {
        for (String dataNode : internalCluster().getDataNodeNames()) {
            long cumulativeUploadsSucceeded = 0, cumulativeUploadsStarted = 0, cumulativeUploadsFailed = 0;
            long cumulativeDownloadsSucceeded = 0, cumulativeDownloadsStarted = 0, cumulativeDownloadsFailed = 0;
            long totalBytesLag = 0, maxBytesLag = 0, maxTimeLag = 0;
            long totalUploadTime = 0, totalDownloadTime = 0;
            // Fetch upload stats
            RemoteStoreStatsResponse remoteStoreStatsFirstIndex = client(dataNode).admin()
                .cluster()
                .prepareRemoteStoreStats(firstIndex, "0")
                .setLocal(true)
                .get();
            cumulativeUploadsSucceeded += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesSucceeded;
            cumulativeUploadsStarted += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesStarted;
            cumulativeUploadsFailed += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesFailed;
            cumulativeDownloadsSucceeded += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0]
                .getSegmentStats().directoryFileTransferTrackerStats.transferredBytesSucceeded;
            cumulativeDownloadsStarted += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0]
                .getSegmentStats().directoryFileTransferTrackerStats.transferredBytesStarted;
            cumulativeDownloadsFailed += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0]
                .getSegmentStats().directoryFileTransferTrackerStats.transferredBytesFailed;
            totalBytesLag += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().bytesLag;
            maxBytesLag = Math.max(maxBytesLag, remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().bytesLag);
            maxTimeLag = Math.max(maxTimeLag, remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().refreshTimeLagMs);
            totalUploadTime += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getSegmentStats().totalUploadTimeInMs;
            totalDownloadTime += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0]
                .getSegmentStats().directoryFileTransferTrackerStats.totalTransferTimeInMs;

            RemoteStoreStatsResponse remoteStoreStatsSecondIndex = client(dataNode).admin()
                .cluster()
                .prepareRemoteStoreStats(secondIndex, "0")
                .setLocal(true)
                .get();
            cumulativeUploadsSucceeded += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesSucceeded;
            cumulativeUploadsStarted += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesStarted;
            cumulativeUploadsFailed += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().uploadBytesFailed;
            cumulativeDownloadsSucceeded += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0]
                .getSegmentStats().directoryFileTransferTrackerStats.transferredBytesSucceeded;
            cumulativeDownloadsStarted += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0]
                .getSegmentStats().directoryFileTransferTrackerStats.transferredBytesStarted;
            cumulativeDownloadsFailed += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0]
                .getSegmentStats().directoryFileTransferTrackerStats.transferredBytesFailed;
            totalBytesLag += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().bytesLag;
            maxBytesLag = Math.max(maxBytesLag, remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().bytesLag);
            maxTimeLag = Math.max(maxTimeLag, remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().refreshTimeLagMs);
            totalUploadTime += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getSegmentStats().totalUploadTimeInMs;
            totalDownloadTime += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0]
                .getSegmentStats().directoryFileTransferTrackerStats.totalTransferTimeInMs;

            // Fetch nodes stats
            NodesStatsResponse nodesStatsResponse = client().admin()
                .cluster()
                .prepareNodesStats(dataNode)
                .setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.Segments, true))
                .get();
            RemoteSegmentStats remoteSegmentStats = nodesStatsResponse.getNodes().get(0).getIndices().getSegments().getRemoteSegmentStats();
            assertEquals(cumulativeUploadsSucceeded, remoteSegmentStats.getUploadBytesSucceeded());
            assertEquals(cumulativeUploadsStarted, remoteSegmentStats.getUploadBytesStarted());
            assertEquals(cumulativeUploadsFailed, remoteSegmentStats.getUploadBytesFailed());
            assertEquals(cumulativeDownloadsSucceeded, remoteSegmentStats.getDownloadBytesSucceeded());
            assertEquals(cumulativeDownloadsStarted, remoteSegmentStats.getDownloadBytesStarted());
            assertEquals(cumulativeDownloadsFailed, remoteSegmentStats.getDownloadBytesFailed());
            assertEquals(totalBytesLag, remoteSegmentStats.getTotalRefreshBytesLag());
            assertEquals(maxBytesLag, remoteSegmentStats.getMaxRefreshBytesLag());
            assertEquals(maxTimeLag, remoteSegmentStats.getMaxRefreshTimeLag());
            // Ensure that total upload time has non-zero value if there has been segments uploaded from the node
            if (cumulativeUploadsStarted > 0) {
                assertTrue(totalUploadTime > 0);
            }
            assertEquals(totalUploadTime, remoteSegmentStats.getTotalUploadTime());
            // Ensure that total download time has non-zero value if there has been segments downloaded to the node
            if (cumulativeDownloadsStarted > 0) {
                assertTrue(totalDownloadTime > 0);
            }
            assertEquals(totalDownloadTime, remoteSegmentStats.getTotalDownloadTime());
        }
    }
}

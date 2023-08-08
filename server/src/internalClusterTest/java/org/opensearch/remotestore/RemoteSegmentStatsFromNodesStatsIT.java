/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.Before;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteSegmentStats;
import org.opensearch.test.OpenSearchIntegTestCase;

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

    public void testNodesStatsParity() {
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
        indexSingleDoc(firstIndex);
        refresh(firstIndex);

        // Create second index
        createIndex(
            secondIndex,
            Settings.builder().put(remoteStoreIndexSettings(0, 1)).put("index.routing.allocation.require._name", randomDataNode).build()
        );
        ensureGreen(secondIndex);
        indexSingleDoc(secondIndex);
        refresh(secondIndex);

        long cumulativeUploads = 0;

        // Fetch upload stats
        RemoteStoreStatsResponse remoteStoreStatsFirstIndex =
            client(randomDataNode).admin().cluster().prepareRemoteStoreStats(firstIndex, "0").setLocal(true).get();
        cumulativeUploads += remoteStoreStatsFirstIndex.getRemoteStoreStats()[0].getStats().uploadBytesSucceeded;
        RemoteStoreStatsResponse remoteStoreStatsSecondIndex =
            client(randomDataNode).admin().cluster().prepareRemoteStoreStats(firstIndex, "0").setLocal(true).get();
        cumulativeUploads += remoteStoreStatsSecondIndex.getRemoteStoreStats()[0].getStats().uploadBytesSucceeded;

        // Fetch nodes stats
        NodesStatsResponse nodesStatsResponse = client().admin()
            .cluster()
            .prepareNodesStats(randomDataNode)
            .setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.Segments, true))
            .get();
        RemoteSegmentStats remoteSegmentStats = nodesStatsResponse.getNodes().get(0).getIndices().getSegments().getRemoteSegmentStats();
        assertEquals(cumulativeUploads, remoteSegmentStats.getUploadBytesSucceeded());
    }

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
        assertTrue(nodesStatsResponseForClusterManager.getNodes().get(0).getNode().isClusterManagerNode()
            && !nodesStatsResponseForClusterManager.getNodes().get(0).getNode().isDataNode());
        assertZeroRemoteSegmentStats(nodesStatsResponseForClusterManager.getNodes()
            .get(0)
            .getIndices()
            .getSegments()
            .getRemoteSegmentStats());
        NodesStatsResponse nodesStatsResponseForDataNode = client().admin()
            .cluster()
            .prepareNodesStats(primaryNodeName(INDEX_NAME))
            .setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.Segments, true))
            .get();
        assertTrue(nodesStatsResponseForDataNode.getNodes().get(0).getNode().isDataNode());
        RemoteSegmentStats remoteSegmentStats =
            nodesStatsResponseForDataNode.getNodes().get(0).getIndices().getSegments().getRemoteSegmentStats();
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
        assertEquals(0, remoteSegmentStats.getMaxRefreshBytesLag());
        assertEquals(0, remoteSegmentStats.getMaxRefreshTimeLag());
    }
}

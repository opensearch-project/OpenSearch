/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.Before;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequestBuilder;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3)
public class RemoteStoreStatsIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-test-idx-1";

    @Before
    public void setup() {
        setupRepo();
    }

    public void testStatsResponseFromAllNodes() {

        // Step 1 - We create cluster, create an index, and then index documents into. We also do multiple refreshes/flushes
        // during this time frame. This ensures that the segment upload has started.
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        indexDocs();

        // Step 2 - We find all the nodes that are present in the cluster. We make the remote store stats api call from
        // each of the node in the cluster and check that the response is coming as expected.
        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getNodes().values().stream().map(DiscoveryNode::getName).collect(Collectors.toList());
        String shardId = "0";
        for (String node : nodes) {
            RemoteStoreStatsResponse response = client(node).admin().cluster().prepareRemoteStoreStats(INDEX_NAME, shardId).get();
            assertTrue(response.getSuccessfulShards() > 0);
            assertTrue(response.getShards() != null && response.getShards().length != 0);
            final String indexShardId = String.format(Locale.ROOT, "[%s][%s]", INDEX_NAME, shardId);
            List<RemoteStoreStats> matches = Arrays.stream(response.getShards())
                .filter(stat -> indexShardId.equals(stat.getStats().shardId.toString()))
                .collect(Collectors.toList());
            assertEquals(1, matches.size());
            RemoteRefreshSegmentTracker.Stats stats = matches.get(0).getStats();
            assertResponseStats(stats);
        }
    }

    public void testStatsResponseAllShards() {

        // Step 1 - We create cluster, create an index, and then index documents into. We also do multiple refreshes/flushes
        // during this time frame. This ensures that the segment upload has started.
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 3));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        indexDocs();

        // Step 2 - We find all the nodes that are present in the cluster. We make the remote store stats api call from
        // each of the node in the cluster and check that the response is coming as expected.
        ClusterState state = getClusterState();
        String node = state.nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).findFirst().get();
        RemoteStoreStatsRequestBuilder remoteStoreStatsRequestBuilder = client(node).admin()
            .cluster()
            .prepareRemoteStoreStats(INDEX_NAME, null);
        RemoteStoreStatsResponse response = remoteStoreStatsRequestBuilder.get();
        assertTrue(response.getSuccessfulShards() == 3);
        assertTrue(response.getShards() != null && response.getShards().length == 3);
        RemoteRefreshSegmentTracker.Stats stats = response.getShards()[0].getStats();
        assertResponseStats(stats);
    }

    public void testStatsResponseFromLocalNode() {

        // Step 1 - We create cluster, create an index, and then index documents into. We also do multiple refreshes/flushes
        // during this time frame. This ensures that the segment upload has started.
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 3));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        indexDocs();

        // Step 2 - We find a data node in the cluster. We make the remote store stats api call from
        // each of the data node in the cluster and check that only local shards are returned.
        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).collect(Collectors.toList());
        for (String node : nodes) {
            RemoteStoreStatsRequestBuilder remoteStoreStatsRequestBuilder = client(node).admin()
                .cluster()
                .prepareRemoteStoreStats(INDEX_NAME, null);
            remoteStoreStatsRequestBuilder.setLocal(true);
            RemoteStoreStatsResponse response = remoteStoreStatsRequestBuilder.get();
            assertTrue(response.getSuccessfulShards() == 1);
            assertTrue(response.getShards() != null && response.getShards().length == 1);
            RemoteRefreshSegmentTracker.Stats stats = response.getShards()[0].getStats();
            assertResponseStats(stats);
        }
    }

    private void indexDocs() {
        // Indexing documents along with refreshes and flushes.
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            if (randomBoolean()) {
                flush(INDEX_NAME);
            } else {
                refresh(INDEX_NAME);
            }
            int numberOfOperations = randomIntBetween(20, 50);
            for (int j = 0; j < numberOfOperations; j++) {
                indexSingleDoc(INDEX_NAME);
            }
        }
    }

    private void assertResponseStats(RemoteRefreshSegmentTracker.Stats stats) {
        assertEquals(0, stats.refreshTimeLagMs);
        assertEquals(stats.localRefreshNumber, stats.remoteRefreshNumber);
        assertTrue(stats.uploadBytesStarted > 0);
        assertEquals(0, stats.uploadBytesFailed);
        assertTrue(stats.uploadBytesSucceeded > 0);
        assertTrue(stats.totalUploadsStarted > 0);
        assertEquals(0, stats.totalUploadsFailed);
        assertTrue(stats.totalUploadsSucceeded > 0);
        assertEquals(0, stats.rejectionCount);
        assertEquals(0, stats.consecutiveFailuresCount);
        assertEquals(0, stats.bytesLag);
        assertTrue(stats.uploadBytesMovingAverage > 0);
        assertTrue(stats.uploadBytesPerSecMovingAverage > 0);
        assertTrue(stats.uploadTimeMovingAverage > 0);
    }
}

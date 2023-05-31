/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.UUIDs;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class RemoteStoreStatsIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-test-idx-1";

    public void testStatsResponseFromAllNodes() {

        // Step 1 - We create cluster, create an index, and then index documents into. We also do multiple refreshes/flushes
        // during this time frame. This ensures that the segment upload has started.
        internalCluster().startDataOnlyNodes(3);
        if (randomBoolean()) {
            createIndex(INDEX_NAME, remoteTranslogIndexSettings(0));
        } else {
            createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        }
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Indexing documents along with refreshes and flushes.
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            if (randomBoolean()) {
                flush(INDEX_NAME);
            } else {
                refresh(INDEX_NAME);
            }
            int numberOfOperations = randomIntBetween(20, 50);
            for (int j = 0; j < numberOfOperations; j++) {
                indexSingleDoc();
            }
        }

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

    private IndexResponse indexSingleDoc() {
        return client().prepareIndex(INDEX_NAME)
            .setId(UUIDs.randomBase64UUID())
            .setSource(randomAlphaOfLength(5), randomAlphaOfLength(5))
            .get();
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.merge;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.merge.MergedSegmentWarmerStats;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/*
 * Integration tests asserting on MergeStats for remote store enabled domains.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MergeStatsIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "test-idx";

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 5)
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.MERGED_SEGMENT_WARMER_EXPERIMENTAL_FLAG, true);
        return featureSettings.build();
    }

    private void setup() {
        internalCluster().startNodes(2);
    }

    public void testNodesStats() throws Exception {
        setup();
        String[] indices = setupIndices(3);

        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getNodes().values().stream().map(DiscoveryNode::getName).toList();

        // ensure merge is executed
        for (String index : indices) {
            client().admin().indices().forceMerge(new ForceMergeRequest(index).maxNumSegments(2));
        }
        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("data:true");
        nodesStatsRequest.indices(CommonStatsFlags.ALL);
        for (String node : nodes) {
            NodesStatsResponse response = client(node).admin().cluster().nodesStats(nodesStatsRequest).get();

            // Shard stats
            List<NodeStats> allNodesStats = response.getNodes();
            assertEquals(2, allNodesStats.size());

            for (NodeStats nodeStats : allNodesStats) {
                assertNotNull(nodeStats.getIndices());
                MergeStats mergeStats = nodeStats.getIndices().getMerge();
                assertNotNull(mergeStats);
                assertMergeStats(mergeStats, StatsScope.AGGREGATED);
                MergedSegmentWarmerStats mergedSegmentWarmerStats = mergeStats.getWarmerStats();
                assertNotNull(mergedSegmentWarmerStats);
                assertMergedSegmentWarmerStats(mergedSegmentWarmerStats, StatsScope.AGGREGATED);
            }
        }
    }

    public void testShardStats() throws Exception {
        setup();

        String[] indices = setupIndices(2);

        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getNodes().values().stream().map(DiscoveryNode::getName).toList();

        // ensure merge is executed
        for (String index : indices) {
            client().admin().indices().forceMerge(new ForceMergeRequest(index).maxNumSegments(2));
        }
        for (String node : nodes) {
            IndicesStatsResponse response = client(node).admin().indices().stats(new IndicesStatsRequest()).get();

            // Shard stats
            ShardStats[] allShardStats = response.getShards();
            assertEquals(4, allShardStats.length);

            for (ShardStats shardStats : allShardStats) {
                StatsScope type = shardStats.getShardRouting().primary() ? StatsScope.PRIMARY_SHARD : StatsScope.REPLICA_SHARD;
                CommonStats commonStats = shardStats.getStats();
                assertNotNull(commonStats);
                MergeStats mergeStats = commonStats.getMerge();
                assertNotNull(mergeStats);
                assertMergeStats(mergeStats, type);
                MergedSegmentWarmerStats mergedSegmentWarmerStats = mergeStats.getWarmerStats();
                assertNotNull(mergedSegmentWarmerStats);
                assertMergedSegmentWarmerStats(mergedSegmentWarmerStats, type);
            }
        }
    }

    public void testIndicesStats() throws Exception {
        setup();
        String[] indices = setupIndices(3);

        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getNodes().values().stream().map(DiscoveryNode::getName).toList();

        // ensure merge is executed
        for (String index : indices) {
            client().admin().indices().forceMerge(new ForceMergeRequest(index).maxNumSegments(2));
        }

        for (String node : nodes) {
            IndicesStatsResponse response = client(node).admin().indices().stats(new IndicesStatsRequest()).get();

            // Shard stats
            Map<String, IndexStats> allIndicesStats = response.getIndices();
            assertEquals(3, allIndicesStats.size());
            for (String index : indices) {
                IndexStats indexStats = allIndicesStats.get(index);
                CommonStats totalStats = indexStats.getTotal();
                CommonStats priStats = indexStats.getPrimaries();
                assertNotNull(totalStats);
                assertNotNull(priStats);

                MergeStats totalMergeStats = totalStats.getMerge();
                assertNotNull(totalMergeStats);
                MergeStats priMergeStats = priStats.getMerge();
                assertNotNull(priMergeStats);

                assertMergeStats(priMergeStats, StatsScope.PRIMARY_SHARD);
                assertMergeStats(totalMergeStats, StatsScope.AGGREGATED);

                MergedSegmentWarmerStats totalMergedSegmentWarmerStats = totalMergeStats.getWarmerStats();
                MergedSegmentWarmerStats priMergedSegmentWarmerStats = priMergeStats.getWarmerStats();

                assertNotNull(totalMergedSegmentWarmerStats);
                assertNotNull(priMergedSegmentWarmerStats);

                assertMergedSegmentWarmerStats(priMergedSegmentWarmerStats, StatsScope.PRIMARY_SHARD);
                assertMergedSegmentWarmerStats(totalMergedSegmentWarmerStats, StatsScope.AGGREGATED);
            }
        }
    }

    private void assertMergeStats(MergeStats stats, StatsScope type) {
        if (type == StatsScope.PRIMARY_SHARD) {
            assertTrue("Current merges should be >= 0", stats.getCurrent() >= 0);
            assertTrue("Current merge docs should be >= 0", stats.getCurrentNumDocs() >= 0);
            assertTrue("Current merge size should be >= 0", stats.getCurrentSizeInBytes() >= 0);
            assertTrue("Total merges should be >= 1", stats.getTotal() >= 1);
            assertTrue("Total merge time should be >= 1ms", stats.getTotalTimeInMillis() >= 1);
            assertTrue("Total merge time should be >= 1ms", stats.getTotalTime().getMillis() >= 1);
            assertTrue("Total merged docs should be >= 1", stats.getTotalNumDocs() >= 1);
            assertTrue("Total merged size should be >= 1 byte", stats.getTotalSizeInBytes() >= 1);
            assertTrue("Total merged size should be >= 1 byte", stats.getTotalSize().getBytes() >= 1);
            assertTrue("Total stopped time should be >= 0", stats.getTotalStoppedTimeInMillis() >= 0);
            assertTrue("Total stopped time should be >= 0", stats.getTotalStoppedTime().getMillis() >= 0);
            assertTrue("Total throttled time should be >= 0", stats.getTotalThrottledTime().getMillis() >= 0);
            assertTrue("Total throttled time should be >= 0", stats.getTotalThrottledTimeInMillis() >= 0);
        } else if (type == StatsScope.REPLICA_SHARD) {
            assertEquals("Replica shard current merges should be 0", 0, stats.getCurrent());
            assertEquals("Replica shard current merge docs should be 0", 0, stats.getCurrentNumDocs());
            assertEquals("Replica shard current merge size should be 0", 0, stats.getCurrentSizeInBytes());
            assertEquals("Replica shard total merges should be 0", 0, stats.getTotal());
            assertEquals("Replica shard total merge time should be 0", 0, stats.getTotalTimeInMillis());
            assertEquals("Replica shard total merge time should be 0", 0, stats.getTotalTime().getMillis());
            assertEquals("Replica shard total merged docs should be 0", 0, stats.getTotalNumDocs());
            assertEquals("Replica shard total merged size should be 0", 0, stats.getTotalSizeInBytes());
            assertEquals("Replica shard total merged size should be 0", 0, stats.getTotalSize().getBytes());
            assertEquals("Replica shard total stopped time should be 0", 0, stats.getTotalStoppedTimeInMillis());
            assertEquals("Replica shard total stopped time should be 0", 0, stats.getTotalStoppedTime().getMillis());
            assertEquals("Replica shard total throttled time should be 0", 0, stats.getTotalThrottledTime().getMillis());
            assertEquals("Replica shard total throttled time should be 0", 0, stats.getTotalThrottledTimeInMillis());
        } else if (type == StatsScope.AGGREGATED) {
            // the node might have both primaries and replicas, only primaries, or only replicas
            boolean primaryShardStatsResult = false;
            boolean replicaShardStatsResult = false;

            try {
                assertMergeStats(stats, StatsScope.PRIMARY_SHARD);
                primaryShardStatsResult = true;
            } catch (AssertionError ignored) {}

            try {
                assertMergeStats(stats, StatsScope.REPLICA_SHARD);
                replicaShardStatsResult = true;
            } catch (AssertionError ignored) {}

            assertTrue(
                "Stats should match either primary or replica shard patterns or both.",
                primaryShardStatsResult || replicaShardStatsResult
            );
        }
    }

    private void assertMergedSegmentWarmerStats(MergedSegmentWarmerStats stats, StatsScope type) {
        if (type == StatsScope.PRIMARY_SHARD) {
            assertTrue("Primary shard warm invocations should be >= 1", stats.getTotalInvocationsCount() >= 1);
            assertTrue("Primary shard warm time should be >= 1ms", stats.getTotalTime().getMillis() >= 1);
            assertTrue("Primary shard warm failures should be >= 0", stats.getTotalFailureCount() >= 0);
            assertTrue("Primary shard sent size should be >= 0", stats.getTotalSentSize().getBytes() >= 0);
            assertEquals("Primary shard received size should be 0", 0, stats.getTotalReceivedSize().getBytes());
            assertTrue("Primary shard send time should be >= 0", stats.getTotalSendTime().millis() >= 0);
            assertEquals("Primary shard receive time should be 0", 0, stats.getTotalReceiveTime().millis());
            assertTrue("Primary shard ongoing warms should be >= 0", stats.getOngoingCount() >= 0);
        } else if (type == StatsScope.REPLICA_SHARD) {
            assertEquals("Replica shard warm invocations should be 0", 0, stats.getTotalInvocationsCount());
            assertEquals("Replica shard warm time should be 0", 0, stats.getTotalTime().getMillis());
            assertEquals("Replica shard warm failures should be 0", 0, stats.getTotalFailureCount());
            assertEquals("Replica shard sent size should be 0", 0, stats.getTotalSentSize().getBytes());
            assertTrue("Replica shard received size should be >= 1", stats.getTotalReceivedSize().getBytes() >= 1);
            assertEquals("Replica shard send time should be 0", 0, stats.getTotalSendTime().millis());
            assertTrue("Replica shard receive time should be >= 1ms", stats.getTotalReceiveTime().millis() >= 1);
            assertEquals("Replica shard ongoing warms should be 0", 0, stats.getOngoingCount());
        } else if (type == StatsScope.AGGREGATED) {
            // the node might have both primaries and replicas, only primaries, or only replicas

            // would evaluate to true if the node only contains primary shards
            boolean primaryShardStatsResult = false;

            // would evaluate to true if the node only contains replica shards
            boolean replicaShardStatsResult = false;

            // would evaluate to true if the node contains a mix of primary and replica shards
            boolean primaryAndReplicaShardsResult = stats.getOngoingCount() >= 0
                && stats.getTotalTime().getMillis() >= 1
                && stats.getTotalSendTime().getMillis() >= 1
                && stats.getTotalReceiveTime().getMillis() >= 1
                && stats.getTotalInvocationsCount() >= 1
                && stats.getTotalReceivedSize().getBytes() >= 1
                && stats.getTotalSentSize().getBytes() >= 1
                && stats.getTotalFailureCount() >= 0;

            if (primaryAndReplicaShardsResult = true) {
                return;
            }

            try {
                assertMergedSegmentWarmerStats(stats, StatsScope.PRIMARY_SHARD);
                primaryShardStatsResult = true;
            } catch (AssertionError ignored) {}

            try {
                assertMergedSegmentWarmerStats(stats, StatsScope.REPLICA_SHARD);
                replicaShardStatsResult = true; // would be true if the node only contains replica shards
            } catch (AssertionError ignored) {}

            assertTrue(
                "Stats should match either primary or replica shard or patterns both.",
                primaryShardStatsResult || replicaShardStatsResult
            );
        }
    }

    private void indexDocs(String... indexNames) {
        for (String indexName : indexNames) {
            for (int i = 0; i < randomIntBetween(25, 30); i++) {
                if (randomBoolean()) {
                    flush(indexName);
                } else {
                    refresh(indexName);
                }
                int numberOfOperations = randomIntBetween(25, 30);
                indexBulk(indexName, numberOfOperations);
            }
        }
    }

    private String[] setupIndices(int count) throws Exception {
        if (count <= 0) {
            return new String[0];
        }
        String[] indices = new String[count];
        for (int i = 0; i < count; i++) {
            indices[i] = INDEX_NAME + i;
        }
        createIndex(indices);
        ensureGreen(indices);
        for (String index : indices) {
            indexDocs(index);
        }
        waitForDocsOnReplicas(indices);
        return indices;
    }

    private void waitForDocsOnReplicas(String... indices) throws Exception {
        for (String index : indices) {
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.preference("_replica");
            assertBusy(() -> {
                long totalDocs = client().search(searchRequest).actionGet().getHits().getTotalHits().value();
                assertTrue("Docs should be searchable on replicas", totalDocs > 0);
            }, 10, TimeUnit.SECONDS);
        }
    }

    private enum StatsScope {
        PRIMARY_SHARD,
        REPLICA_SHARD,
        AGGREGATED
    }
}

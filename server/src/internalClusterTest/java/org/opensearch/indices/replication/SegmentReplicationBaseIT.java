/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.index.SegmentInfos;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.opensearch.test.OpenSearchIntegTestCase.client;
import static org.opensearch.test.OpenSearchTestCase.assertBusy;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public class SegmentReplicationBaseIT extends OpenSearchIntegTestCase {

    protected static final String INDEX_NAME = "test-idx-1";
    protected static final int SHARD_COUNT = 1;
    protected static final int REPLICA_COUNT = 1;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return asList(MockTransportService.TestPlugin.class);
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    @Nullable
    protected ShardRouting getShardRoutingForNodeName(String nodeName) {
        final ClusterState state = getClusterState();
        for (IndexShardRoutingTable shardRoutingTable : state.routingTable().index(INDEX_NAME)) {
            for (ShardRouting shardRouting : shardRoutingTable.activeShards()) {
                final String nodeId = shardRouting.currentNodeId();
                final DiscoveryNode discoveryNode = state.nodes().resolveNode(nodeId);
                if (discoveryNode.getName().equals(nodeName)) {
                    return shardRouting;
                }
            }
        }
        return null;
    }

    protected void assertDocCounts(int expectedDocCount, String... nodeNames) {
        for (String node : nodeNames) {
            assertHitCount(client(node).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedDocCount);
        }
    }

    protected DiscoveryNode getNodeContainingPrimaryShard() {
        final ClusterState state = getClusterState();
        final ShardRouting primaryShard = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
        return state.nodes().resolveNode(primaryShard.currentNodeId());
    }

    /**
     * Waits until all given nodes have at least the expected docCount.
     *
     * @param docCount - Expected Doc count.
     * @param nodes    - List of node names.
     */
    protected void waitForSearchableDocs(long docCount, List<String> nodes) throws Exception {
        // wait until the replica has the latest segment generation.
        waitForSearchableDocs(INDEX_NAME, docCount, nodes);
    }

    public static void waitForSearchableDocs(String indexName, long docCount, List<String> nodes) throws Exception {
        // wait until the replica has the latest segment generation.
        assertBusy(() -> {
            for (String node : nodes) {
                final SearchResponse response = client(node).prepareSearch(indexName).setSize(0).setPreference("_only_local").get();
                final long hits = response.getHits().getTotalHits().value();
                if (hits < docCount) {
                    fail("Expected search hits on node: " + node + " to be at least " + docCount + " but was: " + hits);
                }
            }
        }, 1, TimeUnit.MINUTES);
    }

    protected void waitForSearchableDocs(long docCount, String... nodes) throws Exception {
        waitForSearchableDocs(docCount, Arrays.stream(nodes).collect(Collectors.toList()));
    }

    protected void verifyStoreContent() throws Exception {
        assertBusy(() -> {
            final ClusterState clusterState = getClusterState();
            for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
                for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                    final ShardRouting primaryRouting = shardRoutingTable.primaryShard();
                    final String indexName = primaryRouting.getIndexName();
                    final List<ShardRouting> replicaRouting = shardRoutingTable.replicaShards();
                    final IndexShard primaryShard = getIndexShard(clusterState, primaryRouting, indexName);
                    final int primaryDocCount = getDocCountFromShard(primaryShard);
                    final Map<String, StoreFileMetadata> primarySegmentMetadata = primaryShard.getSegmentMetadataMap();
                    for (ShardRouting replica : replicaRouting) {
                        IndexShard replicaShard = getIndexShard(clusterState, replica, indexName);
                        final Store.RecoveryDiff recoveryDiff = Store.segmentReplicationDiff(
                            primarySegmentMetadata,
                            replicaShard.getSegmentMetadataMap()
                        );
                        final int replicaDocCount = getDocCountFromShard(replicaShard);
                        assertEquals("Doc counts should match", primaryDocCount, replicaDocCount);
                        if (recoveryDiff.missing.isEmpty() == false || recoveryDiff.different.isEmpty() == false) {
                            fail(
                                "Expected no missing or different segments between primary and replica but diff was missing: "
                                    + recoveryDiff.missing
                                    + " Different: "
                                    + recoveryDiff.different
                                    + " Primary Replication Checkpoint : "
                                    + primaryShard.getLatestReplicationCheckpoint()
                                    + " Replica Replication Checkpoint: "
                                    + replicaShard.getLatestReplicationCheckpoint()
                            );
                        }
                        // calls to readCommit will fail if a valid commit point and all its segments are not in the store.
                        replicaShard.store().readLastCommittedSegmentsInfo();
                    }
                }
            }
        }, 1, TimeUnit.MINUTES);
    }

    private int getDocCountFromShard(IndexShard shard) {
        try (final Engine.Searcher searcher = shard.acquireSearcher("test")) {
            return searcher.getDirectoryReader().numDocs();
        }
    }

    private IndexShard getIndexShard(ClusterState state, ShardRouting routing, String indexName) {
        return getIndexShard(state.nodes().get(routing.currentNodeId()).getName(), routing.shardId(), indexName);
    }

    /**
     * Fetch IndexShard, assumes only a single shard per node.
     */
    protected IndexShard getIndexShard(String node, String indexName) {
        final Index index = resolveIndex(indexName);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexService(index);
        assertNotNull(indexService);
        final Optional<Integer> shardId = indexService.shardIds().stream().findFirst();
        return shardId.map(indexService::getShard).orElse(null);
    }

    protected boolean segmentReplicationWithRemoteEnabled() {
        return IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexSettings()).booleanValue();
    }

    protected Releasable blockReplication(List<String> nodes, CountDownLatch latch) {
        CountDownLatch pauseReplicationLatch = new CountDownLatch(nodes.size());
        for (String node : nodes) {

            MockTransportService mockTargetTransportService = ((MockTransportService) internalCluster().getInstance(
                TransportService.class,
                node
            ));
            mockTargetTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                String actionToWaitFor = SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES;
                if (segmentReplicationWithRemoteEnabled()) {
                    actionToWaitFor = SegmentReplicationSourceService.Actions.UPDATE_VISIBLE_CHECKPOINT;
                }
                if (action.equals(actionToWaitFor)) {
                    try {
                        latch.countDown();
                        pauseReplicationLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        return () -> {
            while (pauseReplicationLatch.getCount() > 0) {
                pauseReplicationLatch.countDown();
            }
        };
    }

    protected void assertReplicaCheckpointUpdated(IndexShard primaryShard) throws Exception {
        assertBusy(() -> {
            Set<SegmentReplicationShardStats> groupStats = primaryShard.getReplicationStatsForTrackedReplicas();
            assertEquals(primaryShard.indexSettings().getNumberOfReplicas(), groupStats.size());
            for (SegmentReplicationShardStats shardStat : groupStats) {
                assertEquals(0, shardStat.getCheckpointsBehindCount());
            }
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Returns the latest SIS for a shard but does not incref the segments.
     */
    protected SegmentInfos getLatestSegmentInfos(IndexShard shard) throws IOException {
        final Tuple<GatedCloseable<SegmentInfos>, ReplicationCheckpoint> tuple = shard.getLatestSegmentInfosAndCheckpoint();
        try (final GatedCloseable<SegmentInfos> closeable = tuple.v1()) {
            return closeable.get();
        }
    }
}

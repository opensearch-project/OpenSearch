/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.ReplicationStats;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationStatsIT extends SegmentReplicationBaseIT {

    public void testSegmentReplicationStatsResponse() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String anotherDataNode = internalCluster().startDataOnlyNode();

        int numShards = 4;
        assertAcked(
            prepareCreate(
                INDEX_NAME,
                0,
                Settings.builder()
                    .put("number_of_shards", numShards)
                    .put("number_of_replicas", 1)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            )
        );
        ensureGreen();
        final long numDocs = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            index(INDEX_NAME, "doc", Integer.toString(i));
        }
        refresh(INDEX_NAME);
        ensureSearchable(INDEX_NAME);

        assertBusy(() -> {
            SegmentReplicationStatsResponse segmentReplicationStatsResponse = dataNodeClient().admin()
                .indices()
                .prepareSegmentReplicationStats(INDEX_NAME)
                .setDetailed(true)
                .execute()
                .actionGet();
            SegmentReplicationPerGroupStats perGroupStats = segmentReplicationStatsResponse.getReplicationStats().get(INDEX_NAME).get(0);
            final SegmentReplicationState currentReplicationState = perGroupStats.getReplicaStats()
                .stream()
                .findFirst()
                .get()
                .getCurrentReplicationState();
            assertEquals(segmentReplicationStatsResponse.getReplicationStats().size(), 1);
            assertEquals(segmentReplicationStatsResponse.getTotalShards(), numShards * 2);
            assertEquals(segmentReplicationStatsResponse.getSuccessfulShards(), numShards * 2);
            assertNotNull(currentReplicationState);
            assertEquals(currentReplicationState.getStage(), SegmentReplicationState.Stage.DONE);
            assertTrue(currentReplicationState.getIndex().recoveredFileCount() > 0);
        }, 1, TimeUnit.MINUTES);
    }

    public void testSegmentReplicationStatsResponseForActiveOnly() throws Exception {
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        // index 10 docs
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        refresh(INDEX_NAME);

        // index 10 more docs
        waitForSearchableDocs(10L, asList(primaryNode, replicaNode));
        for (int i = 10; i < 20; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        final CountDownLatch waitForReplication = new CountDownLatch(1);

        final CountDownLatch waitForAssertions = new CountDownLatch(1);
        // Mock transport service to add behaviour of waiting in GET_SEGMENT_FILES Stage of a segment replication event.
        MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            replicaNode
        ));
        mockTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, primaryNode),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES)) {
                    waitForReplication.countDown();
                    try {
                        waitForAssertions.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );
        refresh(INDEX_NAME);
        try {
            waitForReplication.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // verifying active_only by checking if current stage is GET_FILES STAGE
        SegmentReplicationStatsResponse activeOnlyResponse = client().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .setActiveOnly(true)
            .setDetailed(true)
            .execute()
            .actionGet();
        SegmentReplicationPerGroupStats perGroupStats = activeOnlyResponse.getReplicationStats().get(INDEX_NAME).get(0);
        SegmentReplicationState.Stage stage = perGroupStats.getReplicaStats()
            .stream()
            .findFirst()
            .get()
            .getCurrentReplicationState()
            .getStage();
        assertEquals(SegmentReplicationState.Stage.GET_FILES, stage);
        waitForAssertions.countDown();
    }

    public void testNonDetailedResponse() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        int numReplicas = 4;
        List<String> nodes = new ArrayList<>();
        final String primaryNode = internalCluster().startNode();
        nodes.add(primaryNode);
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .build()
        );
        ensureYellow(INDEX_NAME);
        for (int i = 0; i < numReplicas; i++) {
            nodes.add(internalCluster().startNode());
        }
        ensureGreen(INDEX_NAME);

        final long numDocs = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            index(INDEX_NAME, "doc", Integer.toString(i));
        }
        refresh(INDEX_NAME);
        waitForSearchableDocs(numDocs, nodes);

        final IndexShard indexShard = getIndexShard(primaryNode, INDEX_NAME);

        assertBusy(() -> {
            SegmentReplicationStatsResponse segmentReplicationStatsResponse = dataNodeClient().admin()
                .indices()
                .prepareSegmentReplicationStats(INDEX_NAME)
                .execute()
                .actionGet();

            final Map<String, List<SegmentReplicationPerGroupStats>> replicationStats = segmentReplicationStatsResponse
                .getReplicationStats();
            assertEquals(1, replicationStats.size());
            final List<SegmentReplicationPerGroupStats> replicationPerGroupStats = replicationStats.get(INDEX_NAME);
            assertEquals(1, replicationPerGroupStats.size());
            final SegmentReplicationPerGroupStats perGroupStats = replicationPerGroupStats.get(0);
            assertEquals(perGroupStats.getShardId(), indexShard.shardId());
            final Set<SegmentReplicationShardStats> replicaStats = perGroupStats.getReplicaStats();
            assertEquals(4, replicaStats.size());
            for (SegmentReplicationShardStats replica : replicaStats) {
                assertNotNull(replica.getCurrentReplicationState());
            }
        });
    }

    public void testGetSpecificShard() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        List<String> nodes = new ArrayList<>();
        final String primaryNode = internalCluster().startNode();
        nodes.add(primaryNode);
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()
        );
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        nodes.add(internalCluster().startNode());
        ensureGreen(INDEX_NAME);

        final long numDocs = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            index(INDEX_NAME, "doc", Integer.toString(i));
        }
        refresh(INDEX_NAME);
        waitForSearchableDocs(numDocs, nodes);

        final IndexShard indexShard = getIndexShard(primaryNode, INDEX_NAME);

        // search for all
        SegmentReplicationStatsResponse segmentReplicationStatsResponse = client().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .setActiveOnly(true)
            .execute()
            .actionGet();

        Map<String, List<SegmentReplicationPerGroupStats>> replicationStats = segmentReplicationStatsResponse.getReplicationStats();
        assertEquals(1, replicationStats.size());
        List<SegmentReplicationPerGroupStats> replicationPerGroupStats = replicationStats.get(INDEX_NAME);
        assertEquals(2, replicationPerGroupStats.size());
        for (SegmentReplicationPerGroupStats group : replicationPerGroupStats) {
            assertEquals(1, group.getReplicaStats().size());
        }

        // now search for one shard.
        final int id = indexShard.shardId().getId();
        segmentReplicationStatsResponse = client().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .setActiveOnly(true)
            .shards(String.valueOf(id))
            .execute()
            .actionGet();

        replicationStats = segmentReplicationStatsResponse.getReplicationStats();
        assertEquals(1, replicationStats.size());
        replicationPerGroupStats = replicationStats.get(INDEX_NAME);
        assertEquals(1, replicationPerGroupStats.size());
        for (SegmentReplicationPerGroupStats group : replicationPerGroupStats) {
            assertEquals(group.getShardId(), indexShard.shardId());
            assertEquals(1, group.getReplicaStats().size());
        }

    }

    public void testMultipleIndices() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String index_2 = "tst-index-2";
        List<String> nodes = new ArrayList<>();
        final String primaryNode = internalCluster().startDataOnlyNode();
        nodes.add(primaryNode);
        createIndex(INDEX_NAME, index_2);

        ensureYellowAndNoInitializingShards(INDEX_NAME, index_2);
        nodes.add(internalCluster().startDataOnlyNode());
        ensureGreen(INDEX_NAME, index_2);

        final long numDocs = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            index(INDEX_NAME, "doc", Integer.toString(i));
            index(index_2, "doc", Integer.toString(i));
        }
        refresh(INDEX_NAME, index_2);
        waitForSearchableDocs(INDEX_NAME, numDocs, nodes);
        waitForSearchableDocs(index_2, numDocs, nodes);
        ensureSearchable(INDEX_NAME, index_2);

        final IndexShard index_1_primary = getIndexShard(primaryNode, INDEX_NAME);
        final IndexShard index_2_primary = getIndexShard(primaryNode, index_2);

        assertTrue(index_1_primary.routingEntry().primary());
        assertTrue(index_2_primary.routingEntry().primary());

        assertBusy(() -> {
            // test both indices are returned in the response.
            SegmentReplicationStatsResponse segmentReplicationStatsResponse = dataNodeClient().admin()
                .indices()
                .prepareSegmentReplicationStats()
                .execute()
                .actionGet();

            Map<String, List<SegmentReplicationPerGroupStats>> replicationStats = segmentReplicationStatsResponse.getReplicationStats();
            assertEquals(2, replicationStats.size());
            List<SegmentReplicationPerGroupStats> replicationPerGroupStats = replicationStats.get(INDEX_NAME);
            assertEquals(1, replicationPerGroupStats.size());
            SegmentReplicationPerGroupStats perGroupStats = replicationPerGroupStats.get(0);
            assertEquals(perGroupStats.getShardId(), index_1_primary.shardId());
            Set<SegmentReplicationShardStats> replicaStats = perGroupStats.getReplicaStats();
            assertEquals(1, replicaStats.size());
            for (SegmentReplicationShardStats replica : replicaStats) {
                assertNotNull(replica.getCurrentReplicationState());
            }

            replicationPerGroupStats = replicationStats.get(index_2);
            assertEquals(1, replicationPerGroupStats.size());
            perGroupStats = replicationPerGroupStats.get(0);
            assertEquals(perGroupStats.getShardId(), index_2_primary.shardId());
            replicaStats = perGroupStats.getReplicaStats();
            assertEquals(1, replicaStats.size());
            for (SegmentReplicationShardStats replica : replicaStats) {
                assertNotNull(replica.getCurrentReplicationState());
            }
        }, 30, TimeUnit.SECONDS);

        // test only single index queried.
        SegmentReplicationStatsResponse segmentReplicationStatsResponse = dataNodeClient().admin()
            .indices()
            .prepareSegmentReplicationStats()
            .setIndices(index_2)
            .execute()
            .actionGet();
        assertEquals(1, segmentReplicationStatsResponse.getReplicationStats().size());
        assertTrue(segmentReplicationStatsResponse.getReplicationStats().containsKey(index_2));
    }

    public void testQueryAgainstDocRepIndex() {
        internalCluster().startClusterManagerOnlyNode();
        List<String> nodes = new ArrayList<>();
        final String primaryNode = internalCluster().startNode();
        nodes.add(primaryNode);
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
                .build()
        );
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        nodes.add(internalCluster().startNode());
        ensureGreen(INDEX_NAME);

        final long numDocs = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            index(INDEX_NAME, "doc", Integer.toString(i));
        }
        refresh(INDEX_NAME);

        // search for all
        SegmentReplicationStatsResponse segmentReplicationStatsResponse = client().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .execute()
            .actionGet();
        assertTrue(segmentReplicationStatsResponse.getReplicationStats().isEmpty());
    }

    public void testSegmentReplicationNodeAndIndexStats() throws Exception {
        logger.info("--> start primary node");
        final String primaryNode = internalCluster().startNode();

        logger.info("--> create index on node: {}", primaryNode);
        assertAcked(prepareCreate(INDEX_NAME, Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)));

        ensureYellow();
        logger.info("--> start first replica node");
        final String replicaNode1 = internalCluster().startNode();

        logger.info("--> start second replica node");
        final String replicaNode2 = internalCluster().startNode();

        ensureGreen();
        CountDownLatch latch = new CountDownLatch(1);
        // block replication
        try (final Releasable ignored = blockReplication(List.of(replicaNode1, replicaNode2), latch)) {
            // index another doc while blocked, this would not get replicated to the replicas.
            Thread indexingThread = new Thread(() -> {
                client().prepareIndex(INDEX_NAME).setId("2").setSource("foo2", randomInt()).get();
                refresh(INDEX_NAME);
            });

            indexingThread.start();
            indexingThread.join();
            latch.await();

            NodesStatsResponse nodesStatsResponse = client().admin()
                .cluster()
                .prepareNodesStats()
                .clear()
                .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Segments))
                .get();

            for (NodeStats nodeStats : nodesStatsResponse.getNodes()) {
                ReplicationStats replicationStats = nodeStats.getIndices().getSegments().getReplicationStats();
                // primary node - should hold replication statistics
                if (nodeStats.getNode().getName().equals(primaryNode)) {
                    assertTrue(replicationStats.getMaxBytesBehind() > 0);
                    assertTrue(replicationStats.getTotalBytesBehind() > 0);
                    assertTrue(replicationStats.getMaxReplicationLag() > 0);
                    // 2 replicas so total bytes should be double of max
                    assertEquals(replicationStats.getMaxBytesBehind() * 2, replicationStats.getTotalBytesBehind());
                }
                // replica nodes - should hold empty replication statistics
                if (nodeStats.getNode().getName().equals(replicaNode1) || nodeStats.getNode().getName().equals(replicaNode2)) {
                    assertEquals(0, replicationStats.getMaxBytesBehind());
                    assertEquals(0, replicationStats.getTotalBytesBehind());
                    assertEquals(0, replicationStats.getMaxReplicationLag());
                }
            }
            // get replication statistics at index level
            IndicesStatsResponse stats = client().admin().indices().prepareStats().execute().actionGet();

            // stats should be of non-zero value when aggregated at index level
            ReplicationStats indexReplicationStats = stats.getIndex(INDEX_NAME).getTotal().getSegments().getReplicationStats();
            assertNotNull(indexReplicationStats);
            assertTrue(indexReplicationStats.getMaxBytesBehind() > 0);
            assertTrue(indexReplicationStats.getTotalBytesBehind() > 0);
            assertTrue(indexReplicationStats.getMaxReplicationLag() > 0);
            assertEquals(2 * indexReplicationStats.getMaxBytesBehind(), indexReplicationStats.getTotalBytesBehind());
        }

    }

}

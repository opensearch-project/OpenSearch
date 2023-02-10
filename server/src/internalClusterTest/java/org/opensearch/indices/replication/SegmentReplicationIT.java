/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Requests;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static java.util.Arrays.asList;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationIT extends SegmentReplicationBaseIT {

    public void testPrimaryStopped_ReplicaPromoted() throws Exception {
        final String primary = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        waitForSearchableDocs(1, primary, replica);

        // index another doc but don't refresh, we will ensure this is searchable once replica is promoted.
        client().prepareIndex(INDEX_NAME).setId("2").setSource("bar", "baz").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // stop the primary node - we only have one shard on here.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        final ShardRouting replicaShardRouting = getShardRoutingForNodeName(replica);
        assertNotNull(replicaShardRouting);
        assertTrue(replicaShardRouting + " should be promoted as a primary", replicaShardRouting.primary());
        refresh(INDEX_NAME);
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);

        // assert we can index into the new primary.
        client().prepareIndex(INDEX_NAME).setId("3").setSource("bar", "baz").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);

        // start another node, index another doc and replicate.
        String nodeC = internalCluster().startNode();
        ensureGreen(INDEX_NAME);
        client().prepareIndex(INDEX_NAME).setId("4").setSource("baz", "baz").get();
        refresh(INDEX_NAME);
        waitForSearchableDocs(4, nodeC, replica);
        verifyStoreContent();
    }

    public void testRestartPrimary() throws Exception {
        final String primary = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        assertEquals(getNodeContainingPrimaryShard().getName(), primary);

        final int initialDocCount = 1;
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        waitForSearchableDocs(initialDocCount, replica, primary);

        internalCluster().restartNode(primary);
        ensureGreen(INDEX_NAME);

        assertEquals(getNodeContainingPrimaryShard().getName(), replica);

        flushAndRefresh(INDEX_NAME);
        waitForSearchableDocs(initialDocCount, replica, primary);
        verifyStoreContent();
    }

    public void testCancelPrimaryAllocation() throws Exception {
        // this test cancels allocation on the primary - promoting the new replica and recreating the former primary as a replica.
        final String primary = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        final int initialDocCount = 1;

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        waitForSearchableDocs(initialDocCount, replica, primary);

        final IndexShard indexShard = getIndexShard(primary, INDEX_NAME);
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new CancelAllocationCommand(INDEX_NAME, indexShard.shardId().id(), primary, true))
            .execute()
            .actionGet();
        ensureGreen(INDEX_NAME);

        assertEquals(getNodeContainingPrimaryShard().getName(), replica);

        flushAndRefresh(INDEX_NAME);
        waitForSearchableDocs(initialDocCount, replica, primary);
        verifyStoreContent();
    }

    public void testReplicationAfterPrimaryRefreshAndFlush() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 200);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, nodeA, nodeB);

            final int additionalDocCount = scaledRandomIntBetween(0, 200);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);

            flushAndRefresh(INDEX_NAME);
            waitForSearchableDocs(expectedHitCount, nodeA, nodeB);

            ensureGreen(INDEX_NAME);
            verifyStoreContent();
        }
    }

    public void testIndexReopenClose() throws Exception {
        final String primary = internalCluster().startNode();
        final String replica = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(100, 200);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            flush(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, primary, replica);
        }
        logger.info("--> Closing the index ");
        client().admin().indices().prepareClose(INDEX_NAME).get();

        logger.info("--> Opening the index");
        client().admin().indices().prepareOpen(INDEX_NAME).get();

        ensureGreen(INDEX_NAME);
        waitForSearchableDocs(initialDocCount, primary, replica);
        verifyStoreContent();
    }

    public void testMultipleShards() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();
        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(1, 200);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, nodeA, nodeB);

            final int additionalDocCount = scaledRandomIntBetween(0, 200);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);

            flushAndRefresh(INDEX_NAME);
            waitForSearchableDocs(expectedHitCount, nodeA, nodeB);

            ensureGreen(INDEX_NAME);
            verifyStoreContent();
        }
    }

    public void testReplicationAfterForceMerge() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 200);
        final int additionalDocCount = scaledRandomIntBetween(0, 200);
        final int expectedHitCount = initialDocCount + additionalDocCount;
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);

            flush(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, nodeA, nodeB);

            // Index a second set of docs so we can merge into one segment.
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);
            waitForSearchableDocs(expectedHitCount, nodeA, nodeB);

            // Force a merge here so that the in memory SegmentInfos does not reference old segments on disk.
            client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();
            refresh(INDEX_NAME);
            verifyStoreContent();
        }
    }

    public void testCancellation() throws Exception {
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureYellow(INDEX_NAME);

        final String replicaNode = internalCluster().startNode();

        final SegmentReplicationSourceService segmentReplicationSourceService = internalCluster().getInstance(
            SegmentReplicationSourceService.class,
            primaryNode
        );
        final IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);

        CountDownLatch latch = new CountDownLatch(1);

        MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNode
        ));
        mockTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, replicaNode),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationTargetService.Actions.FILE_CHUNK)) {
                    FileChunkRequest req = (FileChunkRequest) request;
                    logger.debug("file chunk [{}] lastChunk: {}", req, req.lastChunk());
                    if (req.name().endsWith("cfs") && req.lastChunk()) {
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );

        final int docCount = scaledRandomIntBetween(0, 200);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(docCount);
            waitForDocs(docCount, indexer);

            flush(INDEX_NAME);
        }
        segmentReplicationSourceService.beforeIndexShardClosed(primaryShard.shardId(), primaryShard, indexSettings());
        latch.countDown();
        assertDocCounts(docCount, primaryNode);
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/5669")
    public void testStartReplicaAfterPrimaryIndexesDocs() throws Exception {
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        // Index a doc to create the first set of segments. _s1.si
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();
        // Flush segments to disk and create a new commit point (Primary: segments_3, _s1.si)
        flushAndRefresh(INDEX_NAME);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);

        // Index to create another segment
        client().prepareIndex(INDEX_NAME).setId("2").setSource("foo", "bar").get();

        // Force a merge here so that the in memory SegmentInfos does not reference old segments on disk.
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();
        refresh(INDEX_NAME);

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);
        assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);

        client().prepareIndex(INDEX_NAME).setId("3").setSource("foo", "bar").get();
        refresh(INDEX_NAME);
        waitForSearchableDocs(3, primaryNode, replicaNode);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);
        assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);
        verifyStoreContent();
    }

    public void testDeleteOperations() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();

        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        final int initialDocCount = scaledRandomIntBetween(0, 200);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, nodeA, nodeB);

            final int additionalDocCount = scaledRandomIntBetween(0, 200);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);
            waitForSearchableDocs(expectedHitCount, nodeA, nodeB);

            ensureGreen(INDEX_NAME);

            Set<String> ids = indexer.getIds();
            String id = ids.toArray()[0].toString();
            client(nodeA).prepareDelete(INDEX_NAME, id).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

            refresh(INDEX_NAME);
            waitForSearchableDocs(expectedHitCount - 1, nodeA, nodeB);
            verifyStoreContent();
        }
    }

    public void testUpdateOperations() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellow(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 200);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, asList(primary, replica));

            final int additionalDocCount = scaledRandomIntBetween(0, 200);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);
            waitForSearchableDocs(expectedHitCount, asList(primary, replica));

            Set<String> ids = indexer.getIds();
            String id = ids.toArray()[0].toString();
            UpdateResponse updateResponse = client(primary).prepareUpdate(INDEX_NAME, id)
                .setDoc(Requests.INDEX_CONTENT_TYPE, "foo", "baz")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .get();
            assertFalse("request shouldn't have forced a refresh", updateResponse.forcedRefresh());
            assertEquals(2, updateResponse.getVersion());

            refresh(INDEX_NAME);

            verifyStoreContent();
            assertSearchHits(client(primary).prepareSearch(INDEX_NAME).setQuery(matchQuery("foo", "baz")).get(), id);
            assertSearchHits(client(replica).prepareSearch(INDEX_NAME).setQuery(matchQuery("foo", "baz")).get(), id);
        }
    }

    public void testDropPrimaryDuringReplication() throws Exception {
        final int replica_count = 6;
        final Settings settings = Settings.builder()
            .put(indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replica_count)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, settings);
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(6);
        ensureGreen(INDEX_NAME);

        int initialDocCount = scaledRandomIntBetween(100, 200);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            // don't wait for replication to complete, stop the primary immediately.
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
            ensureYellow(INDEX_NAME);

            // start another replica.
            dataNodes.add(internalCluster().startDataOnlyNode());
            ensureGreen(INDEX_NAME);

            // index another doc and refresh - without this the new replica won't catch up.
            String docId = String.valueOf(initialDocCount + 1);
            client().prepareIndex(INDEX_NAME).setId(docId).setSource("foo", "bar").get();

            flushAndRefresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount + 1, dataNodes);
            verifyStoreContent();
        }
    }
}

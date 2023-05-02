/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.replication.SegmentReplicationBaseIT;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.index.SegmentReplicationPressureService.MAX_INDEXING_CHECKPOINTS;
import static org.opensearch.index.SegmentReplicationPressureService.MAX_REPLICATION_TIME_SETTING;
import static org.opensearch.index.SegmentReplicationPressureService.SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationPressureIT extends SegmentReplicationBaseIT {

    private static final int MAX_CHECKPOINTS_BEHIND = 2;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(MAX_REPLICATION_TIME_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .put(MAX_INDEXING_CHECKPOINTS.getKey(), MAX_CHECKPOINTS_BEHIND)
            .build();
    }

    @Override
    public Settings indexSettings() {
        // we want to control refreshes
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.refresh_interval", -1)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return asList(MockTransportService.TestPlugin.class);
    }

    public void testWritesRejected() throws Exception {
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        final IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
        final List<String> replicaNodes = asList(replicaNode);
        assertEqualSegmentInfosVersion(replicaNodes, primaryShard);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger totalDocs = new AtomicInteger(0);
        try (final Releasable ignored = blockReplication(replicaNodes, latch)) {
            Thread indexingThread = new Thread(() -> { totalDocs.getAndSet(indexUntilCheckpointCount()); });
            indexingThread.start();
            indexingThread.join();
            latch.await();

            indexDoc();
            totalDocs.incrementAndGet();
            refresh(INDEX_NAME);
            // index again while we are stale.
            assertBusy(() -> {
                expectThrows(OpenSearchRejectedExecutionException.class, () -> {
                    indexDoc();
                    totalDocs.incrementAndGet();
                });
            });
            // Try to index one more doc.
            expectThrows(OpenSearchRejectedExecutionException.class, () -> {
                indexDoc();
                totalDocs.incrementAndGet();
                refresh(INDEX_NAME);
            });

            // Verify the rejected doc count.
            SegmentReplicationStatsResponse segmentReplicationStatsResponse = dataNodeClient().admin()
                .indices()
                .prepareSegmentReplicationStats(INDEX_NAME)
                .setDetailed(true)
                .execute()
                .actionGet();
            SegmentReplicationPerGroupStats perGroupStats = segmentReplicationStatsResponse.getReplicationStats().get(INDEX_NAME).get(0);

            assertEquals(perGroupStats.getRejectedRequestCount(), 2L);
        }
        refresh(INDEX_NAME);
        // wait for the replicas to catch up after block is released.
        waitForSearchableDocs(totalDocs.get(), replicaNodes.toArray(new String[] {}));

        // index another doc showing there is no pressure enforced.
        indexDoc();
        refresh(INDEX_NAME);
        waitForSearchableDocs(totalDocs.incrementAndGet(), replicaNodes.toArray(new String[] {}));
        verifyStoreContent();
    }

    /**
     * This test ensures that a replica can be added while the index is under write block.
     * Ensuring that only write requests are blocked.
     */
    public void testAddReplicaWhileWritesBlocked() throws Exception {
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        final IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
        final List<String> replicaNodes = new ArrayList<>();
        replicaNodes.add(replicaNode);
        assertEqualSegmentInfosVersion(replicaNodes, primaryShard);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger totalDocs = new AtomicInteger(0);
        try (final Releasable ignored = blockReplication(replicaNodes, latch)) {
            Thread indexingThread = new Thread(() -> { totalDocs.getAndSet(indexUntilCheckpointCount()); });
            indexingThread.start();
            indexingThread.join();
            latch.await();
            indexDoc();
            totalDocs.incrementAndGet();
            refresh(INDEX_NAME);
            // index again while we are stale.
            assertBusy(() -> {
                expectThrows(OpenSearchRejectedExecutionException.class, () -> {
                    indexDoc();
                    totalDocs.incrementAndGet();
                });
            });
            final String replica_2 = internalCluster().startNode();
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings(INDEX_NAME)
                    .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 2))
            );
            ensureGreen(INDEX_NAME);
            replicaNodes.add(replica_2);
            waitForSearchableDocs(totalDocs.get(), replica_2);
        }
        refresh(INDEX_NAME);
        // wait for the replicas to catch up after block is released.
        waitForSearchableDocs(totalDocs.get(), replicaNodes.toArray(new String[] {}));

        // index another doc showing there is no pressure enforced.
        indexDoc();
        refresh(INDEX_NAME);
        waitForSearchableDocs(totalDocs.incrementAndGet(), replicaNodes.toArray(new String[] {}));
        verifyStoreContent();
    }

    public void testBelowReplicaLimit() throws Exception {
        final Settings settings = Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 3).build();
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME, settings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        List<String> replicaNodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            replicaNodes.add(internalCluster().startNode());
        }
        ensureGreen(INDEX_NAME);

        final IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
        assertEqualSegmentInfosVersion(replicaNodes, primaryShard);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger totalDocs = new AtomicInteger(0);
        // only block a single replica, pressure should not get applied.
        try (final Releasable ignored = blockReplication(replicaNodes.subList(0, 1), latch)) {
            Thread indexingThread = new Thread(() -> totalDocs.getAndSet(indexUntilCheckpointCount()));
            indexingThread.start();
            indexingThread.join();
            latch.await();
            indexDoc();
            totalDocs.incrementAndGet();
            refresh(INDEX_NAME);
        }
        // index another doc showing there is no pressure enforced.
        indexDoc();
        refresh(INDEX_NAME);
        waitForSearchableDocs(totalDocs.incrementAndGet(), replicaNodes.toArray(new String[] {}));
        verifyStoreContent();
    }

    public void testFailStaleReplica() throws Exception {

        Settings settings = Settings.builder().put(MAX_REPLICATION_TIME_SETTING.getKey(), TimeValue.timeValueMillis(500)).build();
        // Starts a primary and replica node.
        final String primaryNode = internalCluster().startNode(settings);
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startNode(settings);
        ensureGreen(INDEX_NAME);

        final IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
        final List<String> replicaNodes = asList(replicaNode);
        assertEqualSegmentInfosVersion(replicaNodes, primaryShard);
        IndexShard replicaShard = getIndexShard(replicaNode, INDEX_NAME);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger totalDocs = new AtomicInteger(0);
        try (final Releasable ignored = blockReplication(replicaNodes, latch)) {
            // Index docs until replicas are staled.
            totalDocs.getAndSet(indexUntilCheckpointCount());
            latch.await();
            // index again while we are stale.
            indexDoc();
            refresh(INDEX_NAME);
            totalDocs.incrementAndGet();

            // Verify that replica shard is closed.
            assertBusy(() -> { assertTrue(replicaShard.state().equals(IndexShardState.CLOSED)); }, 1, TimeUnit.MINUTES);
        }
        ensureGreen(INDEX_NAME);
        final IndexShard replicaAfterFailure = getIndexShard(replicaNode, INDEX_NAME);

        // Verify that new replica shard after failure is different from old replica shard.
        assertNotEquals(replicaAfterFailure.routingEntry().allocationId().getId(), replicaShard.routingEntry().allocationId().getId());
    }

    public void testWithDocumentReplicationEnabledIndex() throws Exception {
        Settings settings = Settings.builder().put(MAX_REPLICATION_TIME_SETTING.getKey(), TimeValue.timeValueMillis(500)).build();
        // Starts a primary and replica node.
        final String primaryNode = internalCluster().startNode(settings);
        createIndex(
            INDEX_NAME,
            Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT).build()
        );
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startNode(settings);
        ensureGreen(INDEX_NAME);
        final AtomicInteger totalDocs = new AtomicInteger(0);
        // Index docs until replica stale limit is reached.
        totalDocs.getAndSet(indexUntilCheckpointCount());
        // index again after stale limit.
        indexDoc();
        refresh(INDEX_NAME);
        totalDocs.incrementAndGet();
        // verify total doc count is same and docs are not rejected.
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), totalDocs.get());
        assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), totalDocs.get());

    }

    public void testBulkWritesRejected() throws Exception {
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startNode();
        final String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        ensureGreen(INDEX_NAME);

        final IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
        final List<String> replicaNodes = asList(replicaNode);
        assertEqualSegmentInfosVersion(replicaNodes, primaryShard);

        final CountDownLatch latch = new CountDownLatch(1);
        List<String> nodes = List.of(primaryNode, replicaNode, coordinator);

        int docsPerBatch = randomIntBetween(1, 200);
        int totalDocs = docsPerBatch * MAX_CHECKPOINTS_BEHIND;
        try (final Releasable ignored = blockReplication(replicaNodes, latch)) {
            Thread indexingThread = new Thread(() -> {
                for (int i = 0; i < MAX_CHECKPOINTS_BEHIND + 1; i++) {
                    executeBulkRequest(nodes, docsPerBatch);
                    refresh(INDEX_NAME);
                }
            });
            indexingThread.start();
            indexingThread.join();
            latch.await();
            // try and index again while we are stale.
            assertBusy(() -> { assertFailedRequests(executeBulkRequest(nodes, randomIntBetween(1, 200))); });
        }
        refresh(INDEX_NAME);
        // wait for the replicas to catch up after block is released.
        waitForSearchableDocs(totalDocs, replicaNodes.toArray(new String[] {}));

        // index another doc showing there is no pressure enforced.
        executeBulkRequest(nodes, totalDocs);
        waitForSearchableDocs(totalDocs * 2L, replicaNodes.toArray(new String[] {}));
        verifyStoreContent();
    }

    private BulkResponse executeBulkRequest(List<String> nodes, int docsPerBatch) {
        final BulkRequest bulkRequest = new BulkRequest();
        for (int j = 0; j < docsPerBatch; ++j) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            bulkRequest.add(request);
        }
        final BulkResponse bulkItemResponses = client(randomFrom(nodes)).bulk(bulkRequest).actionGet();
        refresh(INDEX_NAME);
        return bulkItemResponses;
    }

    /**
     * Index and Refresh in batches to force checkpoints behind.
     * Asserts that there are no stale replicas according to the primary until cp count is reached.
     */
    private int indexUntilCheckpointCount() {
        int total = 0;
        for (int i = 0; i < MAX_CHECKPOINTS_BEHIND; i++) {
            final int numDocs = randomIntBetween(1, 100);
            for (int j = 0; j < numDocs; ++j) {
                indexDoc();
            }
            total += numDocs;
            refresh(INDEX_NAME);
        }
        return total;
    }

    private void assertFailedRequests(BulkResponse response) {
        assertTrue(Arrays.stream(response.getItems()).allMatch(BulkItemResponse::isFailed));
        assertTrue(
            Arrays.stream(response.getItems())
                .map(BulkItemResponse::getFailure)
                .allMatch((failure) -> failure.getStatus() == RestStatus.TOO_MANY_REQUESTS)
        );
    }

    private void indexDoc() {
        client().prepareIndex(INDEX_NAME).setId(UUIDs.base64UUID()).setSource("{}", "{}").execute().actionGet();
    }

    private void assertEqualSegmentInfosVersion(List<String> replicaNames, IndexShard primaryShard) {
        for (String replicaName : replicaNames) {
            final IndexShard replicaShard = getIndexShard(replicaName, INDEX_NAME);
            assertEquals(
                primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                replicaShard.getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );
        }
    }
}

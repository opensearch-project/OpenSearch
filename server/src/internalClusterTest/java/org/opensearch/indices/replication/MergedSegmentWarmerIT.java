/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.set.Sets;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.TieredMergePolicyProvider;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * This class runs Segment Replication Integ test suite with merged segment warmer enabled.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MergedSegmentWarmerIT extends SegmentReplicationIT {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(RecoverySettings.INDICES_MERGED_SEGMENT_REPLICATION_WARMER_ENABLED_SETTING.getKey(), true)
            .put(RecoverySettings.INDICES_REPLICATION_MERGES_WARMER_MIN_SEGMENT_SIZE_THRESHOLD_SETTING.getKey(), "1b")
            .build();
    }

    public void testPrimaryNodeRestart() throws Exception {
        logger.info("--> start nodes");
        internalCluster().startNode();

        logger.info("--> creating test index: {}", INDEX_NAME);
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put("number_of_shards", 1).put("number_of_replicas", 0).build());

        ensureGreen();

        logger.info("--> indexing sample data");
        final int numDocs = 100;
        final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex(INDEX_NAME)
                .setSource("foo-int", randomInt(), "foo-string", randomAlphaOfLength(32), "foo-float", randomFloat());
        }

        indexRandom(true, docs);
        flush();
        assertThat(client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value(), equalTo((long) numDocs));

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();
    }

    public void testMergeSegmentWarmer() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        final String replicaNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        waitForSearchableDocs(30, primaryNode, replicaNode);

        MockTransportService primaryTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNode
        ));

        primaryTransportService.addRequestHandlingBehavior(
            SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES,
            (handler, request, channel, task) -> {
                logger.info(
                    "replicationId {}, get segment files {}",
                    ((GetSegmentFilesRequest) request).getReplicationId(),
                    ((GetSegmentFilesRequest) request).getFilesToFetch().stream().map(StoreFileMetadata::name).collect(Collectors.toList())
                );
                // After the pre-copy merged segment is complete, the merged segment files is to reuse, so the files to fetch is empty.
                assertEquals(0, ((GetSegmentFilesRequest) request).getFilesToFetch().size());
                handler.messageReceived(request, channel, task);
            }
        );

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(2));

        waitForSegmentCount(INDEX_NAME, 2, logger);
        primaryTransportService.clearAllRules();
    }

    public void testConcurrentMergeSegmentWarmer() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), 5)
                .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), 5)
                .put(IndexSettings.INDEX_MERGE_ON_FLUSH_ENABLED.getKey(), false)
                .build()
        );
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        // ensure pre-copy merge segment concurrent execution
        AtomicInteger getMergeSegmentFilesActionCount = new AtomicInteger(0);
        MockTransportService primaryTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNode
        ));

        CountDownLatch blockFileCopy = new CountDownLatch(1);
        primaryTransportService.addRequestHandlingBehavior(
            SegmentReplicationSourceService.Actions.GET_MERGED_SEGMENT_FILES,
            (handler, request, channel, task) -> {
                logger.info(
                    "replicationId {}, get merge segment files {}",
                    ((GetSegmentFilesRequest) request).getReplicationId(),
                    ((GetSegmentFilesRequest) request).getFilesToFetch().stream().map(StoreFileMetadata::name).collect(Collectors.toList())
                );
                getMergeSegmentFilesActionCount.incrementAndGet();
                if (getMergeSegmentFilesActionCount.get() > 2) {
                    blockFileCopy.countDown();
                }
                handler.messageReceived(request, channel, task);
            }
        );

        primaryTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, replicaNode),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationTargetService.Actions.MERGED_SEGMENT_FILE_CHUNK)) {
                    try {
                        blockFileCopy.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                connection.sendRequest(requestId, action, request, options);
            }
        );

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(2));

        waitForSegmentCount(INDEX_NAME, 2, logger);
        primaryTransportService.clearAllRules();
    }

    public void testMergeSegmentWarmerWithInactiveReplica() throws Exception {
        internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(1)).get();
        final IndicesSegmentResponse response = client().admin().indices().prepareSegments(INDEX_NAME).get();
        assertEquals(1, response.getIndices().get(INDEX_NAME).getShards().values().size());
    }

    // Construct a case with redundant pending merge segments in replica shard, and finally delete these files
    public void testCleanupRedundantPendingMergeFile() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), 5)
                .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), 5)
                .put(IndexSettings.INDEX_MERGE_ON_FLUSH_ENABLED.getKey(), false)
                .build()
        );
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        AtomicBoolean forceMergeComplete = new AtomicBoolean(false);
        MockTransportService primaryTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNode
        ));

        primaryTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, replicaNode),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationTargetService.Actions.FILE_CHUNK)) {
                    if (forceMergeComplete.get() == false) {
                        logger.trace("mock connection exception");
                        throw new ConnectTransportException(connection.getNode(), "mock connection exception");
                    }

                }
                connection.sendRequest(requestId, action, request, options);
            }
        );

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        IndexShard replicaShard = getIndexShard(replicaNode, INDEX_NAME);
        assertBusy(() -> assertFalse(replicaShard.getPendingMergedSegmentCheckpoints().isEmpty()));

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(1)).get();
        forceMergeComplete.set(true);

        // Verify replica shard has pending merged segments
        assertBusy(() -> { assertFalse(replicaShard.getPendingMergedSegmentCheckpoints().isEmpty()); }, 1, TimeUnit.MINUTES);

        waitForSegmentCount(INDEX_NAME, 1, logger);
        primaryTransportService.clearAllRules();

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.INDEX_PUBLISH_REFERENCED_SEGMENTS_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                )
        );

        assertBusy(() -> {
            IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
            Directory primaryDirectory = primaryShard.store().directory();
            Set<String> primaryFiles = Sets.newHashSet(primaryDirectory.listAll());
            primaryFiles.removeIf(f -> f.startsWith("segment"));
            Directory replicaDirectory = replicaShard.store().directory();
            Set<String> replicaFiles = Sets.newHashSet(replicaDirectory.listAll());
            replicaFiles.removeIf(f -> f.startsWith("segment"));
            // Verify replica shard does not have pending merged segments
            assertEquals(0, replicaShard.getPendingMergedSegmentCheckpoints().size());
            // Verify that primary shard and replica shard have the same file list
            assertEquals(primaryFiles, replicaFiles);
        }, 1, TimeUnit.MINUTES);
    }

    public static void waitForSegmentCount(String indexName, int segmentCount, Logger logger) throws Exception {
        assertBusy(() -> {
            Set<String> primarySegments = Sets.newHashSet();
            Set<String> replicaSegments = Sets.newHashSet();
            final IndicesSegmentResponse response = client().admin().indices().prepareSegments(indexName).get();
            for (IndexShardSegments indexShardSegments : response.getIndices().get(indexName).getShards().values()) {
                for (ShardSegments shardSegment : indexShardSegments.getShards()) {
                    for (Segment segment : shardSegment.getSegments()) {
                        if (shardSegment.getShardRouting().primary()) {
                            primarySegments.add(segment.getName());
                        } else {
                            replicaSegments.add(segment.getName());
                        }
                    }
                }
            }
            logger.info("primary segments: {}, replica segments: {}", primarySegments, replicaSegments);
            assertEquals(segmentCount, primarySegments.size());
            assertEquals(segmentCount, replicaSegments.size());
        }, 1, TimeUnit.MINUTES);
    }
}

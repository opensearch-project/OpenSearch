/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.set.Sets;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.TieredMergePolicyProvider;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * This class runs Segment Replication Integ test suite with merged segment warmer enabled.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MergedSegmentWarmerIT extends SegmentReplicationIT {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.MERGED_SEGMENT_WARMER_EXPERIMENTAL_FLAG, true);
        return featureSettings.build();
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

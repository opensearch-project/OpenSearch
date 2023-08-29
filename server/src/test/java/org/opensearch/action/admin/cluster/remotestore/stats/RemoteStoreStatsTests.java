/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.compareStatsResponse;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createShardRouting;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForNewPrimary;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForNewReplica;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForRemoteStoreRestoredPrimary;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class RemoteStoreStatsTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_store_stats_test");
        shardId = new ShardId("index", "uuid", 0);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testXContentBuilderWithPrimaryShard() throws IOException {
        RemoteSegmentTransferTracker.Stats segmentStats = createStatsForNewPrimary(shardId);
        ShardRouting routing = createShardRouting(shardId, true);
        RemoteStoreStats stats = new RemoteStoreStats(segmentStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, segmentStats, routing);
    }

    public void testXContentBuilderWithReplicaShard() throws IOException {
        RemoteSegmentTransferTracker.Stats segmentStats = createStatsForNewReplica(shardId);
        ShardRouting routing = createShardRouting(shardId, false);
        RemoteStoreStats stats = new RemoteStoreStats(segmentStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, segmentStats, routing);
    }

    public void testXContentBuilderWithRemoteStoreRestoredShard() throws IOException {
        RemoteSegmentTransferTracker.Stats segmentStats = createStatsForRemoteStoreRestoredPrimary(shardId);
        ShardRouting routing = createShardRouting(shardId, true);
        RemoteStoreStats stats = new RemoteStoreStats(segmentStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, segmentStats, routing);
    }

    public void testSerializationForPrimaryShard() throws Exception {
        RemoteSegmentTransferTracker.Stats segmentStats = createStatsForNewPrimary(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(segmentStats, createShardRouting(shardId, true));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteStoreStats deserializedStats = new RemoteStoreStats(in);
                assertEquals(stats.getSegmentStats(), deserializedStats.getSegmentStats());
            }
        }
    }

    public void testSerializationForReplicaShard() throws Exception {
        RemoteSegmentTransferTracker.Stats replicaShardStats = createStatsForNewReplica(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(replicaShardStats, createShardRouting(shardId, false));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteStoreStats deserializedStats = new RemoteStoreStats(in);
                assertEquals(stats.getSegmentStats(), deserializedStats.getSegmentStats());
            }
        }
    }

    public void testSerializationForRemoteStoreRestoredPrimaryShard() throws Exception {
        RemoteSegmentTransferTracker.Stats primaryShardStats = createStatsForRemoteStoreRestoredPrimary(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(primaryShardStats, createShardRouting(shardId, true));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteStoreStats deserializedStats = new RemoteStoreStats(in);
                assertEquals(stats.getSegmentStats(), deserializedStats.getSegmentStats());
            }
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.compareStatsResponse;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForNewReplica;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createShardRouting;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForNewPrimary;
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
        RemoteSegmentTransferTracker.Stats uploadStats = createStatsForNewPrimary(shardId);
        ShardRouting routing = createShardRouting(shardId, true);
        RemoteStoreStats stats = new RemoteStoreStats(uploadStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, uploadStats, routing);
    }

    public void testXContentBuilderWithReplicaShard() throws IOException {
        RemoteSegmentTransferTracker.Stats downloadStats = createStatsForNewReplica(shardId);
        ShardRouting routing = createShardRouting(shardId, false);
        RemoteStoreStats stats = new RemoteStoreStats(downloadStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, downloadStats, routing);
    }

    public void testXContentBuilderWithRemoteStoreRestoredShard() throws IOException {
        RemoteSegmentTransferTracker.Stats remotestoreRestoredShardStats = createStatsForRemoteStoreRestoredPrimary(shardId);
        ShardRouting routing = createShardRouting(shardId, true);
        RemoteStoreStats stats = new RemoteStoreStats(remotestoreRestoredShardStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, remotestoreRestoredShardStats, routing);
    }

    public void testSerializationForPrimaryShard() throws Exception {
        RemoteSegmentTransferTracker.Stats primaryShardStats = createStatsForNewPrimary(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(primaryShardStats, createShardRouting(shardId, true));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteSegmentTransferTracker.Stats deserializedStats = new RemoteStoreStats(in).getStats();
                assertEquals(stats.getStats().refreshTimeLagMs, deserializedStats.refreshTimeLagMs);
                assertEquals(stats.getStats().localRefreshNumber, deserializedStats.localRefreshNumber);
                assertEquals(stats.getStats().remoteRefreshNumber, deserializedStats.remoteRefreshNumber);
                assertEquals(stats.getStats().uploadBytesStarted, deserializedStats.uploadBytesStarted);
                assertEquals(stats.getStats().uploadBytesSucceeded, deserializedStats.uploadBytesSucceeded);
                assertEquals(stats.getStats().uploadBytesFailed, deserializedStats.uploadBytesFailed);
                assertEquals(stats.getStats().totalUploadsStarted, deserializedStats.totalUploadsStarted);
                assertEquals(stats.getStats().totalUploadsFailed, deserializedStats.totalUploadsFailed);
                assertEquals(stats.getStats().totalUploadsSucceeded, deserializedStats.totalUploadsSucceeded);
                assertEquals(stats.getStats().rejectionCount, deserializedStats.rejectionCount);
                assertEquals(stats.getStats().consecutiveFailuresCount, deserializedStats.consecutiveFailuresCount);
                assertEquals(stats.getStats().uploadBytesMovingAverage, deserializedStats.uploadBytesMovingAverage, 0);
                assertEquals(stats.getStats().uploadBytesPerSecMovingAverage, deserializedStats.uploadBytesPerSecMovingAverage, 0);
                assertEquals(stats.getStats().uploadTimeMovingAverage, deserializedStats.uploadTimeMovingAverage, 0);
                assertEquals(stats.getStats().bytesLag, deserializedStats.bytesLag);
                assertEquals(0, deserializedStats.directoryFileTransferTrackerStats.transferredBytesStarted);
                assertEquals(0, deserializedStats.directoryFileTransferTrackerStats.transferredBytesFailed);
                assertEquals(0, deserializedStats.directoryFileTransferTrackerStats.transferredBytesSucceeded);
                assertEquals(0, deserializedStats.directoryFileTransferTrackerStats.lastSuccessfulTransferInBytes);
                assertEquals(0, deserializedStats.directoryFileTransferTrackerStats.lastTransferTimestampMs);
            }
        }
    }

    public void testSerializationForReplicaShard() throws Exception {
        RemoteSegmentTransferTracker.Stats replicaShardStats = createStatsForNewReplica(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(replicaShardStats, createShardRouting(shardId, false));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteSegmentTransferTracker.Stats deserializedStats = new RemoteStoreStats(in).getStats();
                assertEquals(0, deserializedStats.refreshTimeLagMs);
                assertEquals(0, deserializedStats.localRefreshNumber);
                assertEquals(0, deserializedStats.remoteRefreshNumber);
                assertEquals(0, deserializedStats.uploadBytesStarted);
                assertEquals(0, deserializedStats.uploadBytesSucceeded);
                assertEquals(0, deserializedStats.uploadBytesFailed);
                assertEquals(0, deserializedStats.totalUploadsStarted);
                assertEquals(0, deserializedStats.totalUploadsFailed);
                assertEquals(0, deserializedStats.totalUploadsSucceeded);
                assertEquals(0, deserializedStats.rejectionCount);
                assertEquals(0, deserializedStats.consecutiveFailuresCount);
                assertEquals(0, deserializedStats.bytesLag);
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.transferredBytesStarted,
                    deserializedStats.directoryFileTransferTrackerStats.transferredBytesStarted
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.transferredBytesFailed,
                    deserializedStats.directoryFileTransferTrackerStats.transferredBytesFailed
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.transferredBytesSucceeded,
                    deserializedStats.directoryFileTransferTrackerStats.transferredBytesSucceeded
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.lastSuccessfulTransferInBytes,
                    deserializedStats.directoryFileTransferTrackerStats.lastSuccessfulTransferInBytes
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.lastTransferTimestampMs,
                    deserializedStats.directoryFileTransferTrackerStats.lastTransferTimestampMs
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage,
                    deserializedStats.directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage,
                    0
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.transferredBytesMovingAverage,
                    deserializedStats.directoryFileTransferTrackerStats.transferredBytesMovingAverage,
                    0
                );
            }
        }
    }

    public void testSerializationForRemoteStoreRestoredPrimaryShard() throws Exception {
        RemoteSegmentTransferTracker.Stats primaryShardStats = createStatsForRemoteStoreRestoredPrimary(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(primaryShardStats, createShardRouting(shardId, true));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteSegmentTransferTracker.Stats deserializedStats = new RemoteStoreStats(in).getStats();
                assertEquals(stats.getStats().refreshTimeLagMs, deserializedStats.refreshTimeLagMs);
                assertEquals(stats.getStats().localRefreshNumber, deserializedStats.localRefreshNumber);
                assertEquals(stats.getStats().remoteRefreshNumber, deserializedStats.remoteRefreshNumber);
                assertEquals(stats.getStats().uploadBytesStarted, deserializedStats.uploadBytesStarted);
                assertEquals(stats.getStats().uploadBytesSucceeded, deserializedStats.uploadBytesSucceeded);
                assertEquals(stats.getStats().uploadBytesFailed, deserializedStats.uploadBytesFailed);
                assertEquals(stats.getStats().totalUploadsStarted, deserializedStats.totalUploadsStarted);
                assertEquals(stats.getStats().totalUploadsFailed, deserializedStats.totalUploadsFailed);
                assertEquals(stats.getStats().totalUploadsSucceeded, deserializedStats.totalUploadsSucceeded);
                assertEquals(stats.getStats().rejectionCount, deserializedStats.rejectionCount);
                assertEquals(stats.getStats().consecutiveFailuresCount, deserializedStats.consecutiveFailuresCount);
                assertEquals(stats.getStats().uploadBytesMovingAverage, deserializedStats.uploadBytesMovingAverage, 0);
                assertEquals(stats.getStats().uploadBytesPerSecMovingAverage, deserializedStats.uploadBytesPerSecMovingAverage, 0);
                assertEquals(stats.getStats().uploadTimeMovingAverage, deserializedStats.uploadTimeMovingAverage, 0);
                assertEquals(stats.getStats().bytesLag, deserializedStats.bytesLag);
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.transferredBytesStarted,
                    deserializedStats.directoryFileTransferTrackerStats.transferredBytesStarted
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.transferredBytesFailed,
                    deserializedStats.directoryFileTransferTrackerStats.transferredBytesFailed
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.transferredBytesSucceeded,
                    deserializedStats.directoryFileTransferTrackerStats.transferredBytesSucceeded
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.lastSuccessfulTransferInBytes,
                    deserializedStats.directoryFileTransferTrackerStats.lastSuccessfulTransferInBytes
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.lastTransferTimestampMs,
                    deserializedStats.directoryFileTransferTrackerStats.lastTransferTimestampMs
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage,
                    deserializedStats.directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage,
                    0
                );
                assertEquals(
                    stats.getStats().directoryFileTransferTrackerStats.transferredBytesMovingAverage,
                    deserializedStats.directoryFileTransferTrackerStats.transferredBytesMovingAverage,
                    0
                );
            }
        }
    }
}

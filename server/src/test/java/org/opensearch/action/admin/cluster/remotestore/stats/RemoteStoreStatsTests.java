/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.compareStatsResponse;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createPressureTrackerStats;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createShardRouting;
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
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = createPressureTrackerStats(shardId);
        ShardRouting routing = createShardRouting(shardId, true);
        RemoteStoreStats stats = new RemoteStoreStats(pressureTrackerStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, pressureTrackerStats, routing);
    }

    public void testXContentBuilderWithReplicaShard() throws IOException {
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = createPressureTrackerStats(shardId);
        ShardRouting routing = createShardRouting(shardId, false);
        RemoteStoreStats stats = new RemoteStoreStats(pressureTrackerStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, pressureTrackerStats, routing);
    }

    public void testSerialization() throws Exception {
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = createPressureTrackerStats(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(pressureTrackerStats, createShardRouting(shardId, true));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteRefreshSegmentTracker.Stats deserializedStats = new RemoteStoreStats(in).getStats();
                assertEquals(deserializedStats.shardId.toString(), stats.getStats().shardId.toString());
                assertEquals(deserializedStats.refreshTimeLagMs, stats.getStats().refreshTimeLagMs);
                assertEquals(deserializedStats.localRefreshNumber, stats.getStats().localRefreshNumber);
                assertEquals(deserializedStats.remoteRefreshNumber, stats.getStats().remoteRefreshNumber);
                assertEquals(deserializedStats.uploadBytesStarted, stats.getStats().uploadBytesStarted);
                assertEquals(deserializedStats.uploadBytesSucceeded, stats.getStats().uploadBytesSucceeded);
                assertEquals(deserializedStats.uploadBytesFailed, stats.getStats().uploadBytesFailed);
                assertEquals(deserializedStats.totalUploadsStarted, stats.getStats().totalUploadsStarted);
                assertEquals(deserializedStats.totalUploadsFailed, stats.getStats().totalUploadsFailed);
                assertEquals(deserializedStats.totalUploadsSucceeded, stats.getStats().totalUploadsSucceeded);
                assertEquals(deserializedStats.rejectionCount, stats.getStats().rejectionCount);
                assertEquals(deserializedStats.consecutiveFailuresCount, stats.getStats().consecutiveFailuresCount);
                assertEquals(deserializedStats.uploadBytesMovingAverage, stats.getStats().uploadBytesMovingAverage, 0);
                assertEquals(deserializedStats.uploadBytesPerSecMovingAverage, stats.getStats().uploadBytesPerSecMovingAverage, 0);
                assertEquals(deserializedStats.uploadTimeMovingAverage, stats.getStats().uploadTimeMovingAverage, 0);
                assertEquals(deserializedStats.bytesLag, stats.getStats().bytesLag);
                assertEquals(deserializedStats.totalDownloadsStarted, stats.getStats().totalDownloadsStarted);
                assertEquals(deserializedStats.totalDownloadsSucceeded, stats.getStats().totalDownloadsSucceeded);
                assertEquals(deserializedStats.totalDownloadsFailed, stats.getStats().totalDownloadsFailed);
                assertEquals(deserializedStats.downloadBytesStarted, stats.getStats().downloadBytesStarted);
                assertEquals(deserializedStats.downloadBytesFailed, stats.getStats().downloadBytesFailed);
                assertEquals(deserializedStats.downloadBytesSucceeded, stats.getStats().downloadBytesSucceeded);
                assertEquals(deserializedStats.lastSuccessfulSegmentDownloadBytes, stats.getStats().lastSuccessfulSegmentDownloadBytes);
                assertEquals(deserializedStats.lastDownloadTimestampMs, stats.getStats().lastDownloadTimestampMs);
                assertEquals(deserializedStats.downloadBytesMovingAverage, stats.getStats().downloadBytesMovingAverage, 0);
                assertEquals(deserializedStats.downloadBytesPerSecMovingAverage, stats.getStats().downloadBytesPerSecMovingAverage, 0);
                assertEquals(deserializedStats.downloadTimeMovingAverage, stats.getStats().downloadTimeMovingAverage, 0);
            }
        }
    }
}

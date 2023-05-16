/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

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

    public void testXContentBuilder() throws IOException {
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = createPressureTrackerStats();
        RemoteStoreStats stats = new RemoteStoreStats(pressureTrackerStats);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertEquals(jsonObject.get("shardId"), pressureTrackerStats.shardId.toString());
        assertEquals(jsonObject.get("local_refresh_timestamp_in_millis"), (int) pressureTrackerStats.localRefreshTimeMs);
        assertEquals(jsonObject.get("local_refresh_cumulative_count"), (int) pressureTrackerStats.localRefreshCount);
        assertEquals(jsonObject.get("remote_refresh_timestamp_in_millis"), (int) pressureTrackerStats.remoteRefreshTimeMs);
        assertEquals(jsonObject.get("remote_refresh_cumulative_count"), (int) pressureTrackerStats.remoteRefreshCount);
        assertEquals(jsonObject.get("bytes_lag"), (int) pressureTrackerStats.bytesLag);

        assertEquals(jsonObject.get("rejection_count"), (int) pressureTrackerStats.rejectionCount);
        assertEquals(jsonObject.get("consecutive_failure_count"), (int) pressureTrackerStats.consecutiveFailuresCount);

        assertEquals(((Map) jsonObject.get("total_uploads_in_bytes")).get("started"), (int) pressureTrackerStats.uploadBytesStarted);
        assertEquals(((Map) jsonObject.get("total_uploads_in_bytes")).get("succeeded"), (int) pressureTrackerStats.uploadBytesSucceeded);
        assertEquals(((Map) jsonObject.get("total_uploads_in_bytes")).get("failed"), (int) pressureTrackerStats.uploadBytesFailed);
        assertEquals(
            ((Map) jsonObject.get("remote_refresh_size_in_bytes")).get("moving_avg"),
            pressureTrackerStats.uploadBytesMovingAverage
        );
        assertEquals(
            ((Map) jsonObject.get("remote_refresh_size_in_bytes")).get("last_successful"),
            (int) pressureTrackerStats.lastSuccessfulRemoteRefreshBytes
        );
        assertEquals(
            ((Map) jsonObject.get("upload_latency_in_bytes_per_sec")).get("moving_avg"),
            pressureTrackerStats.uploadBytesPerSecMovingAverage
        );
        assertEquals(((Map) jsonObject.get("total_remote_refresh")).get("started"), (int) pressureTrackerStats.totalUploadsStarted);
        assertEquals(((Map) jsonObject.get("total_remote_refresh")).get("succeeded"), (int) pressureTrackerStats.totalUploadsSucceeded);
        assertEquals(((Map) jsonObject.get("total_remote_refresh")).get("failed"), (int) pressureTrackerStats.totalUploadsFailed);
        assertEquals(
            ((Map) jsonObject.get("remote_refresh_latency_in_nanos")).get("moving_avg"),
            pressureTrackerStats.uploadTimeMovingAverage
        );
    }

    public void testSerialization() throws Exception {
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = createPressureTrackerStats();
        RemoteStoreStats stats = new RemoteStoreStats(pressureTrackerStats);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteStoreStats deserializedStats = new RemoteStoreStats(in);
                assertEquals(deserializedStats.getStats().shardId.toString(), stats.getStats().shardId.toString());
                assertEquals(deserializedStats.getStats().localRefreshCount, stats.getStats().localRefreshCount);
                assertEquals(deserializedStats.getStats().localRefreshTimeMs, stats.getStats().localRefreshTimeMs);
                assertEquals(deserializedStats.getStats().remoteRefreshCount, stats.getStats().remoteRefreshCount);
                assertEquals(deserializedStats.getStats().remoteRefreshTimeMs, stats.getStats().remoteRefreshTimeMs);
                assertEquals(deserializedStats.getStats().uploadBytesStarted, stats.getStats().uploadBytesStarted);
                assertEquals(deserializedStats.getStats().uploadBytesSucceeded, stats.getStats().uploadBytesSucceeded);
                assertEquals(deserializedStats.getStats().uploadBytesFailed, stats.getStats().uploadBytesFailed);
                assertEquals(deserializedStats.getStats().totalUploadsStarted, stats.getStats().totalUploadsStarted);
                assertEquals(deserializedStats.getStats().totalUploadsFailed, stats.getStats().totalUploadsFailed);
                assertEquals(deserializedStats.getStats().totalUploadsSucceeded, stats.getStats().totalUploadsSucceeded);
                assertEquals(deserializedStats.getStats().rejectionCount, stats.getStats().rejectionCount);
                assertEquals(deserializedStats.getStats().consecutiveFailuresCount, stats.getStats().consecutiveFailuresCount);
                assertEquals(deserializedStats.getStats().uploadBytesMovingAverage, stats.getStats().uploadBytesMovingAverage, 0);
                assertEquals(
                    deserializedStats.getStats().uploadBytesPerSecMovingAverage,
                    stats.getStats().uploadBytesPerSecMovingAverage,
                    0
                );
                assertEquals(deserializedStats.getStats().uploadTimeMovingAverage, stats.getStats().uploadTimeMovingAverage, 0);
                assertEquals(deserializedStats.getStats().bytesLag, stats.getStats().bytesLag);
            }
        }
    }

    private RemoteRefreshSegmentTracker.Stats createPressureTrackerStats() {
        return new RemoteRefreshSegmentTracker.Stats(
            shardId,
            3,
            System.nanoTime() / 1_000_000L + randomIntBetween(10, 100),
            2,
            System.nanoTime() / 1_000_000L + randomIntBetween(10, 100),
            10,
            5,
            5,
            10,
            5,
            5,
            3,
            2,
            5,
            2,
            3,
            4,
            9
        );
    }
}

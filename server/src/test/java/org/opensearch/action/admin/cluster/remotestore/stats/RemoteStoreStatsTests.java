/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.compareStatsResponse;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createPressureTrackerStats;
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
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = createPressureTrackerStats(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(pressureTrackerStats);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, pressureTrackerStats);
    }

    public void testSerialization() throws Exception {
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = createPressureTrackerStats(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(pressureTrackerStats);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteStoreStats deserializedStats = new RemoteStoreStats(in);
                assertEquals(deserializedStats.getStats().shardId.toString(), stats.getStats().shardId.toString());
                assertEquals(deserializedStats.getStats().refreshTimeLagMs, stats.getStats().refreshTimeLagMs);
                assertEquals(deserializedStats.getStats().localRefreshNumber, stats.getStats().localRefreshNumber);
                assertEquals(deserializedStats.getStats().remoteRefreshNumber, stats.getStats().remoteRefreshNumber);
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
}

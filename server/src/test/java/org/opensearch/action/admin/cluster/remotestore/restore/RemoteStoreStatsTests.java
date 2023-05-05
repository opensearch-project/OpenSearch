/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.restore;

import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteRefreshSegmentPressureService;
import org.opensearch.index.remote.RemoteRefreshSegmentPressureSettings;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class RemoteStoreStatsTests extends OpenSearchTestCase {
    private RemoteRefreshSegmentPressureSettings pressureSettings;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_store_stats_test");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        pressureSettings = new RemoteRefreshSegmentPressureSettings(
            clusterService,
            Settings.EMPTY,
            mock(RemoteRefreshSegmentPressureService.class)
        );
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
        assertEquals(jsonObject.get("latest_upload_files_count"), (int) pressureTrackerStats.latestUploadFilesCount);
        assertEquals(jsonObject.get("latest_local_files_count"), (int) pressureTrackerStats.latestLocalFilesCount);
        assertEquals(jsonObject.get("local_refresh_time"), (int) pressureTrackerStats.localRefreshTime);
        assertEquals(jsonObject.get("local_refresh_seq_no"), (int) pressureTrackerStats.localRefreshSeqNo);
        assertEquals(jsonObject.get("remote_refresh_time"), (int) pressureTrackerStats.remoteRefreshTime);
        assertEquals(jsonObject.get("remote_refresh_seqno"), (int) pressureTrackerStats.remoteRefreshSeqNo);
        assertEquals(jsonObject.get("bytes_lag"), (int) pressureTrackerStats.bytesLag);
        assertEquals(jsonObject.get("inflight_upload_bytes"), (int) pressureTrackerStats.inflightUploadBytes);
        assertEquals(jsonObject.get("inflight_uploads"), (int) pressureTrackerStats.inflightUploads);
        assertEquals(jsonObject.get("rejection_count"), (int) pressureTrackerStats.rejectionCount);
        assertEquals(jsonObject.get("consecutive_failure_count"), (int) pressureTrackerStats.consecutiveFailuresCount);
        assertEquals(((Map) jsonObject.get("last_upload_time")).get("started"), -1);
        assertEquals(((Map) jsonObject.get("last_upload_time")).get("succeeded"), -1);
        assertEquals(((Map) jsonObject.get("last_upload_time")).get("failed"), -1);
        assertEquals(((Map) jsonObject.get("upload_bytes")).get("started"), (int) pressureTrackerStats.uploadBytesStarted);
        assertEquals(((Map) jsonObject.get("upload_bytes")).get("succeeded"), (int) pressureTrackerStats.uploadBytesSucceeded);
        assertEquals(((Map) jsonObject.get("upload_bytes")).get("failed"), (int) pressureTrackerStats.uploadBytesFailed);
        assertEquals(
            ((Map) ((Map) jsonObject.get("upload_bytes")).get("moving_avg")).get("started"),
            pressureTrackerStats.uploadBytesMovingAverage
        );
        assertEquals(
            ((Map) ((Map) jsonObject.get("upload_bytes")).get("moving_avg")).get("succeeded"),
            pressureTrackerStats.uploadBytesMovingAverage
        );
        assertEquals(((Map) ((Map) jsonObject.get("upload_bytes")).get("moving_avg")).get("failed"), -1);
        assertEquals(
            ((Map) ((Map) jsonObject.get("upload_bytes")).get("per_sec_moving_avg")).get("started"),
            pressureTrackerStats.uploadBytesPerSecMovingAverage
        );
        assertEquals(
            ((Map) ((Map) jsonObject.get("upload_bytes")).get("per_sec_moving_avg")).get("succeeded"),
            pressureTrackerStats.uploadBytesPerSecMovingAverage
        );
        assertEquals(((Map) ((Map) jsonObject.get("upload_bytes")).get("per_sec_moving_avg")).get("failed"), -1);
        assertEquals(((Map) jsonObject.get("total_uploads")).get("started"), (int) pressureTrackerStats.totalUploadsStarted);
        assertEquals(((Map) jsonObject.get("total_uploads")).get("succeeded"), (int) pressureTrackerStats.totalUploadsSucceeded);
        assertEquals(((Map) jsonObject.get("total_uploads")).get("failed"), (int) pressureTrackerStats.totalUploadsFailed);
        assertEquals(((Map) jsonObject.get("total_deletes")).get("started"), -1);
        assertEquals(((Map) jsonObject.get("total_deletes")).get("succeeded"), -1);
        assertEquals(((Map) jsonObject.get("total_deletes")).get("failed"), -1);
        assertEquals(((Map) jsonObject.get("upload_latency")).get("avg"), -1);
        assertEquals(((Map) jsonObject.get("upload_latency")).get("moving_avg"), pressureTrackerStats.uploadTimeMovingAverage);
        assertEquals(((Map) jsonObject.get("upload_latency")).get("max"), -1);
        assertEquals(((Map) jsonObject.get("upload_latency")).get("min"), -1);
        assertEquals(((Map) jsonObject.get("upload_latency")).get("p90"), -1);
        assertEquals(((Map) jsonObject.get("delete_latency")).get("avg"), -1);
        assertEquals(((Map) jsonObject.get("delete_latency")).get("moving_avg"), -1);
        assertEquals(((Map) jsonObject.get("delete_latency")).get("max"), -1);
        assertEquals(((Map) jsonObject.get("delete_latency")).get("min"), -1);
        assertEquals(((Map) jsonObject.get("delete_latency")).get("p90"), -1);
        assertEquals(((Map) jsonObject.get("latest_segments_filesize")).get("avg"), (int) pressureTrackerStats.latestLocalFileSizeAvg);
        assertEquals(((Map) jsonObject.get("latest_segments_filesize")).get("max"), (int) pressureTrackerStats.latestLocalFileSizeMax);
        assertEquals(((Map) jsonObject.get("latest_segments_filesize")).get("min"), (int) pressureTrackerStats.latestLocalFileSizeMin);
        assertEquals(((Map) jsonObject.get("latest_segments_filesize")).get("p90"), -1);
    }

    public void testSerialization() throws Exception {
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = createPressureTrackerStats();
        RemoteStoreStats stats = new RemoteStoreStats(pressureTrackerStats);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteStoreStats deserializedStats = new RemoteStoreStats(in);
                assertEquals(deserializedStats.getStats().shardId.toString(), stats.getStats().shardId.toString());
                assertEquals(deserializedStats.getStats().latestLocalFilesCount, stats.getStats().latestLocalFilesCount);
                assertEquals(deserializedStats.getStats().latestUploadFilesCount, stats.getStats().latestUploadFilesCount);
                assertEquals(deserializedStats.getStats().localRefreshSeqNo, stats.getStats().localRefreshSeqNo);
                assertEquals(deserializedStats.getStats().localRefreshTime, stats.getStats().localRefreshTime);
                assertEquals(deserializedStats.getStats().remoteRefreshSeqNo, stats.getStats().remoteRefreshSeqNo);
                assertEquals(deserializedStats.getStats().remoteRefreshTime, stats.getStats().remoteRefreshTime);
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
                assertEquals(deserializedStats.getStats().inflightUploads, stats.getStats().inflightUploads);
                assertEquals(deserializedStats.getStats().inflightUploadBytes, stats.getStats().inflightUploadBytes);
                assertEquals(deserializedStats.getStats().latestLocalFileSizeAvg, stats.getStats().latestLocalFileSizeAvg);
                assertEquals(deserializedStats.getStats().latestLocalFileSizeMax, stats.getStats().latestLocalFileSizeMax);
                assertEquals(deserializedStats.getStats().latestLocalFileSizeMin, stats.getStats().latestLocalFileSizeMin);
            }
        }
    }

    private RemoteRefreshSegmentTracker.Stats createPressureTrackerStats() {
        return new RemoteRefreshSegmentTracker.Stats(
            shardId,
            3,
            2,
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
            2,
            3,
            4,
            9,
            3,
            8,
            100,
            120,
            80
        );
    }
}

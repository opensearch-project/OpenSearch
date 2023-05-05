/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.restore;

import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
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

import java.util.ArrayList;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class RemoteStoreStatsResponseTests extends OpenSearchTestCase {
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

    public void testSerialization() throws Exception {
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = createPressureTrackerStats();
        RemoteStoreStats stats = new RemoteStoreStats(pressureTrackerStats);
        RemoteStoreStatsResponse statsResponse = new RemoteStoreStatsResponse(
            new RemoteStoreStats[] { stats },
            1,
            1,
            0,
            new ArrayList<DefaultShardOperationFailedException>()
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        statsResponse.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonResponseObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();

        ArrayList<Map<String, Object>> statsObjectArray = (ArrayList<Map<String, Object>>) jsonResponseObject.get("stats");
        assertEquals(statsObjectArray.size(), 1);
        Map<String, Object> statsObject = statsObjectArray.get(0);
        Map<String, Object> shardsObject = (Map<String, Object>) jsonResponseObject.get("_shards");

        assertEquals(shardsObject.get("total"), 1);
        assertEquals(shardsObject.get("successful"), 1);
        assertEquals(shardsObject.get("failed"), 0);
        assertEquals(statsObject.get("shardId"), pressureTrackerStats.shardId.toString());
        assertEquals(statsObject.get("latest_upload_files_count"), (int) pressureTrackerStats.latestUploadFilesCount);
        assertEquals(statsObject.get("latest_local_files_count"), (int) pressureTrackerStats.latestLocalFilesCount);
        assertEquals(statsObject.get("local_refresh_time"), (int) pressureTrackerStats.localRefreshTime);
        assertEquals(statsObject.get("local_refresh_seq_no"), (int) pressureTrackerStats.localRefreshSeqNo);
        assertEquals(statsObject.get("remote_refresh_time"), (int) pressureTrackerStats.remoteRefreshTime);
        assertEquals(statsObject.get("remote_refresh_seqno"), (int) pressureTrackerStats.remoteRefreshSeqNo);
        assertEquals(statsObject.get("bytes_lag"), (int) pressureTrackerStats.bytesLag);
        assertEquals(statsObject.get("inflight_upload_bytes"), (int) pressureTrackerStats.inflightUploadBytes);
        assertEquals(statsObject.get("inflight_uploads"), (int) pressureTrackerStats.inflightUploads);
        assertEquals(statsObject.get("rejection_count"), (int) pressureTrackerStats.rejectionCount);
        assertEquals(statsObject.get("consecutive_failure_count"), (int) pressureTrackerStats.consecutiveFailuresCount);
        assertEquals(((Map) statsObject.get("last_upload_time")).get("started"), -1);
        assertEquals(((Map) statsObject.get("last_upload_time")).get("succeeded"), -1);
        assertEquals(((Map) statsObject.get("last_upload_time")).get("failed"), -1);
        assertEquals(((Map) statsObject.get("upload_bytes")).get("started"), (int) pressureTrackerStats.uploadBytesStarted);
        assertEquals(((Map) statsObject.get("upload_bytes")).get("succeeded"), (int) pressureTrackerStats.uploadBytesSucceeded);
        assertEquals(((Map) statsObject.get("upload_bytes")).get("failed"), (int) pressureTrackerStats.uploadBytesFailed);
        assertEquals(
            ((Map) ((Map) statsObject.get("upload_bytes")).get("moving_avg")).get("started"),
            pressureTrackerStats.uploadBytesMovingAverage
        );
        assertEquals(
            ((Map) ((Map) statsObject.get("upload_bytes")).get("moving_avg")).get("succeeded"),
            pressureTrackerStats.uploadBytesMovingAverage
        );
        assertEquals(((Map) ((Map) statsObject.get("upload_bytes")).get("moving_avg")).get("failed"), -1);
        assertEquals(
            ((Map) ((Map) statsObject.get("upload_bytes")).get("per_sec_moving_avg")).get("started"),
            pressureTrackerStats.uploadBytesPerSecMovingAverage
        );
        assertEquals(
            ((Map) ((Map) statsObject.get("upload_bytes")).get("per_sec_moving_avg")).get("succeeded"),
            pressureTrackerStats.uploadBytesPerSecMovingAverage
        );
        assertEquals(((Map) ((Map) statsObject.get("upload_bytes")).get("per_sec_moving_avg")).get("failed"), -1);
        assertEquals(((Map) statsObject.get("total_uploads")).get("started"), (int) pressureTrackerStats.totalUploadsStarted);
        assertEquals(((Map) statsObject.get("total_uploads")).get("succeeded"), (int) pressureTrackerStats.totalUploadsSucceeded);
        assertEquals(((Map) statsObject.get("total_uploads")).get("failed"), (int) pressureTrackerStats.totalUploadsFailed);
        assertEquals(((Map) statsObject.get("total_deletes")).get("started"), -1);
        assertEquals(((Map) statsObject.get("total_deletes")).get("succeeded"), -1);
        assertEquals(((Map) statsObject.get("total_deletes")).get("failed"), -1);
        assertEquals(((Map) statsObject.get("upload_latency")).get("avg"), -1);
        assertEquals(((Map) statsObject.get("upload_latency")).get("moving_avg"), pressureTrackerStats.uploadTimeMovingAverage);
        assertEquals(((Map) statsObject.get("upload_latency")).get("max"), -1);
        assertEquals(((Map) statsObject.get("upload_latency")).get("min"), -1);
        assertEquals(((Map) statsObject.get("upload_latency")).get("p90"), -1);
        assertEquals(((Map) statsObject.get("delete_latency")).get("avg"), -1);
        assertEquals(((Map) statsObject.get("delete_latency")).get("moving_avg"), -1);
        assertEquals(((Map) statsObject.get("delete_latency")).get("max"), -1);
        assertEquals(((Map) statsObject.get("delete_latency")).get("min"), -1);
        assertEquals(((Map) statsObject.get("delete_latency")).get("p90"), -1);
        assertEquals(((Map) statsObject.get("latest_segments_filesize")).get("avg"), (int) pressureTrackerStats.latestLocalFileSizeAvg);
        assertEquals(((Map) statsObject.get("latest_segments_filesize")).get("max"), (int) pressureTrackerStats.latestLocalFileSizeMax);
        assertEquals(((Map) statsObject.get("latest_segments_filesize")).get("min"), (int) pressureTrackerStats.latestLocalFileSizeMin);
        assertEquals(((Map) statsObject.get("latest_segments_filesize")).get("p90"), -1);
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

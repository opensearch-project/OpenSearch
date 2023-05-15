/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

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
        assertEquals(statsObject.get("latest_remote_refresh_files_count"), (int) pressureTrackerStats.latestRemoteRefreshFilesCount);
        assertEquals(statsObject.get("latest_local_refresh_files_count"), (int) pressureTrackerStats.latestLocalRefreshFilesCount);
        assertEquals(statsObject.get("local_refresh_timestamp_in_millis"), (int) pressureTrackerStats.localRefreshTimeMs);
        assertEquals(statsObject.get("local_refresh_cumulative_count"), (int) pressureTrackerStats.localRefreshCount);
        assertEquals(statsObject.get("remote_refresh_timestamp_in_millis"), (int) pressureTrackerStats.remoteRefreshTimeMs);
        assertEquals(statsObject.get("remote_refresh_cumulative_count"), (int) pressureTrackerStats.remoteRefreshCount);
        assertEquals(statsObject.get("bytes_lag"), (int) pressureTrackerStats.bytesLag);
        assertEquals(statsObject.get("inflight_upload_bytes"), (int) pressureTrackerStats.inflightUploadBytes);
        assertEquals(statsObject.get("inflight_remote_refreshes"), (int) pressureTrackerStats.inflightUploads);
        assertEquals(statsObject.get("rejection_count"), (int) pressureTrackerStats.rejectionCount);
        assertEquals(statsObject.get("consecutive_failure_count"), (int) pressureTrackerStats.consecutiveFailuresCount);
        assertEquals(((Map) statsObject.get("total_upload_in_bytes")).get("started"), (int) pressureTrackerStats.uploadBytesStarted);
        assertEquals(((Map) statsObject.get("total_upload_in_bytes")).get("succeeded"), (int) pressureTrackerStats.uploadBytesSucceeded);
        assertEquals(((Map) statsObject.get("total_upload_in_bytes")).get("failed"), (int) pressureTrackerStats.uploadBytesFailed);
        assertEquals(
            ((Map) ((Map) statsObject.get("total_upload_in_bytes")).get("moving_avg")).get("started"),
            pressureTrackerStats.uploadBytesMovingAverage
        );
        assertEquals(
            ((Map) ((Map) statsObject.get("upload_speed_in_bytes_per_sec")).get("moving_avg")).get("started"),
            pressureTrackerStats.uploadBytesPerSecMovingAverage
        );
        assertEquals(((Map) statsObject.get("total_remote_refresh")).get("started"), (int) pressureTrackerStats.totalUploadsStarted);
        assertEquals(((Map) statsObject.get("total_remote_refresh")).get("succeeded"), (int) pressureTrackerStats.totalUploadsSucceeded);
        assertEquals(((Map) statsObject.get("total_remote_refresh")).get("failed"), (int) pressureTrackerStats.totalUploadsFailed);
        assertEquals(((Map) statsObject.get("remote_refresh_latency")).get("moving_avg"), pressureTrackerStats.uploadTimeMovingAverage);
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
            8
        );
    }

}

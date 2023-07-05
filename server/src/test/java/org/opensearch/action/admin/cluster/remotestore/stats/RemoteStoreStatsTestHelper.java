/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.shard.ShardId;

import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.opensearch.test.OpenSearchTestCase.assertEquals;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;

/**
 * Helper utilities for Remote Store stats tests
 */
public class RemoteStoreStatsTestHelper {
    static RemoteRefreshSegmentTracker.Stats createPressureTrackerStats(ShardId shardId) {
        return new RemoteRefreshSegmentTracker.Stats(
            shardId,
            101,
            102,
            100,
            3,
            10,
            2,
            10,
            5,
            5,
            10,
            10,
            5,
            5,
            5,
            5,
            5,
            5,
            5,
            3,
            2,
            5,
            2,
            3,
            4,
            9,
            3,
            4,
            9,
            5
        );
    }

    static ShardRouting createShardRouting(ShardId shardId, boolean isPrimary) {
        return TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(4), isPrimary, ShardRoutingState.STARTED);
    }

    static void compareStatsResponse(
        Map<String, Object> statsObject,
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats,
        ShardRouting routing
    ) {
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.ROUTING)).get(RemoteStoreStats.RoutingFields.NODE_ID),
            routing.currentNodeId()
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.ROUTING)).get(RemoteStoreStats.RoutingFields.STATE),
            routing.state().toString()
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.ROUTING)).get(RemoteStoreStats.RoutingFields.PRIMARY),
            routing.primary()
        );

        Map<String, Object> segment = ((Map) statsObject.get(RemoteStoreStats.Fields.SEGMENT));
        Map<String, Object> segmentDownloads = ((Map) segment.get(RemoteStoreStats.SubFields.DOWNLOAD));
        Map<String, Object> segmentUploads = ((Map) segment.get(RemoteStoreStats.SubFields.UPLOAD));

        if (!routing.primary()) {
            assertEquals(
                segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.LAST_DOWNLOAD_TIMESTAMP),
                (int) pressureTrackerStats.lastDownloadTimestampMs
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_FILE_DOWNLOADS)).get(
                    RemoteStoreStats.SubFields.STARTED
                ),
                (int) pressureTrackerStats.totalDownloadsStarted
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_FILE_DOWNLOADS)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ),
                (int) pressureTrackerStats.totalDownloadsSucceeded
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_FILE_DOWNLOADS)).get(
                    RemoteStoreStats.SubFields.FAILED
                ),
                (int) pressureTrackerStats.totalDownloadsFailed
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_FILE_DOWNLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.STARTED
                ),
                (int) pressureTrackerStats.downloadBytesStarted
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_FILE_DOWNLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ),
                (int) pressureTrackerStats.downloadBytesSucceeded
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_FILE_DOWNLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.FAILED
                ),
                (int) pressureTrackerStats.downloadBytesFailed
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.LAST_SUCCESSFUL
                ),
                (int) pressureTrackerStats.lastSuccessfulSegmentDownloadBytes
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerStats.downloadBytesMovingAverage
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SPEED_IN_BYTES_PER_SEC)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerStats.downloadBytesPerSecMovingAverage
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_LATENCY_IN_MILLIS)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerStats.downloadTimeMovingAverage
            );
            assertTrue(segmentUploads.isEmpty());
        } else {
            assertTrue(segmentDownloads.isEmpty());
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.LOCAL_REFRESH_TIMESTAMP),
                (int) pressureTrackerStats.localRefreshClockTimeMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_TIMESTAMP),
                (int) pressureTrackerStats.remoteRefreshClockTimeMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REFRESH_TIME_LAG_IN_MILLIS),
                (int) pressureTrackerStats.refreshTimeLagMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REFRESH_LAG),
                (int) (pressureTrackerStats.localRefreshNumber - pressureTrackerStats.remoteRefreshNumber)
            );
            assertEquals(segmentUploads.get(RemoteStoreStats.UploadStatsFields.BYTES_LAG), (int) pressureTrackerStats.bytesLag);

            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.BACKPRESSURE_REJECTION_COUNT),
                (int) pressureTrackerStats.rejectionCount
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.CONSECUTIVE_FAILURE_COUNT),
                (int) pressureTrackerStats.consecutiveFailuresCount
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.STARTED
                ),
                (int) pressureTrackerStats.uploadBytesStarted
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ),
                (int) pressureTrackerStats.uploadBytesSucceeded
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.FAILED
                ),
                (int) pressureTrackerStats.uploadBytesFailed
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerStats.uploadBytesMovingAverage
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.LAST_SUCCESSFUL
                ),
                (int) pressureTrackerStats.lastSuccessfulRemoteRefreshBytes
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.UPLOAD_LATENCY_IN_BYTES_PER_SEC)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerStats.uploadBytesPerSecMovingAverage
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.SubFields.STARTED),
                (int) pressureTrackerStats.totalUploadsStarted
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_REMOTE_REFRESH)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ),
                (int) pressureTrackerStats.totalUploadsSucceeded
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.SubFields.FAILED),
                (int) pressureTrackerStats.totalUploadsFailed
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_LATENCY_IN_MILLIS)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerStats.uploadTimeMovingAverage
            );
        }
    }
}

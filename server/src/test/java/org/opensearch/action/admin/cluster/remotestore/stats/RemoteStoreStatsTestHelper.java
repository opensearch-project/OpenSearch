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
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.DirectoryFileTransferTracker;

import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.opensearch.test.OpenSearchTestCase.assertEquals;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;

/**
 * Helper utilities for Remote Store stats tests
 */
public class RemoteStoreStatsTestHelper {
    static RemoteSegmentTransferTracker.Stats createStatsForNewPrimary(ShardId shardId) {
        return new RemoteSegmentTransferTracker.Stats(
            shardId,
            101,
            102,
            100,
            0,
            10,
            2,
            10,
            5,
            5,
            0,
            0,
            0,
            5,
            5,
            5,
            0,
            0,
            0,
            createZeroDirectoryFileTransferStats()
        );
    }

    static RemoteSegmentTransferTracker.Stats createStatsForNewReplica(ShardId shardId) {
        return new RemoteSegmentTransferTracker.Stats(
            shardId,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            createSampleDirectoryFileTransferStats()
        );
    }

    static RemoteSegmentTransferTracker.Stats createStatsForRemoteStoreRestoredPrimary(ShardId shardId) {
        return new RemoteSegmentTransferTracker.Stats(
            shardId,
            50,
            50,
            0,
            50,
            11,
            11,
            10,
            10,
            0,
            10,
            10,
            0,
            10,
            10,
            0,
            0,
            0,
            100,
            createSampleDirectoryFileTransferStats()
        );
    }

    static DirectoryFileTransferTracker.Stats createSampleDirectoryFileTransferStats() {
        return new DirectoryFileTransferTracker.Stats(10, 0, 10, 12345, 5, 5, 5);
    }

    static DirectoryFileTransferTracker.Stats createZeroDirectoryFileTransferStats() {
        return new DirectoryFileTransferTracker.Stats(0, 0, 0, 0, 0, 0, 0);
    }

    static ShardRouting createShardRouting(ShardId shardId, boolean isPrimary) {
        return TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(4), isPrimary, ShardRoutingState.STARTED);
    }

    static void compareStatsResponse(
        Map<String, Object> statsObject,
        RemoteSegmentTransferTracker.Stats statsTracker,
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

        if (statsTracker.directoryFileTransferTrackerStats.transferredBytesStarted != 0) {
            assertEquals(
                segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.LAST_SYNC_TIMESTAMP),
                (int) statsTracker.directoryFileTransferTrackerStats.lastTransferTimestampMs
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.STARTED
                ),
                (int) statsTracker.directoryFileTransferTrackerStats.transferredBytesStarted
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ),
                (int) statsTracker.directoryFileTransferTrackerStats.transferredBytesSucceeded
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.FAILED
                ),
                (int) statsTracker.directoryFileTransferTrackerStats.transferredBytesFailed
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.LAST_SUCCESSFUL
                ),
                (int) statsTracker.directoryFileTransferTrackerStats.lastSuccessfulTransferInBytes
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                statsTracker.directoryFileTransferTrackerStats.transferredBytesMovingAverage
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SPEED_IN_BYTES_PER_SEC)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                statsTracker.directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage
            );
        } else {
            assertTrue(segmentDownloads.isEmpty());
        }

        if (statsTracker.totalUploadsStarted != 0) {
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.LOCAL_REFRESH_TIMESTAMP),
                (int) statsTracker.localRefreshClockTimeMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_TIMESTAMP),
                (int) statsTracker.remoteRefreshClockTimeMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REFRESH_TIME_LAG_IN_MILLIS),
                (int) statsTracker.refreshTimeLagMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REFRESH_LAG),
                (int) (statsTracker.localRefreshNumber - statsTracker.remoteRefreshNumber)
            );
            assertEquals(segmentUploads.get(RemoteStoreStats.UploadStatsFields.BYTES_LAG), (int) statsTracker.bytesLag);

            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.BACKPRESSURE_REJECTION_COUNT),
                (int) statsTracker.rejectionCount
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.CONSECUTIVE_FAILURE_COUNT),
                (int) statsTracker.consecutiveFailuresCount
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.STARTED
                ),
                (int) statsTracker.uploadBytesStarted
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ),
                (int) statsTracker.uploadBytesSucceeded
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.FAILED
                ),
                (int) statsTracker.uploadBytesFailed
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                statsTracker.uploadBytesMovingAverage
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.LAST_SUCCESSFUL
                ),
                (int) statsTracker.lastSuccessfulRemoteRefreshBytes
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.UPLOAD_LATENCY_IN_BYTES_PER_SEC)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                statsTracker.uploadBytesPerSecMovingAverage
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_SYNCS_TO_REMOTE)).get(
                    RemoteStoreStats.SubFields.STARTED
                ),
                (int) statsTracker.totalUploadsStarted
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_SYNCS_TO_REMOTE)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ),
                (int) statsTracker.totalUploadsSucceeded
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_SYNCS_TO_REMOTE)).get(RemoteStoreStats.SubFields.FAILED),
                (int) statsTracker.totalUploadsFailed
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_LATENCY_IN_MILLIS)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                statsTracker.uploadTimeMovingAverage
            );
        } else {
            assertTrue(segmentUploads.isEmpty());
        }
    }
}

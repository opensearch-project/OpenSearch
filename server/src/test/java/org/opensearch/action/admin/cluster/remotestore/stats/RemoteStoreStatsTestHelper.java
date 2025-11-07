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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.store.DirectoryFileTransferTracker;

import java.util.Map;

import static org.opensearch.test.OpenSearchTestCase.assertEquals;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Helper utilities for Remote Store stats tests
 */
public class RemoteStoreStatsTestHelper {
    static RemoteSegmentTransferTracker.Stats createStatsForNewPrimary(ShardId shardId) {
        return new RemoteSegmentTransferTracker.Stats.Builder().shardId(shardId)
            .localRefreshClockTimeMs(101)
            .remoteRefreshClockTimeMs(102)
            .refreshTimeLagMs(100)
            .localRefreshNumber(0)
            .remoteRefreshNumber(10)
            .uploadBytesStarted(2)
            .uploadBytesSucceeded(10)
            .uploadBytesFailed(5)
            .totalUploadsStarted(5)
            .totalUploadsSucceeded(0)
            .totalUploadsFailed(0)
            .rejectionCount(0)
            .consecutiveFailuresCount(5)
            .lastSuccessfulRemoteRefreshBytes(5)
            .uploadBytesMovingAverage(5)
            .uploadBytesPerSecMovingAverage(0)
            .uploadTimeMovingAverage(0)
            .bytesLag(0)
            .totalUploadTimeInMs(10)
            .directoryFileTransferTrackerStats(createZeroDirectoryFileTransferStats())
            .build();
    }

    static RemoteSegmentTransferTracker.Stats createStatsForNewReplica(ShardId shardId) {
        return new RemoteSegmentTransferTracker.Stats.Builder().shardId(shardId)
            .localRefreshClockTimeMs(0)
            .remoteRefreshClockTimeMs(0)
            .refreshTimeLagMs(0)
            .localRefreshNumber(0)
            .remoteRefreshNumber(0)
            .uploadBytesStarted(0)
            .uploadBytesSucceeded(0)
            .uploadBytesFailed(0)
            .totalUploadsStarted(0)
            .totalUploadsSucceeded(0)
            .totalUploadsFailed(0)
            .rejectionCount(0)
            .consecutiveFailuresCount(0)
            .lastSuccessfulRemoteRefreshBytes(0)
            .uploadBytesMovingAverage(0)
            .uploadBytesPerSecMovingAverage(0)
            .uploadTimeMovingAverage(0)
            .bytesLag(0)
            .totalUploadTimeInMs(0)
            .directoryFileTransferTrackerStats(createZeroDirectoryFileTransferStats())
            .build();
    }

    static RemoteSegmentTransferTracker.Stats createStatsForRemoteStoreRestoredPrimary(ShardId shardId) {
        return new RemoteSegmentTransferTracker.Stats.Builder().shardId(shardId)
            .localRefreshClockTimeMs(50)
            .remoteRefreshClockTimeMs(50)
            .refreshTimeLagMs(0)
            .localRefreshNumber(50)
            .remoteRefreshNumber(11)
            .uploadBytesStarted(11)
            .uploadBytesSucceeded(10)
            .uploadBytesFailed(10)
            .totalUploadsStarted(0)
            .totalUploadsSucceeded(10)
            .totalUploadsFailed(10)
            .rejectionCount(0)
            .consecutiveFailuresCount(10)
            .lastSuccessfulRemoteRefreshBytes(10)
            .uploadBytesMovingAverage(0)
            .uploadBytesPerSecMovingAverage(0)
            .uploadTimeMovingAverage(0)
            .bytesLag(100)
            .totalUploadTimeInMs(10)
            .directoryFileTransferTrackerStats(createZeroDirectoryFileTransferStats())
            .build();
    }

    static DirectoryFileTransferTracker.Stats createSampleDirectoryFileTransferStats() {
        return new DirectoryFileTransferTracker.Stats.Builder().transferredBytesStarted(10)
            .transferredBytesFailed(0)
            .transferredBytesSucceeded(10)
            .lastTransferTimestampMs(12345)
            .totalTransferTimeInMs(5)
            .transferredBytesMovingAverage(5)
            .lastSuccessfulTransferInBytes(5)
            .transferredBytesPerSecMovingAverage(10)
            .build();
    }

    static DirectoryFileTransferTracker.Stats createZeroDirectoryFileTransferStats() {
        return new DirectoryFileTransferTracker.Stats.Builder().transferredBytesStarted(0)
            .transferredBytesFailed(0)
            .transferredBytesSucceeded(0)
            .lastTransferTimestampMs(0)
            .totalTransferTimeInMs(0)
            .transferredBytesMovingAverage(0)
            .lastSuccessfulTransferInBytes(0)
            .transferredBytesPerSecMovingAverage(0)
            .build();
    }

    static ShardRouting createShardRouting(ShardId shardId, boolean isPrimary) {
        return TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(4), isPrimary, ShardRoutingState.STARTED);
    }

    static RemoteTranslogTransferTracker.Stats createTranslogStats(ShardId shardId) {
        return new RemoteTranslogTransferTracker.Stats.Builder().shardId(shardId)
            .lastSuccessfulUploadTimestamp(1L)
            .totalUploadsStarted(2L)
            .totalUploadsSucceeded(3L)
            .totalUploadsFailed(4L)
            .uploadBytesStarted(5L)
            .uploadBytesSucceeded(6L)
            .uploadBytesFailed(7L)
            .totalUploadTimeInMillis(8L)
            .uploadBytesMovingAverage(9D)
            .uploadBytesPerSecMovingAverage(10D)
            .uploadTimeMovingAverage(11D)
            .lastSuccessfulDownloadTimestamp(1L)
            .totalDownloadsSucceeded(2L)
            .downloadBytesSucceeded(3L)
            .totalDownloadTimeInMillis(4L)
            .downloadBytesMovingAverage(9D)
            .downloadBytesPerSecMovingAverage(10D)
            .downloadTimeMovingAverage(11D)
            .build();
    }

    static RemoteTranslogTransferTracker.Stats createEmptyTranslogStats(ShardId shardId) {
        return new RemoteTranslogTransferTracker.Stats.Builder().shardId(shardId)
            .lastSuccessfulUploadTimestamp(0L)
            .totalUploadsStarted(0L)
            .totalUploadsSucceeded(0L)
            .totalUploadsFailed(0L)
            .uploadBytesStarted(0L)
            .uploadBytesSucceeded(0L)
            .uploadBytesFailed(0L)
            .totalUploadTimeInMillis(0L)
            .uploadBytesMovingAverage(0D)
            .uploadBytesPerSecMovingAverage(0D)
            .uploadTimeMovingAverage(0D)
            .lastSuccessfulDownloadTimestamp(0L)
            .totalDownloadsSucceeded(0L)
            .downloadBytesSucceeded(0L)
            .totalDownloadTimeInMillis(0L)
            .downloadBytesMovingAverage(0D)
            .downloadBytesPerSecMovingAverage(0D)
            .downloadTimeMovingAverage(0D)
            .build();
    }

    static void compareStatsResponse(
        Map<String, Object> statsObject,
        RemoteSegmentTransferTracker.Stats segmentTransferStats,
        RemoteTranslogTransferTracker.Stats translogTransferStats,
        ShardRouting routing
    ) {
        // Compare Remote Segment Store stats
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

        if (segmentTransferStats.directoryFileTransferTrackerStats.transferredBytesStarted != 0) {
            assertEquals(
                segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.LAST_SYNC_TIMESTAMP),
                (int) segmentTransferStats.directoryFileTransferTrackerStats.lastTransferTimestampMs
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOAD_SIZE)).get(
                    RemoteStoreStats.SubFields.STARTED_BYTES
                ),
                (int) segmentTransferStats.directoryFileTransferTrackerStats.transferredBytesStarted
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOAD_SIZE)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED_BYTES
                ),
                (int) segmentTransferStats.directoryFileTransferTrackerStats.transferredBytesSucceeded
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOAD_SIZE)).get(
                    RemoteStoreStats.SubFields.FAILED_BYTES
                ),
                (int) segmentTransferStats.directoryFileTransferTrackerStats.transferredBytesFailed
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.LAST_SUCCESSFUL
                ),
                (int) segmentTransferStats.directoryFileTransferTrackerStats.lastSuccessfulTransferInBytes
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                segmentTransferStats.directoryFileTransferTrackerStats.transferredBytesMovingAverage
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SPEED_IN_BYTES_PER_SEC)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                segmentTransferStats.directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage
            );
        } else {
            assertTrue(segmentDownloads.isEmpty());
        }

        if (segmentTransferStats.totalUploadsStarted != 0) {
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.LOCAL_REFRESH_TIMESTAMP),
                (int) segmentTransferStats.localRefreshClockTimeMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_TIMESTAMP),
                (int) segmentTransferStats.remoteRefreshClockTimeMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REFRESH_TIME_LAG_IN_MILLIS),
                (int) segmentTransferStats.refreshTimeLagMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REFRESH_LAG),
                (int) (segmentTransferStats.localRefreshNumber - segmentTransferStats.remoteRefreshNumber)
            );
            assertEquals(segmentUploads.get(RemoteStoreStats.UploadStatsFields.BYTES_LAG), (int) segmentTransferStats.bytesLag);

            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.BACKPRESSURE_REJECTION_COUNT),
                (int) segmentTransferStats.rejectionCount
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.CONSECUTIVE_FAILURE_COUNT),
                (int) segmentTransferStats.consecutiveFailuresCount
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOAD_SIZE)).get(
                    RemoteStoreStats.SubFields.STARTED_BYTES
                ),
                (int) segmentTransferStats.uploadBytesStarted
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOAD_SIZE)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED_BYTES
                ),
                (int) segmentTransferStats.uploadBytesSucceeded
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOAD_SIZE)).get(
                    RemoteStoreStats.SubFields.FAILED_BYTES
                ),
                (int) segmentTransferStats.uploadBytesFailed
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                segmentTransferStats.uploadBytesMovingAverage
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.LAST_SUCCESSFUL
                ),
                (int) segmentTransferStats.lastSuccessfulRemoteRefreshBytes
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.UPLOAD_SPEED_IN_BYTES_PER_SEC)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                segmentTransferStats.uploadBytesPerSecMovingAverage
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS)).get(RemoteStoreStats.SubFields.STARTED),
                (int) segmentTransferStats.totalUploadsStarted
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS)).get(RemoteStoreStats.SubFields.SUCCEEDED),
                (int) segmentTransferStats.totalUploadsSucceeded
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS)).get(RemoteStoreStats.SubFields.FAILED),
                (int) segmentTransferStats.totalUploadsFailed
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_LATENCY_IN_MILLIS)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                segmentTransferStats.uploadTimeMovingAverage
            );
        } else {
            assertTrue(segmentUploads.isEmpty());
        }

        // Compare Remote Translog Store stats
        Map<?, ?> tlogStatsObj = (Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.TRANSLOG);
        Map<?, ?> tlogUploadStatsObj = (Map<?, ?>) tlogStatsObj.get(RemoteStoreStats.SubFields.UPLOAD);
        if (translogTransferStats.totalUploadsStarted > 0) {
            assertEquals(
                translogTransferStats.lastSuccessfulUploadTimestamp,
                Long.parseLong(tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.LAST_SUCCESSFUL_UPLOAD_TIMESTAMP).toString())
            );

            assertEquals(
                translogTransferStats.totalUploadsStarted,
                Long.parseLong(
                    ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS)).get(
                        RemoteStoreStats.SubFields.STARTED
                    ).toString()
                )
            );
            assertEquals(
                translogTransferStats.totalUploadsSucceeded,
                Long.parseLong(
                    ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS)).get(
                        RemoteStoreStats.SubFields.SUCCEEDED
                    ).toString()
                )
            );
            assertEquals(
                translogTransferStats.totalUploadsFailed,
                Long.parseLong(
                    ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS)).get(
                        RemoteStoreStats.SubFields.FAILED
                    ).toString()
                )
            );

            assertEquals(
                translogTransferStats.uploadBytesStarted,
                Long.parseLong(
                    ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOAD_SIZE)).get(
                        RemoteStoreStats.SubFields.STARTED_BYTES
                    ).toString()
                )
            );
            assertEquals(
                translogTransferStats.uploadBytesSucceeded,
                Long.parseLong(
                    ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOAD_SIZE)).get(
                        RemoteStoreStats.SubFields.SUCCEEDED_BYTES
                    ).toString()
                )
            );
            assertEquals(
                translogTransferStats.uploadBytesFailed,
                Long.parseLong(
                    ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOAD_SIZE)).get(
                        RemoteStoreStats.SubFields.FAILED_BYTES
                    ).toString()
                )
            );

            assertEquals(
                translogTransferStats.totalUploadTimeInMillis,
                Long.parseLong(tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOAD_TIME_IN_MILLIS).toString())
            );

            assertEquals(
                translogTransferStats.uploadBytesMovingAverage,
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.UPLOAD_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                )
            );
            assertEquals(
                translogTransferStats.uploadBytesPerSecMovingAverage,
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.UPLOAD_SPEED_IN_BYTES_PER_SEC)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                )
            );
            assertEquals(
                translogTransferStats.uploadTimeMovingAverage,
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.UPLOAD_TIME_IN_MILLIS)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                )
            );
        } else {
            assertNull(tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS));
        }

        Map<?, ?> tlogDownloadStatsObj = (Map<?, ?>) tlogStatsObj.get(RemoteStoreStats.SubFields.DOWNLOAD);
        if (translogTransferStats.totalDownloadsSucceeded > 0) {
            assertEquals(
                translogTransferStats.lastSuccessfulDownloadTimestamp,
                Long.parseLong(tlogDownloadStatsObj.get(RemoteStoreStats.DownloadStatsFields.LAST_SUCCESSFUL_DOWNLOAD_TIMESTAMP).toString())
            );
            assertEquals(
                translogTransferStats.totalDownloadsSucceeded,
                Long.parseLong(
                    ((Map<?, ?>) tlogDownloadStatsObj.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOADS)).get(
                        RemoteStoreStats.SubFields.SUCCEEDED
                    ).toString()
                )
            );
            assertEquals(
                translogTransferStats.downloadBytesSucceeded,
                Long.parseLong(
                    ((Map<?, ?>) tlogDownloadStatsObj.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOAD_SIZE)).get(
                        RemoteStoreStats.SubFields.SUCCEEDED_BYTES
                    ).toString()
                )
            );
            assertEquals(
                translogTransferStats.totalDownloadTimeInMillis,
                Long.parseLong(tlogDownloadStatsObj.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOAD_TIME_IN_MILLIS).toString())
            );

            assertEquals(
                translogTransferStats.downloadBytesMovingAverage,
                ((Map<?, ?>) tlogDownloadStatsObj.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                )
            );
            assertEquals(
                translogTransferStats.downloadBytesPerSecMovingAverage,
                ((Map<?, ?>) tlogDownloadStatsObj.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SPEED_IN_BYTES_PER_SEC)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                )
            );
            assertEquals(
                translogTransferStats.downloadTimeMovingAverage,
                ((Map<?, ?>) tlogDownloadStatsObj.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_TIME_IN_MILLIS)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                )
            );
        } else {
            assertNull(tlogDownloadStatsObj.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOAD_SIZE));
        }
    }
}

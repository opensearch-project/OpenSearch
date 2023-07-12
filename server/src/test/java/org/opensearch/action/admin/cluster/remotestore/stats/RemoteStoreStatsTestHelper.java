/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.core.index.shard.ShardId;

import java.util.Map;

import static org.opensearch.test.OpenSearchTestCase.assertEquals;

/**
 * Helper utilities for Remote Store stats tests
 */
public class RemoteStoreStatsTestHelper {
    static RemoteRefreshSegmentTracker.Stats createPressureTrackerStats(ShardId shardId) {
        return new RemoteRefreshSegmentTracker.Stats(shardId, 101, 102, 100, 3, 2, 10, 5, 5, 10, 5, 5, 3, 2, 5, 2, 3, 4, 9);
    }

    static void compareStatsResponse(Map<String, Object> statsObject, RemoteRefreshSegmentTracker.Stats pressureTrackerStats) {
        assertEquals(statsObject.get(RemoteStoreStats.Fields.SHARD_ID), pressureTrackerStats.shardId.toString());
        assertEquals(statsObject.get(RemoteStoreStats.Fields.LOCAL_REFRESH_TIMESTAMP), (int) pressureTrackerStats.localRefreshClockTimeMs);
        assertEquals(
            statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_TIMESTAMP),
            (int) pressureTrackerStats.remoteRefreshClockTimeMs
        );
        assertEquals(statsObject.get(RemoteStoreStats.Fields.REFRESH_TIME_LAG_IN_MILLIS), (int) pressureTrackerStats.refreshTimeLagMs);
        assertEquals(
            statsObject.get(RemoteStoreStats.Fields.REFRESH_LAG),
            (int) (pressureTrackerStats.localRefreshNumber - pressureTrackerStats.remoteRefreshNumber)
        );
        assertEquals(statsObject.get(RemoteStoreStats.Fields.BYTES_LAG), (int) pressureTrackerStats.bytesLag);

        assertEquals(statsObject.get(RemoteStoreStats.Fields.BACKPRESSURE_REJECTION_COUNT), (int) pressureTrackerStats.rejectionCount);
        assertEquals(
            statsObject.get(RemoteStoreStats.Fields.CONSECUTIVE_FAILURE_COUNT),
            (int) pressureTrackerStats.consecutiveFailuresCount
        );

        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.SubFields.STARTED),
            (int) pressureTrackerStats.uploadBytesStarted
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.SubFields.SUCCEEDED),
            (int) pressureTrackerStats.uploadBytesSucceeded
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.SubFields.FAILED),
            (int) pressureTrackerStats.uploadBytesFailed
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(RemoteStoreStats.SubFields.MOVING_AVG),
            pressureTrackerStats.uploadBytesMovingAverage
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(RemoteStoreStats.SubFields.LAST_SUCCESSFUL),
            (int) pressureTrackerStats.lastSuccessfulRemoteRefreshBytes
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.UPLOAD_LATENCY_IN_BYTES_PER_SEC)).get(RemoteStoreStats.SubFields.MOVING_AVG),
            pressureTrackerStats.uploadBytesPerSecMovingAverage
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.SubFields.STARTED),
            (int) pressureTrackerStats.totalUploadsStarted
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.SubFields.SUCCEEDED),
            (int) pressureTrackerStats.totalUploadsSucceeded
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.SubFields.FAILED),
            (int) pressureTrackerStats.totalUploadsFailed
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_LATENCY_IN_MILLIS)).get(RemoteStoreStats.SubFields.MOVING_AVG),
            pressureTrackerStats.uploadTimeMovingAverage
        );
    }
}

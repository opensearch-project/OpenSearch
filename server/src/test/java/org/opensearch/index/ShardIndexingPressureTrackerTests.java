/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.index.ShardIndexingPressureTracker.OperationTracker;
import org.opensearch.index.ShardIndexingPressureTracker.CommonOperationTracker;
import org.opensearch.index.ShardIndexingPressureTracker.StatsTracker;
import org.opensearch.index.ShardIndexingPressureTracker.RejectionTracker;
import org.opensearch.index.ShardIndexingPressureTracker.PerformanceTracker;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

public class ShardIndexingPressureTrackerTests extends OpenSearchTestCase {

    public void testShardIndexingPressureTracker() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        ShardIndexingPressureTracker shardIndexingPressureTracker = new ShardIndexingPressureTracker(shardId, 100L, 150L);

        assertEquals(shardId, shardIndexingPressureTracker.getShardId());
        assertEquals(100L, shardIndexingPressureTracker.getPrimaryAndCoordinatingLimits());
        assertEquals(150L, shardIndexingPressureTracker.getReplicaLimits());

        OperationTracker coordinatingTracker = shardIndexingPressureTracker.getCoordinatingOperationTracker();
        assertStatsTracker(coordinatingTracker.getStatsTracker());
        assertRejectionTracker(coordinatingTracker.getRejectionTracker());
        assertPerformanceTracker(coordinatingTracker.getPerformanceTracker());

        OperationTracker primaryTracker = shardIndexingPressureTracker.getPrimaryOperationTracker();
        assertStatsTracker(primaryTracker.getStatsTracker());
        assertRejectionTracker(primaryTracker.getRejectionTracker());
        assertPerformanceTracker(primaryTracker.getPerformanceTracker());

        OperationTracker replicaTracker = shardIndexingPressureTracker.getReplicaOperationTracker();
        assertStatsTracker(replicaTracker.getStatsTracker());
        assertRejectionTracker(replicaTracker.getRejectionTracker());
        assertPerformanceTracker(replicaTracker.getPerformanceTracker());

        CommonOperationTracker commonOperationTracker = shardIndexingPressureTracker.getCommonOperationTracker();
        assertEquals(0L, commonOperationTracker.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0L, commonOperationTracker.getTotalCombinedCoordinatingAndPrimaryBytes());
    }

    private void assertStatsTracker(StatsTracker statsTracker) {
        assertEquals(0L, statsTracker.getCurrentBytes());
        assertEquals(0L, statsTracker.getTotalBytes());
        assertEquals(0L, statsTracker.getRequestCount());
    }

    private void assertRejectionTracker(RejectionTracker rejectionTracker) {
        assertEquals(0L, rejectionTracker.getTotalRejections());
        assertEquals(0L, rejectionTracker.getNodeLimitsBreachedRejections());
        assertEquals(0L, rejectionTracker.getLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(0L, rejectionTracker.getThroughputDegradationLimitsBreachedRejections());
    }

    private void assertPerformanceTracker(PerformanceTracker performanceTracker) {
        assertEquals(0L, performanceTracker.getLatencyInMillis());
        assertEquals(0L, performanceTracker.getLastSuccessfulRequestTimestamp());
        assertEquals(0L, performanceTracker.getTotalOutstandingRequests());
        assertEquals(0L, performanceTracker.getThroughputMovingAverage());
        assertTrue(performanceTracker.addNewThroughout(10.0));
        assertEquals(1L, performanceTracker.getThroughputMovingQueueSize());
        assertEquals(10.0, performanceTracker.getFirstThroughput(), 0.0);
        assertEquals(0L, performanceTracker.getThroughputMovingQueueSize());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;

public class ShardIndexingPressureMemoryManagerTests extends OpenSearchTestCase {

    private final Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
        .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
        .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), 20)
        .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 2)
        .build();
    private final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final ShardIndexingPressureSettings shardIndexingPressureSettings =
        new ShardIndexingPressureSettings(new ClusterService(settings, clusterSettings, null), settings,
            IndexingPressure.MAX_INDEXING_BYTES.get(settings).getBytes());

    private final Index index = new Index("IndexName", "UUID");
    private final ShardId shardId1 = new ShardId(index, 0);
    private final ShardId shardId2 = new ShardId(index, 1);

    public void testCoordinatingPrimaryShardLimitsNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker = memoryManager.getShardIndexingPressureTracker(shardId1);
        tracker.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(1);
        long requestStartTime = System.currentTimeMillis();
        Map<ShardId, ShardIndexingPressureTracker> hotStore = memoryManager.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker, 1 * 1024, requestStartTime));
        assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker,  1 * 1024, requestStartTime));
    }

    public void testReplicaShardLimitsNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker = memoryManager.getShardIndexingPressureTracker(shardId1);
        tracker.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(1);
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = Collections.singletonMap((long) shardId1.hashCode(), tracker);

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker, 1 * 1024, requestStartTime));
    }

    public void testCoordinatingPrimaryShardLimitsIncreasedAndSoftLimitNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker = memoryManager.getShardIndexingPressureTracker(shardId1);
        tracker.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(10);
        long baseLimit = tracker.getPrimaryAndCoordinatingLimits();
        long requestStartTime = System.currentTimeMillis();
        Map<ShardId, ShardIndexingPressureTracker> hotStore = memoryManager.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker, 1 * 1024, requestStartTime));
        assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker, 1 * 1024, requestStartTime));

        assertTrue(tracker.getPrimaryAndCoordinatingLimits() > baseLimit);
        assertEquals(tracker.getPrimaryAndCoordinatingLimits(), (long)(baseLimit/0.85));
    }

    public void testReplicaShardLimitsIncreasedAndSoftLimitNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker = memoryManager.getShardIndexingPressureTracker(shardId1);
        tracker.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(15);
        long baseLimit = tracker.getReplicaLimits();
        long requestStartTime = System.currentTimeMillis();
        Map<ShardId, ShardIndexingPressureTracker> hotStore = memoryManager.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker, 1 * 1024, requestStartTime));
        assertTrue(tracker.getReplicaLimits() > baseLimit);
        assertEquals(tracker.getReplicaLimits(), (long)(baseLimit/0.85));
    }

    public void testCoordinatingPrimarySoftLimitNotBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(4 * 1024);
        tracker2.compareAndSetPrimaryAndCoordinatingLimits(tracker2.getPrimaryAndCoordinatingLimits(), 6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits();
        long requestStartTime = System.currentTimeMillis();
        Map<ShardId, ShardIndexingPressureTracker> hotStore = memoryManager.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, 8 * 1024, requestStartTime));
        assertEquals(1, tracker1.getCoordinatingOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getCoordinatingOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());

        assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, 8 * 1024, requestStartTime));
        assertEquals(1, tracker1.getPrimaryOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getPrimaryOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());

        assertEquals(limit1, tracker1.getPrimaryAndCoordinatingLimits());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits());
        assertEquals(2, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitNotBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(5 * 1024);
        tracker2.compareAndSetReplicaLimits(tracker2.getReplicaLimits(), 10 * 1024);
        long limit1 = tracker1.getReplicaLimits();
        long limit2 = tracker2.getReplicaLimits();
        long requestStartTime = System.currentTimeMillis();
        Map<ShardId, ShardIndexingPressureTracker> hotStore = memoryManager.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, 10 * 1024, requestStartTime));
        assertEquals(limit1, tracker1.getReplicaLimits());
        assertEquals(limit2, tracker2.getReplicaLimits());
        assertEquals(1, tracker1.getReplicaOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getReplicaOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(4 * 1024);
        tracker2.compareAndSetPrimaryAndCoordinatingLimits(tracker2.getPrimaryAndCoordinatingLimits(), 6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits();
        long requestStartTime = System.currentTimeMillis();

        assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, 8 * 1024, requestStartTime));
        assertEquals(1, tracker1.getCoordinatingOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getCoordinatingOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());

        assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1,  8 * 1024, requestStartTime));
        assertEquals(1, tracker1.getPrimaryOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getPrimaryOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());

        assertEquals(limit1, tracker1.getPrimaryAndCoordinatingLimits());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits());
        assertEquals(2, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(5 * 1024);
        tracker2.compareAndSetReplicaLimits(tracker2.getReplicaLimits(), 12 * 1024);
        long limit1 = tracker1.getReplicaLimits();
        long limit2 = tracker2.getReplicaLimits();
        long requestStartTime = System.currentTimeMillis();
        Map<ShardId, ShardIndexingPressureTracker> hotStore = memoryManager.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1,  12 * 1024, requestStartTime));
        assertEquals(limit1, tracker1.getReplicaLimits());
        assertEquals(limit2, tracker2.getReplicaLimits());
        assertEquals(1, tracker1.getReplicaOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getReplicaOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(4 * 1024);
        tracker2.compareAndSetPrimaryAndCoordinatingLimits(tracker2.getPrimaryAndCoordinatingLimits(), 6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits();
        long requestStartTime = System.currentTimeMillis();
        Map<ShardId, ShardIndexingPressureTracker> hotStore = memoryManager.getShardIndexingPressureHotStore();


        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().updateLastSuccessfulRequestTimestamp(requestStartTime - 100);
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();

        assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, 8 * 1024, requestStartTime));
        assertEquals(1, tracker1.getCoordinatingOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(0, tracker2.getCoordinatingOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());

        tracker1.getPrimaryOperationTracker().getPerformanceTracker().updateLastSuccessfulRequestTimestamp(requestStartTime - 100);
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();

        assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1,  8 * 1024, requestStartTime));
        assertEquals(1, tracker1.getPrimaryOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(0, tracker2.getPrimaryOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());

        assertEquals(limit1, tracker1.getPrimaryAndCoordinatingLimits());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits());
        assertEquals(2, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);

        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(5 * 1024);
        tracker2.compareAndSetReplicaLimits(tracker2.getReplicaLimits(), 12 * 1024);
        long limit1 = tracker1.getReplicaLimits();
        long limit2 = tracker2.getReplicaLimits();
        long requestStartTime = System.currentTimeMillis();
        tracker1.getReplicaOperationTracker().getPerformanceTracker().updateLastSuccessfulRequestTimestamp(requestStartTime - 100);
        tracker1.getReplicaOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getReplicaOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1,  12 * 1024, requestStartTime));
        assertEquals(limit1, tracker1.getReplicaLimits());
        assertEquals(limit2, tracker2.getReplicaLimits());
        assertEquals(1, tracker1.getReplicaOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(0, tracker2.getReplicaOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(1, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndLessOutstandingRequestsAndNoLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(1 * 1024);
        tracker2.compareAndSetPrimaryAndCoordinatingLimits(tracker2.getPrimaryAndCoordinatingLimits(), 6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits();
        long requestStartTime = System.currentTimeMillis();
        Map<ShardId, ShardIndexingPressureTracker> hotStore = memoryManager.getShardIndexingPressureHotStore();

        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().updateLastSuccessfulRequestTimestamp(requestStartTime - 100);
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();

        assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker1, 8 * 1024, requestStartTime));
        assertEquals(0, tracker1.getCoordinatingOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(0, tracker2.getCoordinatingOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());

        tracker1.getPrimaryOperationTracker().getPerformanceTracker().updateLastSuccessfulRequestTimestamp(requestStartTime - 100);
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();

        assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker1,  8 * 1024, requestStartTime));
        assertEquals(0, tracker1.getPrimaryOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(0, tracker2.getPrimaryOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());

        assertTrue(tracker1.getPrimaryAndCoordinatingLimits() > limit1);
        assertEquals((long)(1 * 1024/0.85), tracker1.getPrimaryAndCoordinatingLimits());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits());
        assertEquals(0, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndLessOutstandingRequestsAndNoLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(2 * 1024);
        tracker2.compareAndSetReplicaLimits(tracker2.getReplicaLimits(), 12 * 1024);
        long limit1 = tracker1.getReplicaLimits();
        long limit2 = tracker2.getReplicaLimits();
        long requestStartTime = System.currentTimeMillis();
        tracker1.getReplicaOperationTracker().getPerformanceTracker().updateLastSuccessfulRequestTimestamp(requestStartTime - 100);
        tracker1.getReplicaOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker1,  12 * 1024, requestStartTime));
        assertTrue(tracker1.getReplicaLimits() > limit1);
        assertEquals((long)(2 * 1024/0.85), tracker1.getReplicaLimits());
        assertEquals(limit2, tracker2.getReplicaLimits());
        assertEquals(0, tracker1.getReplicaOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(0, tracker2.getReplicaOperationTracker().getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(0, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(4 * 1024);
        tracker2.compareAndSetPrimaryAndCoordinatingLimits(tracker2.getPrimaryAndCoordinatingLimits(), 6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits();
        long requestStartTime = System.currentTimeMillis();

        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().updateThroughputMovingAverage(Double.doubleToLongBits(1d));
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getCoordinatingOperationTracker().getStatsTracker().incrementTotalBytes(60);
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().addLatencyInMillis(10);
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().addNewThroughout(1d);
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().addNewThroughout(2d);

        assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, 8 * 1024, requestStartTime));
        assertEquals(1, tracker1.getCoordinatingOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, tracker2.getCoordinatingOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());

        tracker1.getPrimaryOperationTracker().getPerformanceTracker().updateThroughputMovingAverage(Double.doubleToLongBits(1d));
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getPrimaryOperationTracker().getStatsTracker().incrementTotalBytes(60);
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().addLatencyInMillis(10);
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().addNewThroughout(1d);
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().addNewThroughout(2d);

        assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1,  8 * 1024, requestStartTime));
        assertEquals(1, tracker1.getPrimaryOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, tracker2.getPrimaryOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());

        assertEquals(limit1, tracker1.getPrimaryAndCoordinatingLimits());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits());
        assertEquals(2, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(5 * 1024);
        tracker2.compareAndSetReplicaLimits(tracker2.getReplicaLimits(), 12 * 1024);
        long limit1 = tracker1.getReplicaLimits();
        long limit2 = tracker2.getReplicaLimits();
        long requestStartTime = System.currentTimeMillis();
        tracker1.getReplicaOperationTracker().getPerformanceTracker().updateThroughputMovingAverage(Double.doubleToLongBits(1d));
        tracker1.getReplicaOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getReplicaOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementTotalBytes(80);
        tracker1.getReplicaOperationTracker().getPerformanceTracker().addLatencyInMillis(10);
        tracker1.getReplicaOperationTracker().getPerformanceTracker().addNewThroughout(1d);
        tracker1.getReplicaOperationTracker().getPerformanceTracker().addNewThroughout(2d);

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1,  12 * 1024, requestStartTime));
        assertEquals(limit1, tracker1.getReplicaLimits());
        assertEquals(limit2, tracker2.getReplicaLimits());
        assertEquals(1, tracker1.getReplicaOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, tracker2.getReplicaOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());
        assertEquals(1, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndMovingAverageQueueNotBuildUpAndNoThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);

        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(1 * 1024);
        tracker2.compareAndSetPrimaryAndCoordinatingLimits(tracker2.getPrimaryAndCoordinatingLimits(), 6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits();
        long requestStartTime = System.currentTimeMillis();

        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().updateThroughputMovingAverage
            (Double.doubleToLongBits(1d));
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getCoordinatingOperationTracker().getStatsTracker().incrementTotalBytes(60);
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().addLatencyInMillis(10);
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().addNewThroughout(1d);

        assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker1, 8 * 1024, requestStartTime));
        assertEquals(0, tracker1.getCoordinatingOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, tracker2.getCoordinatingOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, tracker1.getCoordinatingOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getCoordinatingOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());

        tracker1.getPrimaryOperationTracker().getPerformanceTracker().updateThroughputMovingAverage(Double.doubleToLongBits(1d));
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getPrimaryOperationTracker().getStatsTracker().incrementTotalBytes(60);
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().addLatencyInMillis(10);
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().addNewThroughout(1d);

        assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker1,  8 * 1024, requestStartTime));
        assertEquals(0, tracker1.getPrimaryOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, tracker2.getPrimaryOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, tracker1.getPrimaryOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getPrimaryOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());

        assertTrue(tracker1.getPrimaryAndCoordinatingLimits() > limit1);
        assertEquals((long)(1 * 1024/0.85), tracker1.getPrimaryAndCoordinatingLimits());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits());
        assertEquals(0, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndMovingAverageQueueNotBuildUpAndNThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(2 * 1024);
        tracker2.compareAndSetReplicaLimits(tracker2.getReplicaLimits(), 12 * 1024);
        long limit1 = tracker1.getReplicaLimits();
        long limit2 = tracker2.getReplicaLimits();
        long requestStartTime = System.currentTimeMillis();
        tracker1.getReplicaOperationTracker().getPerformanceTracker().updateThroughputMovingAverage(Double.doubleToLongBits(1d));
        tracker1.getReplicaOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementTotalBytes(80);
        tracker1.getReplicaOperationTracker().getPerformanceTracker().addLatencyInMillis(10);
        tracker1.getReplicaOperationTracker().getPerformanceTracker().addNewThroughout(1d);

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker1,  12 * 1024, requestStartTime));
        assertTrue(tracker1.getReplicaLimits() > limit1);
        assertEquals((long)(2 * 1024/0.85), tracker1.getReplicaLimits());
        assertEquals(limit2, tracker2.getReplicaLimits());
        assertEquals(0, tracker1.getReplicaOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, tracker2.getReplicaOperationTracker().getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, tracker1.getReplicaOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndNoSecondaryParameterBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(4 * 1024);
        tracker2.compareAndSetPrimaryAndCoordinatingLimits(tracker2.getPrimaryAndCoordinatingLimits(), 6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits();
        long requestStartTime = System.currentTimeMillis();

        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().updateThroughputMovingAverage(Double.doubleToLongBits(1d));
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getCoordinatingOperationTracker().getStatsTracker().incrementTotalBytes(60);
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().addLatencyInMillis(10);
        tracker1.getCoordinatingOperationTracker().getPerformanceTracker().addNewThroughout(1d);

        assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, 8 * 1024, requestStartTime));
        assertEquals(1, tracker1.getCoordinatingOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getCoordinatingOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());

        tracker1.getPrimaryOperationTracker().getPerformanceTracker().updateThroughputMovingAverage(Double.doubleToLongBits(1d));
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getPrimaryOperationTracker().getStatsTracker().incrementTotalBytes(60);
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().addLatencyInMillis(10);
        tracker1.getPrimaryOperationTracker().getPerformanceTracker().addNewThroughout(1d);

        assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1,  8 * 1024, requestStartTime));
        assertEquals(1, tracker1.getPrimaryOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getPrimaryOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());

        assertEquals(limit1, tracker1.getPrimaryAndCoordinatingLimits());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits());
        assertEquals(2, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndNoSecondaryParameterBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);

        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = memoryManager.getShardIndexingPressureTracker(shardId2);
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(5 * 1024);
        tracker2.compareAndSetReplicaLimits(tracker2.getReplicaLimits(), 12 * 1024);
        long limit1 = tracker1.getReplicaLimits();
        long limit2 = tracker2.getReplicaLimits();
        long requestStartTime = System.currentTimeMillis();
        tracker1.getReplicaOperationTracker().getPerformanceTracker().updateThroughputMovingAverage(Double.doubleToLongBits(1d));
        tracker1.getReplicaOperationTracker().getPerformanceTracker().incrementTotalOutstandingRequests();
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementTotalBytes(80);
        tracker1.getReplicaOperationTracker().getPerformanceTracker().addLatencyInMillis(10);
        tracker1.getReplicaOperationTracker().getPerformanceTracker().addNewThroughout(1d);

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1,  12 * 1024, requestStartTime));
        assertEquals(limit1, tracker1.getReplicaLimits());
        assertEquals(limit2, tracker2.getReplicaLimits());
        assertEquals(1, tracker1.getReplicaOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(0, tracker2.getReplicaOperationTracker().getRejectionTracker().getNodeLimitsBreachedRejections());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testDecreaseShardPrimaryAndCoordinatingLimitsToBaseLimit() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        tracker1.compareAndSetPrimaryAndCoordinatingLimits(tracker1.getPrimaryAndCoordinatingLimits(), 1 * 1024);
        tracker1.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(0);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits();
        memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker1);

        assertTrue(tracker1.getPrimaryAndCoordinatingLimits() < limit1);
        assertEquals(10, tracker1.getPrimaryAndCoordinatingLimits());
    }

    public void testDecreaseShardReplicaLimitsToBaseLimit() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);

        tracker1.compareAndSetReplicaLimits(tracker1.getReplicaLimits(), 1 * 1024);
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(0);
        long limit1 = tracker1.getReplicaLimits();
        memoryManager.decreaseShardReplicaLimits(tracker1);

        assertTrue(tracker1.getReplicaLimits() < limit1);
        assertEquals(15, tracker1.getReplicaLimits());
    }

    public void testDecreaseShardPrimaryAndCoordinatingLimits() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);
        tracker1.compareAndSetPrimaryAndCoordinatingLimits(tracker1.getPrimaryAndCoordinatingLimits(), 1 * 1024);
        tracker1.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(512);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits();
        memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker1);

        assertTrue(tracker1.getPrimaryAndCoordinatingLimits() < limit1);
        assertEquals((long)(512/0.85), tracker1.getPrimaryAndCoordinatingLimits());
    }

    public void testDecreaseShardReplicaLimits() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = memoryManager.getShardIndexingPressureTracker(shardId1);

        tracker1.compareAndSetReplicaLimits(tracker1.getReplicaLimits(), 1 * 1024);
        tracker1.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(512);
        long limit1 = tracker1.getReplicaLimits();
        memoryManager.decreaseShardReplicaLimits(tracker1);

        assertTrue(tracker1.getReplicaLimits() < limit1);
        assertEquals((long)(512/0.85), tracker1.getReplicaLimits());
    }
}

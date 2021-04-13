/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
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
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId1);
        tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(1);
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
        assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
    }

    public void testReplicaShardLimitsNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId1);
        tracker.memory().getCurrentReplicaBytes().addAndGet(1);
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = Collections.singletonMap((long) shardId1.hashCode(), tracker);

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
    }

    public void testCoordinatingPrimaryShardLimitsIncreasedAndSoftLimitNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId1);
        tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(10);
        long baseLimit = tracker.getPrimaryAndCoordinatingLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
        assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));

        assertTrue(tracker.getPrimaryAndCoordinatingLimits().get() > baseLimit);
        assertEquals(tracker.getPrimaryAndCoordinatingLimits().get(), (long)(baseLimit/0.85));
    }

    public void testReplicaShardLimitsIncreasedAndSoftLimitNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId1);
        tracker.memory().getCurrentReplicaBytes().addAndGet(15);
        long baseLimit = tracker.getReplicaLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
        assertTrue(tracker.getReplicaLimits().get() > baseLimit);
        assertEquals(tracker.getReplicaLimits().get(), (long)(baseLimit/0.85));
    }

    public void testCoordinatingPrimarySoftLimitNotBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(4 * 1024);
        tracker2.getPrimaryAndCoordinatingLimits().addAndGet(6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits().get();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 6 * 1024));
        assertEquals(1, tracker1.rejection().getCoordinatingNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getCoordinatingNodeLimitsBreachedRejections().get());

        assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 6 * 1024));
        assertEquals(1, tracker1.rejection().getPrimaryNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getPrimaryNodeLimitsBreachedRejections().get());

        assertEquals(limit1, tracker1.getPrimaryAndCoordinatingLimits().get());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits().get());
        assertEquals(2, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitNotBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentReplicaBytes().addAndGet(5 * 1024);
        tracker2.getReplicaLimits().addAndGet(10 * 1024);
        long limit1 = tracker1.getReplicaLimits().get();
        long limit2 = tracker2.getReplicaLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 10 * 1024));
        assertEquals(limit1, tracker1.getReplicaLimits().get());
        assertEquals(limit2, tracker2.getReplicaLimits().get());
        assertEquals(1, tracker1.rejection().getReplicaNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getReplicaNodeLimitsBreachedRejections().get());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(4 * 1024);
        tracker2.getPrimaryAndCoordinatingLimits().addAndGet(6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits().get();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(1, tracker1.rejection().getCoordinatingNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getCoordinatingNodeLimitsBreachedRejections().get());

        assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(1, tracker1.rejection().getPrimaryNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getPrimaryNodeLimitsBreachedRejections().get());

        assertEquals(limit1, tracker1.getPrimaryAndCoordinatingLimits().get());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits().get());
        assertEquals(2, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentReplicaBytes().addAndGet(5 * 1024);
        tracker2.getReplicaLimits().addAndGet(12 * 1024);
        long limit1 = tracker1.getReplicaLimits().get();
        long limit2 = tracker2.getReplicaLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertEquals(limit1, tracker1.getReplicaLimits().get());
        assertEquals(limit2, tracker2.getReplicaLimits().get());
        assertEquals(1, tracker1.rejection().getReplicaNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getReplicaNodeLimitsBreachedRejections().get());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(4 * 1024);
        tracker2.getPrimaryAndCoordinatingLimits().addAndGet(6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits().get();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();


        tracker1.timeStamp().getLastSuccessfulCoordinatingRequestTimestamp().addAndGet(requestStartTime - 100);
        tracker1.outstandingRequest().getTotalOutstandingCoordinatingRequests().addAndGet(2);

        assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(1, tracker1.rejection().getCoordinatingLastSuccessfulRequestLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getCoordinatingLastSuccessfulRequestLimitsBreachedRejections().get());

        tracker1.timeStamp().getLastSuccessfulPrimaryRequestTimestamp().addAndGet(requestStartTime - 100);
        tracker1.outstandingRequest().getTotalOutstandingPrimaryRequests().addAndGet(2);

        assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(1, tracker1.rejection().getPrimaryLastSuccessfulRequestLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getPrimaryLastSuccessfulRequestLimitsBreachedRejections().get());

        assertEquals(limit1, tracker1.getPrimaryAndCoordinatingLimits().get());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits().get());
        assertEquals(2, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentReplicaBytes().addAndGet(5 * 1024);
        tracker2.getReplicaLimits().addAndGet(12 * 1024);
        long limit1 = tracker1.getReplicaLimits().get();
        long limit2 = tracker2.getReplicaLimits().get();
        long requestStartTime = System.currentTimeMillis();
        tracker1.timeStamp().getLastSuccessfulReplicaRequestTimestamp().addAndGet(requestStartTime - 100);
        tracker1.outstandingRequest().getTotalOutstandingReplicaRequests().addAndGet(2);

        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertEquals(limit1, tracker1.getReplicaLimits().get());
        assertEquals(limit2, tracker2.getReplicaLimits().get());
        assertEquals(1, tracker1.rejection().getReplicaLastSuccessfulRequestLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getReplicaLastSuccessfulRequestLimitsBreachedRejections().get());
        assertEquals(1, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndLessOutstandingRequestsAndNoLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(1 * 1024);
        tracker2.getPrimaryAndCoordinatingLimits().addAndGet(6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits().get();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        tracker1.timeStamp().getLastSuccessfulCoordinatingRequestTimestamp().addAndGet(requestStartTime - 100);
        tracker1.outstandingRequest().getTotalOutstandingCoordinatingRequests().addAndGet(1);

        assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(0, tracker1.rejection().getCoordinatingLastSuccessfulRequestLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getCoordinatingLastSuccessfulRequestLimitsBreachedRejections().get());

        tracker1.timeStamp().getLastSuccessfulPrimaryRequestTimestamp().addAndGet(requestStartTime - 100);
        tracker1.outstandingRequest().getTotalOutstandingPrimaryRequests().addAndGet(1);

        assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(0, tracker1.rejection().getPrimaryLastSuccessfulRequestLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getPrimaryLastSuccessfulRequestLimitsBreachedRejections().get());

        assertTrue(tracker1.getPrimaryAndCoordinatingLimits().get() > limit1);
        assertEquals((long)(1 * 1024/0.85), tracker1.getPrimaryAndCoordinatingLimits().get());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits().get());
        assertEquals(0, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndLessOutstandingRequestsAndNoLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentReplicaBytes().addAndGet(2 * 1024);
        tracker2.getReplicaLimits().addAndGet(12 * 1024);
        long limit1 = tracker1.getReplicaLimits().get();
        long limit2 = tracker2.getReplicaLimits().get();
        long requestStartTime = System.currentTimeMillis();
        tracker1.timeStamp().getLastSuccessfulReplicaRequestTimestamp().addAndGet(requestStartTime - 100);
        tracker1.outstandingRequest().getTotalOutstandingReplicaRequests().addAndGet(1);

        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertTrue(tracker1.getReplicaLimits().get() > limit1);
        assertEquals((long)(2 * 1024/0.85), tracker1.getReplicaLimits().get());
        assertEquals(limit2, tracker2.getReplicaLimits().get());
        assertEquals(0, tracker1.rejection().getReplicaLastSuccessfulRequestLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getReplicaLastSuccessfulRequestLimitsBreachedRejections().get());
        assertEquals(0, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(4 * 1024);
        tracker2.getPrimaryAndCoordinatingLimits().addAndGet(6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits().get();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        tracker1.throughput().getCoordinatingThroughputMovingAverage().addAndGet(Double.doubleToLongBits(1d));
        tracker1.outstandingRequest().getTotalOutstandingCoordinatingRequests().addAndGet(2);
        tracker1.memory().getTotalCoordinatingBytes().addAndGet(60);
        tracker1.latency().getCoordinatingTimeInMillis().addAndGet(10);
        tracker1.throughput().getCoordinatingThroughputMovingQueue().offer(1d);
        tracker1.throughput().getCoordinatingThroughputMovingQueue().offer(2d);

        assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(1, tracker1.rejection().getCoordinatingThroughputDegradationLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getCoordinatingThroughputDegradationLimitsBreachedRejections().get());

        tracker1.throughput().getPrimaryThroughputMovingAverage().addAndGet(Double.doubleToLongBits(1d));
        tracker1.outstandingRequest().getTotalOutstandingPrimaryRequests().addAndGet(2);
        tracker1.memory().getTotalPrimaryBytes().addAndGet(60);
        tracker1.latency().getPrimaryTimeInMillis().addAndGet(10);
        tracker1.throughput().getPrimaryThroughputMovingQueue().offer(1d);
        tracker1.throughput().getPrimaryThroughputMovingQueue().offer(2d);

        assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(1, tracker1.rejection().getPrimaryThroughputDegradationLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getPrimaryThroughputDegradationLimitsBreachedRejections().get());

        assertEquals(limit1, tracker1.getPrimaryAndCoordinatingLimits().get());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits().get());
        assertEquals(2, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentReplicaBytes().addAndGet(5 * 1024);
        tracker2.getReplicaLimits().addAndGet(12 * 1024);
        long limit1 = tracker1.getReplicaLimits().get();
        long limit2 = tracker2.getReplicaLimits().get();
        long requestStartTime = System.currentTimeMillis();
        tracker1.throughput().getReplicaThroughputMovingAverage().addAndGet(Double.doubleToLongBits(1d));
        tracker1.outstandingRequest().getTotalOutstandingReplicaRequests().addAndGet(2);
        tracker1.memory().getTotalReplicaBytes().addAndGet(80);
        tracker1.latency().getReplicaTimeInMillis().addAndGet(10);
        tracker1.throughput().getReplicaThroughputMovingQueue().offer(1d);
        tracker1.throughput().getReplicaThroughputMovingQueue().offer(2d);

        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertEquals(limit1, tracker1.getReplicaLimits().get());
        assertEquals(limit2, tracker2.getReplicaLimits().get());
        assertEquals(1, tracker1.rejection().getReplicaThroughputDegradationLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getReplicaThroughputDegradationLimitsBreachedRejections().get());
        assertEquals(1, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndMovingAverageQueueNotBuildUpAndNoThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(1 * 1024);
        tracker2.getPrimaryAndCoordinatingLimits().addAndGet(6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits().get();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        tracker1.throughput().getCoordinatingThroughputMovingAverage().addAndGet(Double.doubleToLongBits(1d));
        tracker1.outstandingRequest().getTotalOutstandingCoordinatingRequests().addAndGet(1);
        tracker1.memory().getTotalCoordinatingBytes().addAndGet(60);
        tracker1.latency().getCoordinatingTimeInMillis().addAndGet(10);
        tracker1.throughput().getCoordinatingThroughputMovingQueue().offer(1d);

        assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(0, tracker1.rejection().getCoordinatingThroughputDegradationLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getCoordinatingThroughputDegradationLimitsBreachedRejections().get());
        assertEquals(0, tracker1.rejection().getCoordinatingNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getCoordinatingNodeLimitsBreachedRejections().get());

        tracker1.throughput().getPrimaryThroughputMovingAverage().addAndGet(Double.doubleToLongBits(1d));
        tracker1.outstandingRequest().getTotalOutstandingPrimaryRequests().addAndGet(1);
        tracker1.memory().getTotalPrimaryBytes().addAndGet(60);
        tracker1.latency().getPrimaryTimeInMillis().addAndGet(10);
        tracker1.throughput().getPrimaryThroughputMovingQueue().offer(1d);

        assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(0, tracker1.rejection().getPrimaryThroughputDegradationLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getPrimaryThroughputDegradationLimitsBreachedRejections().get());
        assertEquals(0, tracker1.rejection().getPrimaryNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getPrimaryNodeLimitsBreachedRejections().get());

        assertTrue(tracker1.getPrimaryAndCoordinatingLimits().get() > limit1);
        assertEquals((long)(1 * 1024/0.85), tracker1.getPrimaryAndCoordinatingLimits().get());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits().get());
        assertEquals(0, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndMovingAverageQueueNotBuildUpAndNThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentReplicaBytes().addAndGet(2 * 1024);
        tracker2.getReplicaLimits().addAndGet(12 * 1024);
        long limit1 = tracker1.getReplicaLimits().get();
        long limit2 = tracker2.getReplicaLimits().get();
        long requestStartTime = System.currentTimeMillis();
        tracker1.throughput().getReplicaThroughputMovingAverage().addAndGet(Double.doubleToLongBits(1d));
        tracker1.outstandingRequest().getTotalOutstandingReplicaRequests().addAndGet(1);
        tracker1.memory().getTotalReplicaBytes().addAndGet(80);
        tracker1.latency().getReplicaTimeInMillis().addAndGet(10);
        tracker1.throughput().getReplicaThroughputMovingQueue().offer(1d);

        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertTrue(tracker1.getReplicaLimits().get() > limit1);
        assertEquals((long)(2 * 1024/0.85), tracker1.getReplicaLimits().get());
        assertEquals(limit2, tracker2.getReplicaLimits().get());
        assertEquals(0, tracker1.rejection().getReplicaThroughputDegradationLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getReplicaThroughputDegradationLimitsBreachedRejections().get());
        assertEquals(0, tracker1.rejection().getReplicaNodeLimitsBreachedRejections().get());
        assertEquals(0, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndNoSecondaryParameterBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(4 * 1024);
        tracker2.getPrimaryAndCoordinatingLimits().addAndGet(6 * 1024);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits().get();
        long limit2 = tracker2.getPrimaryAndCoordinatingLimits().get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        tracker1.throughput().getCoordinatingThroughputMovingAverage().addAndGet(Double.doubleToLongBits(1d));
        tracker1.outstandingRequest().getTotalOutstandingCoordinatingRequests().addAndGet(1);
        tracker1.memory().getTotalCoordinatingBytes().addAndGet(60);
        tracker1.latency().getCoordinatingTimeInMillis().addAndGet(10);
        tracker1.throughput().getCoordinatingThroughputMovingQueue().offer(1d);

        assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(1, tracker1.rejection().getCoordinatingNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getCoordinatingNodeLimitsBreachedRejections().get());

        tracker1.throughput().getPrimaryThroughputMovingAverage().addAndGet(Double.doubleToLongBits(1d));
        tracker1.outstandingRequest().getTotalOutstandingPrimaryRequests().addAndGet(1);
        tracker1.memory().getTotalPrimaryBytes().addAndGet(60);
        tracker1.latency().getPrimaryTimeInMillis().addAndGet(10);
        tracker1.throughput().getPrimaryThroughputMovingQueue().offer(1d);

        assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
        assertEquals(1, tracker1.rejection().getPrimaryNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getPrimaryNodeLimitsBreachedRejections().get());

        assertEquals(limit1, tracker1.getPrimaryAndCoordinatingLimits().get());
        assertEquals(limit2, tracker2.getPrimaryAndCoordinatingLimits().get());
        assertEquals(2, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndNoSecondaryParameterBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.memory().getCurrentReplicaBytes().addAndGet(5 * 1024);
        tracker2.getReplicaLimits().addAndGet(12 * 1024);
        long limit1 = tracker1.getReplicaLimits().get();
        long limit2 = tracker2.getReplicaLimits().get();
        long requestStartTime = System.currentTimeMillis();
        tracker1.throughput().getReplicaThroughputMovingAverage().addAndGet(Double.doubleToLongBits(1d));
        tracker1.outstandingRequest().getTotalOutstandingReplicaRequests().addAndGet(1);
        tracker1.memory().getTotalReplicaBytes().addAndGet(80);
        tracker1.latency().getReplicaTimeInMillis().addAndGet(10);
        tracker1.throughput().getReplicaThroughputMovingQueue().offer(1d);

        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertEquals(limit1, tracker1.getReplicaLimits().get());
        assertEquals(limit2, tracker2.getReplicaLimits().get());
        assertEquals(1, tracker1.rejection().getReplicaNodeLimitsBreachedRejections().get());
        assertEquals(0, tracker2.rejection().getReplicaNodeLimitsBreachedRejections().get());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testDecreaseShardPrimaryAndCoordinatingLimitsToBaseLimit() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        tracker1.getPrimaryAndCoordinatingLimits().addAndGet(1 * 1024);
        tracker1.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(0);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits().get();
        memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker1);

        assertTrue(tracker1.getPrimaryAndCoordinatingLimits().get() < limit1);
        assertEquals(10, tracker1.getPrimaryAndCoordinatingLimits().get());
    }

    public void testDecreaseShardReplicaLimitsToBaseLimit() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);

        tracker1.getReplicaLimits().addAndGet(1 * 1024);
        tracker1.memory().getCurrentReplicaBytes().addAndGet(0);
        long limit1 = tracker1.getReplicaLimits().get();
        memoryManager.decreaseShardReplicaLimits(tracker1);

        assertTrue(tracker1.getReplicaLimits().get() < limit1);
        assertEquals(15, tracker1.getReplicaLimits().get());
    }

    public void testDecreaseShardPrimaryAndCoordinatingLimits() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        tracker1.getPrimaryAndCoordinatingLimits().addAndGet(1 * 1024);
        tracker1.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(512);
        long limit1 = tracker1.getPrimaryAndCoordinatingLimits().get();
        memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker1);

        assertTrue(tracker1.getPrimaryAndCoordinatingLimits().get() < limit1);
        assertEquals((long)(512/0.85), tracker1.getPrimaryAndCoordinatingLimits().get());
    }

    public void testDecreaseShardReplicaLimits() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);

        tracker1.getReplicaLimits().addAndGet(1 * 1024);
        tracker1.memory().getCurrentReplicaBytes().addAndGet(512);
        long limit1 = tracker1.getReplicaLimits().get();
        memoryManager.decreaseShardReplicaLimits(tracker1);

        assertTrue(tracker1.getReplicaLimits().get() < limit1);
        assertEquals((long)(512/0.85), tracker1.getReplicaLimits().get());
    }
}

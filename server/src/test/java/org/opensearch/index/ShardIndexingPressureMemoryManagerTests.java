/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

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
        new ShardIndexingPressureSettings(clusterSettings, settings, IndexingPressure.MAX_INDEXING_BYTES.get(settings).getBytes());

    private final Index index = new Index("IndexName", "UUID");
    private final ShardId shardId1 = new ShardId(index, 0);
    private final ShardId shardId2 = new ShardId(index, 1);

    public void testCoordinatingPrimaryShardLimitsNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId1);
        tracker.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(1);
        long requestStartTime = System.currentTimeMillis();
        boolean randomBoolean = randomBoolean();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        if(randomBoolean) {
            assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
        } else {
            assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
        }
    }

    public void testReplicaShardLimitsNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId1);
        tracker.currentReplicaBytes.addAndGet(1);
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = Collections.singletonMap((long) shardId1.hashCode(), tracker);

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
    }

    public void testCoordinatingPrimaryShardLimitsIncreasedAndSoftLimitNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId1);
        tracker.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(10);
        long baseLimit = tracker.primaryAndCoordinatingLimits.get();
        long requestStartTime = System.currentTimeMillis();
        boolean randomBoolean = randomBoolean();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        if(randomBoolean) {
            assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
        } else {
            assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
        }

        assertTrue(tracker.primaryAndCoordinatingLimits.get() > baseLimit);
        assertEquals(tracker.primaryAndCoordinatingLimits.get(), (long)(baseLimit/0.85));
    }

    public void testReplicaShardLimitsIncreasedAndSoftLimitNotBreached() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId1);
        tracker.currentReplicaBytes.addAndGet(15);
        long baseLimit = tracker.replicaLimits.get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker, requestStartTime, hotStore, 1 * 1024));
        assertTrue(tracker.replicaLimits.get() > baseLimit);
        assertEquals(tracker.replicaLimits.get(), (long)(baseLimit/0.85));
    }

    public void testCoordinatingPrimarySoftLimitNotBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(4 * 1024);
        tracker2.primaryAndCoordinatingLimits.addAndGet(6 * 1024);
        long limit1 = tracker1.primaryAndCoordinatingLimits.get();
        long limit2 = tracker2.primaryAndCoordinatingLimits.get();
        long requestStartTime = System.currentTimeMillis();
        boolean randomBoolean = randomBoolean();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        if(randomBoolean) {
            assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 6 * 1024));
            assertEquals(1, tracker1.coordinatingNodeLimitsBreachedRejections.get());
            assertEquals(0, tracker2.coordinatingNodeLimitsBreachedRejections.get());
        } else {
            assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 6 * 1024));
            assertEquals(1, tracker1.primaryNodeLimitsBreachedRejections.get());
            assertEquals(0, tracker2.primaryNodeLimitsBreachedRejections.get());
        }

        assertEquals(limit1, tracker1.primaryAndCoordinatingLimits.get());
        assertEquals(limit2, tracker2.primaryAndCoordinatingLimits.get());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitNotBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentReplicaBytes.addAndGet(5 * 1024);
        tracker2.replicaLimits.addAndGet(10 * 1024);
        long limit1 = tracker1.replicaLimits.get();
        long limit2 = tracker2.replicaLimits.get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 10 * 1024));
        assertEquals(limit1, tracker1.replicaLimits.get());
        assertEquals(limit2, tracker2.replicaLimits.get());
        assertEquals(1, tracker1.replicaNodeLimitsBreachedRejections.get());
        assertEquals(0, tracker2.replicaNodeLimitsBreachedRejections.get());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(4 * 1024);
        tracker2.primaryAndCoordinatingLimits.addAndGet(6 * 1024);
        long limit1 = tracker1.primaryAndCoordinatingLimits.get();
        long limit2 = tracker2.primaryAndCoordinatingLimits.get();
        long requestStartTime = System.currentTimeMillis();
        boolean randomBoolean = randomBoolean();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        if(randomBoolean) {
            assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(1, tracker1.coordinatingNodeLimitsBreachedRejections.get());
            assertEquals(0, tracker2.coordinatingNodeLimitsBreachedRejections.get());
        } else {
            assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(1, tracker1.primaryNodeLimitsBreachedRejections.get());
            assertEquals(0, tracker2.primaryNodeLimitsBreachedRejections.get());
        }

        assertEquals(limit1, tracker1.primaryAndCoordinatingLimits.get());
        assertEquals(limit2, tracker2.primaryAndCoordinatingLimits.get());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentReplicaBytes.addAndGet(5 * 1024);
        tracker2.replicaLimits.addAndGet(12 * 1024);
        long limit1 = tracker1.replicaLimits.get();
        long limit2 = tracker2.replicaLimits.get();
        long requestStartTime = System.currentTimeMillis();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertEquals(limit1, tracker1.replicaLimits.get());
        assertEquals(limit2, tracker2.replicaLimits.get());
        assertEquals(1, tracker1.replicaNodeLimitsBreachedRejections.get());
        assertEquals(0, tracker2.replicaNodeLimitsBreachedRejections.get());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(4 * 1024);
        tracker2.primaryAndCoordinatingLimits.addAndGet(6 * 1024);
        long limit1 = tracker1.primaryAndCoordinatingLimits.get();
        long limit2 = tracker2.primaryAndCoordinatingLimits.get();
        long requestStartTime = System.currentTimeMillis();
        boolean randomBoolean = randomBoolean();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        if(randomBoolean) {
            tracker1.lastSuccessfulCoordinatingRequestTimestamp.addAndGet(requestStartTime - 100);
            tracker1.totalOutstandingCoordinatingRequests.addAndGet(2);

            assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(1, tracker1.coordinatingLastSuccessfulRequestLimitsBreachedRejections.get());
            assertEquals(0, tracker2.coordinatingLastSuccessfulRequestLimitsBreachedRejections.get());
        } else {
            tracker1.lastSuccessfulPrimaryRequestTimestamp.addAndGet(requestStartTime - 100);
            tracker1.totalOutstandingPrimaryRequests.addAndGet(2);

            assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(1, tracker1.primaryLastSuccessfulRequestLimitsBreachedRejections.get());
            assertEquals(0, tracker2.primaryLastSuccessfulRequestLimitsBreachedRejections.get());
        }

        assertEquals(limit1, tracker1.primaryAndCoordinatingLimits.get());
        assertEquals(limit2, tracker2.primaryAndCoordinatingLimits.get());
        assertEquals(1, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentReplicaBytes.addAndGet(5 * 1024);
        tracker2.replicaLimits.addAndGet(12 * 1024);
        long limit1 = tracker1.replicaLimits.get();
        long limit2 = tracker2.replicaLimits.get();
        long requestStartTime = System.currentTimeMillis();
        tracker1.lastSuccessfulReplicaRequestTimestamp.addAndGet(requestStartTime - 100);
        tracker1.totalOutstandingReplicaRequests.addAndGet(2);

        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertEquals(limit1, tracker1.replicaLimits.get());
        assertEquals(limit2, tracker2.replicaLimits.get());
        assertEquals(1, tracker1.replicaLastSuccessfulRequestLimitsBreachedRejections.get());
        assertEquals(0, tracker2.replicaLastSuccessfulRequestLimitsBreachedRejections.get());
        assertEquals(1, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndLessOutstandingRequestsAndNoLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(1 * 1024);
        tracker2.primaryAndCoordinatingLimits.addAndGet(6 * 1024);
        long limit1 = tracker1.primaryAndCoordinatingLimits.get();
        long limit2 = tracker2.primaryAndCoordinatingLimits.get();
        long requestStartTime = System.currentTimeMillis();
        boolean randomBoolean = randomBoolean();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        if(randomBoolean) {
            tracker1.lastSuccessfulCoordinatingRequestTimestamp.addAndGet(requestStartTime - 100);
            tracker1.totalOutstandingCoordinatingRequests.addAndGet(1);

            assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(0, tracker1.coordinatingLastSuccessfulRequestLimitsBreachedRejections.get());
            assertEquals(0, tracker2.coordinatingLastSuccessfulRequestLimitsBreachedRejections.get());
        } else {
            tracker1.lastSuccessfulPrimaryRequestTimestamp.addAndGet(requestStartTime - 100);
            tracker1.totalOutstandingPrimaryRequests.addAndGet(1);

            assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(0, tracker1.primaryLastSuccessfulRequestLimitsBreachedRejections.get());
            assertEquals(0, tracker2.primaryLastSuccessfulRequestLimitsBreachedRejections.get());
        }

        assertTrue(tracker1.primaryAndCoordinatingLimits.get() > limit1);
        assertEquals((long)(1 * 1024/0.85), tracker1.primaryAndCoordinatingLimits.get());
        assertEquals(limit2, tracker2.primaryAndCoordinatingLimits.get());
        assertEquals(0, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndLessOutstandingRequestsAndNoLastSuccessfulRequestLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentReplicaBytes.addAndGet(2 * 1024);
        tracker2.replicaLimits.addAndGet(12 * 1024);
        long limit1 = tracker1.replicaLimits.get();
        long limit2 = tracker2.replicaLimits.get();
        long requestStartTime = System.currentTimeMillis();
        tracker1.lastSuccessfulReplicaRequestTimestamp.addAndGet(requestStartTime - 100);
        tracker1.totalOutstandingReplicaRequests.addAndGet(1);

        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertTrue(tracker1.replicaLimits.get() > limit1);
        assertEquals((long)(2 * 1024/0.85), tracker1.replicaLimits.get());
        assertEquals(limit2, tracker2.replicaLimits.get());
        assertEquals(0, tracker1.replicaLastSuccessfulRequestLimitsBreachedRejections.get());
        assertEquals(0, tracker2.replicaLastSuccessfulRequestLimitsBreachedRejections.get());
        assertEquals(0, memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(4 * 1024);
        tracker2.primaryAndCoordinatingLimits.addAndGet(6 * 1024);
        long limit1 = tracker1.primaryAndCoordinatingLimits.get();
        long limit2 = tracker2.primaryAndCoordinatingLimits.get();
        long requestStartTime = System.currentTimeMillis();
        boolean randomBoolean = randomBoolean();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        if(randomBoolean) {
            tracker1.coordinatingThroughputMovingAverage.addAndGet(Double.doubleToLongBits(1d));
            tracker1.totalOutstandingCoordinatingRequests.addAndGet(2);
            tracker1.totalCoordinatingBytes.addAndGet(60);
            tracker1.coordinatingTimeInMillis.addAndGet(10);
            tracker1.coordinatingThroughputMovingQueue.offer(1d);
            tracker1.coordinatingThroughputMovingQueue.offer(2d);

            assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(1, tracker1.coordinatingThroughputDegradationLimitsBreachedRejections.get());
            assertEquals(0, tracker2.coordinatingThroughputDegradationLimitsBreachedRejections.get());
        } else {
            tracker1.primaryThroughputMovingAverage.addAndGet(Double.doubleToLongBits(1d));
            tracker1.totalOutstandingPrimaryRequests.addAndGet(2);
            tracker1.totalPrimaryBytes.addAndGet(60);
            tracker1.primaryTimeInMillis.addAndGet(10);
            tracker1.primaryThroughputMovingQueue.offer(1d);
            tracker1.primaryThroughputMovingQueue.offer(2d);

            assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(1, tracker1.primaryThroughputDegradationLimitsBreachedRejections.get());
            assertEquals(0, tracker2.primaryThroughputDegradationLimitsBreachedRejections.get());
        }

        assertEquals(limit1, tracker1.primaryAndCoordinatingLimits.get());
        assertEquals(limit2, tracker2.primaryAndCoordinatingLimits.get());
        assertEquals(1, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentReplicaBytes.addAndGet(5 * 1024);
        tracker2.replicaLimits.addAndGet(12 * 1024);
        long limit1 = tracker1.replicaLimits.get();
        long limit2 = tracker2.replicaLimits.get();
        long requestStartTime = System.currentTimeMillis();
        tracker1.replicaThroughputMovingAverage.addAndGet(Double.doubleToLongBits(1d));
        tracker1.totalOutstandingReplicaRequests.addAndGet(2);
        tracker1.totalReplicaBytes.addAndGet(80);
        tracker1.replicaTimeInMillis.addAndGet(10);
        tracker1.replicaThroughputMovingQueue.offer(1d);
        tracker1.replicaThroughputMovingQueue.offer(2d);

        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertEquals(limit1, tracker1.replicaLimits.get());
        assertEquals(limit2, tracker2.replicaLimits.get());
        assertEquals(1, tracker1.replicaThroughputDegradationLimitsBreachedRejections.get());
        assertEquals(0, tracker2.replicaThroughputDegradationLimitsBreachedRejections.get());
        assertEquals(1, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndMovingAverageQueueNotBuildUpAndNoThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(1 * 1024);
        tracker2.primaryAndCoordinatingLimits.addAndGet(6 * 1024);
        long limit1 = tracker1.primaryAndCoordinatingLimits.get();
        long limit2 = tracker2.primaryAndCoordinatingLimits.get();
        long requestStartTime = System.currentTimeMillis();
        boolean randomBoolean = randomBoolean();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        if(randomBoolean) {
            tracker1.coordinatingThroughputMovingAverage.addAndGet(Double.doubleToLongBits(1d));
            tracker1.totalOutstandingCoordinatingRequests.addAndGet(1);
            tracker1.totalCoordinatingBytes.addAndGet(60);
            tracker1.coordinatingTimeInMillis.addAndGet(10);
            tracker1.coordinatingThroughputMovingQueue.offer(1d);

            assertFalse(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(0, tracker1.coordinatingThroughputDegradationLimitsBreachedRejections.get());
            assertEquals(0, tracker2.coordinatingThroughputDegradationLimitsBreachedRejections.get());
            assertEquals(0, tracker1.coordinatingNodeLimitsBreachedRejections.get());
            assertEquals(0, tracker2.coordinatingNodeLimitsBreachedRejections.get());
        } else {
            tracker1.primaryThroughputMovingAverage.addAndGet(Double.doubleToLongBits(1d));
            tracker1.totalOutstandingPrimaryRequests.addAndGet(1);
            tracker1.totalPrimaryBytes.addAndGet(60);
            tracker1.primaryTimeInMillis.addAndGet(10);
            tracker1.primaryThroughputMovingQueue.offer(1d);

            assertFalse(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(0, tracker1.primaryThroughputDegradationLimitsBreachedRejections.get());
            assertEquals(0, tracker2.primaryThroughputDegradationLimitsBreachedRejections.get());
            assertEquals(0, tracker1.primaryNodeLimitsBreachedRejections.get());
            assertEquals(0, tracker2.primaryNodeLimitsBreachedRejections.get());
        }

        assertTrue(tracker1.primaryAndCoordinatingLimits.get() > limit1);
        assertEquals((long)(1 * 1024/0.85), tracker1.primaryAndCoordinatingLimits.get());
        assertEquals(limit2, tracker2.primaryAndCoordinatingLimits.get());
        assertEquals(0, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndMovingAverageQueueNotBuildUpAndNThroughputDegradationLimitRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentReplicaBytes.addAndGet(2 * 1024);
        tracker2.replicaLimits.addAndGet(12 * 1024);
        long limit1 = tracker1.replicaLimits.get();
        long limit2 = tracker2.replicaLimits.get();
        long requestStartTime = System.currentTimeMillis();
        tracker1.replicaThroughputMovingAverage.addAndGet(Double.doubleToLongBits(1d));
        tracker1.totalOutstandingReplicaRequests.addAndGet(1);
        tracker1.totalReplicaBytes.addAndGet(80);
        tracker1.replicaTimeInMillis.addAndGet(10);
        tracker1.replicaThroughputMovingQueue.offer(1d);

        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertFalse(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertTrue(tracker1.replicaLimits.get() > limit1);
        assertEquals((long)(2 * 1024/0.85), tracker1.replicaLimits.get());
        assertEquals(limit2, tracker2.replicaLimits.get());
        assertEquals(0, tracker1.replicaThroughputDegradationLimitsBreachedRejections.get());
        assertEquals(0, tracker2.replicaThroughputDegradationLimitsBreachedRejections.get());
        assertEquals(0, tracker1.replicaNodeLimitsBreachedRejections.get());
        assertEquals(0, memoryManager.totalThroughputDegradationLimitsBreachedRejections.get());
    }

    public void testCoordinatingPrimarySoftLimitBreachedAndNoSecondaryParameterBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(4 * 1024);
        tracker2.primaryAndCoordinatingLimits.addAndGet(6 * 1024);
        long limit1 = tracker1.primaryAndCoordinatingLimits.get();
        long limit2 = tracker2.primaryAndCoordinatingLimits.get();
        long requestStartTime = System.currentTimeMillis();
        boolean randomBoolean = randomBoolean();
        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        if(randomBoolean) {
            tracker1.coordinatingThroughputMovingAverage.addAndGet(Double.doubleToLongBits(1d));
            tracker1.totalOutstandingCoordinatingRequests.addAndGet(1);
            tracker1.totalCoordinatingBytes.addAndGet(60);
            tracker1.coordinatingTimeInMillis.addAndGet(10);
            tracker1.coordinatingThroughputMovingQueue.offer(1d);

            assertTrue(memoryManager.isCoordinatingShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(1, tracker1.coordinatingNodeLimitsBreachedRejections.get());
            assertEquals(0, tracker2.coordinatingNodeLimitsBreachedRejections.get());
        } else {
            tracker1.primaryThroughputMovingAverage.addAndGet(Double.doubleToLongBits(1d));
            tracker1.totalOutstandingPrimaryRequests.addAndGet(1);
            tracker1.totalPrimaryBytes.addAndGet(60);
            tracker1.primaryTimeInMillis.addAndGet(10);
            tracker1.primaryThroughputMovingQueue.offer(1d);

            assertTrue(memoryManager.isPrimaryShardLimitBreached(tracker1, requestStartTime, hotStore, 8 * 1024));
            assertEquals(1, tracker1.primaryNodeLimitsBreachedRejections.get());
            assertEquals(0, tracker2.primaryNodeLimitsBreachedRejections.get());
        }

        assertEquals(limit1, tracker1.primaryAndCoordinatingLimits.get());
        assertEquals(limit2, tracker2.primaryAndCoordinatingLimits.get());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testReplicaShardLimitsSoftLimitBreachedAndNoSecondaryParameterBreachedAndNodeLevelRejections() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(shardId2);
        tracker1.currentReplicaBytes.addAndGet(5 * 1024);
        tracker2.replicaLimits.addAndGet(12 * 1024);
        long limit1 = tracker1.replicaLimits.get();
        long limit2 = tracker2.replicaLimits.get();
        long requestStartTime = System.currentTimeMillis();
        tracker1.replicaThroughputMovingAverage.addAndGet(Double.doubleToLongBits(1d));
        tracker1.totalOutstandingReplicaRequests.addAndGet(1);
        tracker1.totalReplicaBytes.addAndGet(80);
        tracker1.replicaTimeInMillis.addAndGet(10);
        tracker1.replicaThroughputMovingQueue.offer(1d);

        Map<Long, ShardIndexingPressureTracker> hotStore = store.getShardIndexingPressureHotStore();

        assertTrue(memoryManager.isReplicaShardLimitBreached(tracker1, requestStartTime, hotStore, 12 * 1024));
        assertEquals(limit1, tracker1.replicaLimits.get());
        assertEquals(limit2, tracker2.replicaLimits.get());
        assertEquals(1, tracker1.replicaNodeLimitsBreachedRejections.get());
        assertEquals(0, tracker2.replicaNodeLimitsBreachedRejections.get());
        assertEquals(1, memoryManager.totalNodeLimitsBreachedRejections.get());
    }

    public void testDecreaseShardPrimaryAndCoordinatingLimitsToBaseLimit() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        tracker1.primaryAndCoordinatingLimits.addAndGet(1 * 1024);
        tracker1.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(0);
        long limit1 = tracker1.primaryAndCoordinatingLimits.get();
        memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker1);

        assertTrue(tracker1.primaryAndCoordinatingLimits.get() < limit1);
        assertEquals(10, tracker1.primaryAndCoordinatingLimits.get());
    }

    public void testDecreaseShardReplicaLimitsToBaseLimit() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);

        tracker1.replicaLimits.addAndGet(1 * 1024);
        tracker1.currentReplicaBytes.addAndGet(0);
        long limit1 = tracker1.replicaLimits.get();
        memoryManager.decreaseShardReplicaLimits(tracker1);

        assertTrue(tracker1.replicaLimits.get() < limit1);
        assertEquals(15, tracker1.replicaLimits.get());
    }

    public void testDecreaseShardPrimaryAndCoordinatingLimits() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);
        tracker1.primaryAndCoordinatingLimits.addAndGet(1 * 1024);
        tracker1.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(512);
        long limit1 = tracker1.primaryAndCoordinatingLimits.get();
        memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker1);

        assertTrue(tracker1.primaryAndCoordinatingLimits.get() < limit1);
        assertEquals((long)(512/0.85), tracker1.primaryAndCoordinatingLimits.get());
    }

    public void testDecreaseShardReplicaLimits() {
        ShardIndexingPressureMemoryManager memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings,
            clusterSettings, settings);
        ShardIndexingPressureStore store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(shardId1);

        tracker1.replicaLimits.addAndGet(1 * 1024);
        tracker1.currentReplicaBytes.addAndGet(512);
        long limit1 = tracker1.replicaLimits.get();
        memoryManager.decreaseShardReplicaLimits(tracker1);

        assertTrue(tracker1.replicaLimits.get() < limit1);
        assertEquals((long)(512/0.85), tracker1.replicaLimits.get());
    }
}

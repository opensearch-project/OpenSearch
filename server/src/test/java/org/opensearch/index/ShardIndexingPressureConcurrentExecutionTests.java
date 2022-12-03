/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.stats.IndexingPressurePerShardStats;
import org.opensearch.index.stats.IndexingPressureStats;
import org.opensearch.index.stats.ShardIndexingPressureStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ShardIndexingPressureConcurrentExecutionTests extends OpenSearchTestCase {

    private final Settings settings = Settings.builder()
        .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
        .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
        .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
        .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 100)
        .build();

    private final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

    public enum OperationType {
        COORDINATING,
        PRIMARY,
        REPLICA
    }

    public void testCoordinatingPrimaryThreadedUpdateToShardLimits() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 500);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        Releasable[] releasable;
        if (randomBoolean) {
            releasable = fireConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 15, OperationType.COORDINATING);
        } else {
            releasable = fireConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 15, OperationType.PRIMARY);
        }

        if (randomBoolean) {
            assertEquals(
                NUM_THREADS * 15,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes()
            );
        } else {
            assertEquals(
                NUM_THREADS * 15,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes()
            );
        }
        assertEquals(
            NUM_THREADS * 15,
            shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes()
        );
        MatcherAssert.assertThat(
            (double) (NUM_THREADS * 15) / shardIndexingPressure.shardStats()
                .getIndexingPressureShardStats(shardId1)
                .getCurrentPrimaryAndCoordinatingLimits(),
            isInOperatingFactorRange()
        );

        for (int i = 0; i < NUM_THREADS; i++) {
            releasable[i].close();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        if (randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
        }
        assertEquals(
            0,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes()
        );
        assertEquals(
            10,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits()
        );
    }

    public void testReplicaThreadedUpdateToShardLimits() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 500);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        Releasable[] releasable = fireConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 15, OperationType.REPLICA);

        assertEquals(NUM_THREADS * 15, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        MatcherAssert.assertThat(
            (double) (NUM_THREADS * 15) / shardIndexingPressure.shardStats()
                .getIndexingPressureShardStats(shardId1)
                .getCurrentReplicaLimits(),
            isInOperatingFactorRange()
        );

        for (int i = 0; i < NUM_THREADS; i++) {
            releasable[i].close();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryThreadedSimultaneousUpdateToShardLimits() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 500);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();

        if (randomBoolean) {
            fireAndCompleteConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 100, OperationType.COORDINATING);
        } else {
            fireAndCompleteConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 100, OperationType.PRIMARY);
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);
        if (randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
        }
        assertEquals(
            0,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes()
        );
        assertEquals(
            10,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits()
        );
    }

    public void testReplicaThreadedSimultaneousUpdateToShardLimits() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 500);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        fireAndCompleteConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 100, OperationType.REPLICA);

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryThreadedUpdateToShardLimitsWithRandomBytes() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 400);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();

        if (randomBoolean) {
            fireAllThenCompleteConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 15, OperationType.COORDINATING);
        } else {
            fireAllThenCompleteConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 15, OperationType.PRIMARY);
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        if (randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
        }
        assertEquals(
            0,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes()
        );
        assertEquals(
            10,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits()
        );
    }

    public void testReplicaThreadedUpdateToShardLimitsWithRandomBytes() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 400);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        fireAllThenCompleteConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 15, OperationType.REPLICA);

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryThreadedUpdateToShardLimitsAndRejections() throws Exception {
        final int NUM_THREADS = 100;
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        AtomicInteger rejectionCount = new AtomicInteger();
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                try {
                    if (randomBoolean) {
                        releasables[counter] = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 200, false);
                    } else {
                        releasables[counter] = shardIndexingPressure.markPrimaryOperationStarted(shardId1, 200, false);
                    }
                } catch (OpenSearchRejectedExecutionException e) {
                    rejectionCount.addAndGet(1);
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        ShardIndexingPressureStats shardStats = shardIndexingPressure.shardStats();
        if (randomBoolean) {
            assertEquals(rejectionCount.get(), nodeStats.getCoordinatingRejections());
            assertTrue(shardStats.getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes() < 50 * 200);
        } else {
            assertTrue(shardStats.getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes() < 50 * 200);
            assertEquals(rejectionCount.get(), nodeStats.getPrimaryRejections());
        }
        assertTrue(nodeStats.getCurrentCombinedCoordinatingAndPrimaryBytes() < 50 * 200);
        assertTrue(shardStats.getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes() < 50 * 200);

        for (Releasable releasable : releasables) {
            if (releasable != null) {
                releasable.close();
            }
        }

        nodeStats = shardIndexingPressure.stats();
        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        // If rejection count equals NUM_THREADS that means rejections happened until the last request, then we'll get shardStoreStats which
        // was updated on the last request. In other cases, the shardStoreStats simply moves to the cold store and null is returned.
        if (rejectionCount.get() == NUM_THREADS) {
            assertEquals(10, shardStoreStats.getCurrentPrimaryAndCoordinatingLimits());
        } else {
            assertNull(shardStoreStats);
        }
        shardStats = shardIndexingPressure.coldStats();
        if (randomBoolean) {
            assertEquals(rejectionCount.get(), nodeStats.getCoordinatingRejections());
            assertEquals(
                rejectionCount.get(),
                shardStats.getIndexingPressureShardStats(shardId1).getCoordinatingNodeLimitsBreachedRejections()
            );
            assertEquals(0, shardStats.getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
        } else {
            assertEquals(rejectionCount.get(), nodeStats.getPrimaryRejections());
            assertEquals(rejectionCount.get(), shardStats.getIndexingPressureShardStats(shardId1).getPrimaryNodeLimitsBreachedRejections());
            assertEquals(0, shardStats.getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
        }

        assertEquals(0, nodeStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, shardStats.getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardStats.getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits());
    }

    public void testReplicaThreadedUpdateToShardLimitsAndRejections() throws Exception {
        final int NUM_THREADS = 100;
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        AtomicInteger rejectionCount = new AtomicInteger();
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                try {
                    releasables[counter] = shardIndexingPressure.markReplicaOperationStarted(shardId1, 300, false);
                } catch (OpenSearchRejectedExecutionException e) {
                    rejectionCount.addAndGet(1);
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        assertEquals(rejectionCount.get(), nodeStats.getReplicaRejections());
        assertTrue(nodeStats.getCurrentReplicaBytes() < 50 * 300);

        ShardIndexingPressureStats shardStats = shardIndexingPressure.shardStats();
        assertTrue(shardStats.getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes() < 50 * 300);

        for (Releasable releasable : releasables) {
            if (releasable != null) {
                releasable.close();
            }
        }

        nodeStats = shardIndexingPressure.stats();
        assertEquals(rejectionCount.get(), nodeStats.getReplicaRejections());
        assertEquals(0, nodeStats.getCurrentReplicaBytes());

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        // If rejection count equals NUM_THREADS that means rejections happened until the last request, then we'll get shardStoreStats which
        // was updated on the last request. In other cases, the shardStoreStats simply moves to the cold store and null is returned.
        if (rejectionCount.get() == NUM_THREADS) {
            assertEquals(15, shardStoreStats.getCurrentReplicaLimits());
        } else {
            assertNull(shardStoreStats);
        }

        shardStats = shardIndexingPressure.coldStats();
        assertEquals(rejectionCount.get(), shardStats.getIndexingPressureShardStats(shardId1).getReplicaNodeLimitsBreachedRejections());
        assertEquals(0, shardStats.getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardStats.getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryConcurrentUpdatesOnShardIndexingPressureTrackerObjects() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 400);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "new_uuid");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        Releasable[] releasables;
        if (randomBoolean) {
            releasables = fireConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 15, OperationType.COORDINATING);
        } else {
            releasables = fireConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 15, OperationType.PRIMARY);
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        assertThat(shardStoreStats.getCurrentPrimaryAndCoordinatingLimits(), Matchers.greaterThan(100L));

        CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);
        IndexingPressurePerShardStats shardStoreStats2 = shardIndexingPressure.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId1);
        ;
        assertEquals(shardStoreStats.getCurrentPrimaryAndCoordinatingLimits(), shardStoreStats2.getCurrentPrimaryAndCoordinatingLimits());

        statsFlag.includeOnlyTopIndexingPressureMetrics(true);
        assertNull(shardIndexingPressure.shardStats(statsFlag).getIndexingPressureShardStats(shardId1));
        statsFlag.includeOnlyTopIndexingPressureMetrics(false);

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        // No object in host store as no active shards
        shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        if (randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
        }
        assertEquals(
            0,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes()
        );
        assertEquals(
            10,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits()
        );

        shardStoreStats2 = shardIndexingPressure.shardStats(statsFlag).getIndexingPressureShardStats(shardId1);
        assertEquals(
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits(),
            shardStoreStats2.getCurrentPrimaryAndCoordinatingLimits()
        );

        statsFlag.includeAllShardIndexingPressureTrackers(false);
        assertNull(shardIndexingPressure.shardStats(statsFlag).getIndexingPressureShardStats(shardId1));
    }

    public void testReplicaConcurrentUpdatesOnShardIndexingPressureTrackerObjects() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 400);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "new_uuid");
        ShardId shardId1 = new ShardId(index, 0);

        final Releasable[] releasables = fireConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 20, OperationType.REPLICA);

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        assertThat(shardStoreStats.getCurrentReplicaLimits(), Matchers.greaterThan(100L));

        CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);
        IndexingPressurePerShardStats shardStoreStats2 = shardIndexingPressure.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId1);
        ;
        assertEquals(shardStoreStats.getCurrentReplicaLimits(), shardStoreStats2.getCurrentReplicaLimits());

        statsFlag.includeOnlyTopIndexingPressureMetrics(true);
        assertNull(shardIndexingPressure.shardStats(statsFlag).getIndexingPressureShardStats(shardId1));
        statsFlag.includeOnlyTopIndexingPressureMetrics(false);

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        // No object in host store as no active shards
        shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());

        shardStoreStats2 = shardIndexingPressure.shardStats(statsFlag).getIndexingPressureShardStats(shardId1);
        ;
        assertEquals(
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits(),
            shardStoreStats2.getCurrentReplicaLimits()
        );

        statsFlag.includeAllShardIndexingPressureTrackers(false);
        assertNull(shardIndexingPressure.shardStats(statsFlag).getIndexingPressureShardStats(shardId1));
    }

    public void testCoordinatingPrimaryThreadedThroughputDegradationAndRejection() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "15KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 80)
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(80, 100);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();

        // Generating a concurrent + sequential load to have a fair throughput
        if (randomBoolean) {
            fireConcurrentAndParallelRequestsForUniformThroughPut(
                NUM_THREADS,
                shardIndexingPressure,
                shardId1,
                100,
                100,
                OperationType.COORDINATING
            );
        } else {
            fireConcurrentAndParallelRequestsForUniformThroughPut(
                NUM_THREADS,
                shardIndexingPressure,
                shardId1,
                100,
                100,
                OperationType.PRIMARY
            );
        }

        // Generating a load to such that the requests in the window shows degradation in throughput.
        if (randomBoolean) {
            fireAllThenCompleteConcurrentRequestsWithUniformDelay(
                ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.get(settings),
                shardIndexingPressure,
                shardId1,
                100,
                200,
                OperationType.COORDINATING
            );
        } else {
            fireAllThenCompleteConcurrentRequestsWithUniformDelay(
                ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.get(settings),
                shardIndexingPressure,
                shardId1,
                100,
                200,
                OperationType.PRIMARY
            );
        }

        // Generate a load which breaches both primary parameter
        if (randomBoolean) {
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 11 * 1024, false)
            );

            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingRejections());
            assertEquals(
                1,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getCoordinatingThroughputDegradationLimitsBreachedRejections()
            );
            assertEquals(
                0,
                shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingNodeLimitsBreachedRejections()
            );
            assertEquals(
                0,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getCoordinatingLastSuccessfulRequestLimitsBreachedRejections()
            );
        } else {
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> shardIndexingPressure.markPrimaryOperationStarted(shardId1, 11 * 1024, false)
            );

            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryRejections());
            assertEquals(
                1,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getPrimaryThroughputDegradationLimitsBreachedRejections()
            );
            assertEquals(
                0,
                shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryNodeLimitsBreachedRejections()
            );
            assertEquals(
                0,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getPrimaryLastSuccessfulRequestLimitsBreachedRejections()
            );
        }
        assertEquals(
            0,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes()
        );
        assertEquals(
            15,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits()
        );
    }

    public void testReplicaThreadedThroughputDegradationAndRejection() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 100)
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(80, 100);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);

        // Generating a load to have a fair throughput
        fireConcurrentAndParallelRequestsForUniformThroughPut(
            NUM_THREADS,
            shardIndexingPressure,
            shardId1,
            100,
            100,
            OperationType.REPLICA
        );

        // Generating a load to such that the requests in the window shows degradation in throughput.
        fireAllThenCompleteConcurrentRequestsWithUniformDelay(
            ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.get(settings),
            shardIndexingPressure,
            shardId1,
            100,
            200,
            OperationType.REPLICA
        );

        // Generate a load which breaches both primary parameter
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> shardIndexingPressure.markReplicaOperationStarted(shardId1, 11 * 1024, false)
        );

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
        assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaRejections());
        assertEquals(
            1,
            shardIndexingPressure.coldStats()
                .getIndexingPressureShardStats(shardId1)
                .getReplicaThroughputDegradationLimitsBreachedRejections()
        );
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaNodeLimitsBreachedRejections());
        assertEquals(
            0,
            shardIndexingPressure.coldStats()
                .getIndexingPressureShardStats(shardId1)
                .getReplicaLastSuccessfulRequestLimitsBreachedRejections()
        );
    }

    public void testCoordinatingPrimaryThreadedLastSuccessfulRequestsAndRejection() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "250KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 100)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(110, 150);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();

        // One request being successful
        if (randomBoolean) {
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 10, false);
            coordinating.close();
        } else {
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId1, 10, false);
            primary.close();
        }

        // Generating a load such that requests are blocked requests.
        Releasable[] releasables;
        if (randomBoolean) {
            releasables = fireConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 10, OperationType.COORDINATING);
        } else {
            releasables = fireConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 10, OperationType.PRIMARY);
        }

        // Mimic the time elapsed after requests being stuck
        Thread.sleep(randomIntBetween(50, 100));

        // Generate a load which breaches both primary parameter
        if (randomBoolean) {
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 200 * 1024, false)
            );
        } else {
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> shardIndexingPressure.markPrimaryOperationStarted(shardId1, 200 * 1024, false)
            );
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        if (randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingRejections());
            assertEquals(
                0,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getCoordinatingThroughputDegradationLimitsBreachedRejections()
            );
            assertEquals(
                0,
                shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingNodeLimitsBreachedRejections()
            );
            assertEquals(
                1,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getCoordinatingLastSuccessfulRequestLimitsBreachedRejections()
            );
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryRejections());
            assertEquals(
                0,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getPrimaryThroughputDegradationLimitsBreachedRejections()
            );
            assertEquals(
                0,
                shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryNodeLimitsBreachedRejections()
            );
            assertEquals(
                1,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getPrimaryLastSuccessfulRequestLimitsBreachedRejections()
            );
        }
        assertEquals(
            0,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes()
        );
        assertEquals(
            256,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits()
        );
    }

    public void testReplicaThreadedLastSuccessfulRequestsAndRejection() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "250KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 100)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(110, 150);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);

        // One request being successful
        Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId1, 10, false);
        replica.close();

        // Generating a load such that requests are blocked requests.
        final Releasable[] releasables = fireConcurrentRequests(NUM_THREADS, shardIndexingPressure, shardId1, 10, OperationType.REPLICA);
        // Mimic the time elapsed after requests being stuck
        Thread.sleep(randomIntBetween(50, 100));

        // Generate a load which breaches both primary parameter
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> shardIndexingPressure.markReplicaOperationStarted(shardId1, 300 * 1024, false)
        );

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaRejections());
        assertEquals(
            0,
            shardIndexingPressure.coldStats()
                .getIndexingPressureShardStats(shardId1)
                .getReplicaThroughputDegradationLimitsBreachedRejections()
        );
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaNodeLimitsBreachedRejections());
        assertEquals(
            1,
            shardIndexingPressure.coldStats()
                .getIndexingPressureShardStats(shardId1)
                .getReplicaLastSuccessfulRequestLimitsBreachedRejections()
        );
        assertEquals(384, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryThreadedNodeLimitsAndRejection() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "250KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 100)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(100, 150);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();

        // Generating a load to such that the requests in the window shows degradation in throughput.
        Releasable[] releasables;
        if (randomBoolean) {
            releasables = fireConcurrentRequestsWithUniformDelay(
                NUM_THREADS,
                shardIndexingPressure,
                shardId1,
                10,
                randomIntBetween(50, 100),
                OperationType.COORDINATING
            );
        } else {
            releasables = fireConcurrentRequestsWithUniformDelay(
                NUM_THREADS,
                shardIndexingPressure,
                shardId1,
                10,
                randomIntBetween(50, 100),
                OperationType.PRIMARY
            );
        }

        // Generate a load which breaches both primary parameter
        if (randomBoolean) {
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 240 * 1024, false)
            );
        } else {
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> shardIndexingPressure.markPrimaryOperationStarted(shardId1, 240 * 1024, false)
            );
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        if (randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingRejections());
            assertEquals(
                0,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getCoordinatingThroughputDegradationLimitsBreachedRejections()
            );
            assertEquals(
                1,
                shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingNodeLimitsBreachedRejections()
            );
            assertEquals(
                0,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getCoordinatingLastSuccessfulRequestLimitsBreachedRejections()
            );
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryRejections());
            assertEquals(
                0,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getPrimaryThroughputDegradationLimitsBreachedRejections()
            );
            assertEquals(
                1,
                shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryNodeLimitsBreachedRejections()
            );
            assertEquals(
                0,
                shardIndexingPressure.coldStats()
                    .getIndexingPressureShardStats(shardId1)
                    .getPrimaryLastSuccessfulRequestLimitsBreachedRejections()
            );
        }
        assertEquals(
            0,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes()
        );
        assertEquals(
            256,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits()
        );
    }

    public void testReplicaThreadedNodeLimitsAndRejection() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "250KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 100)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(100, 150);
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);

        // Generating a load to such that the requests in the window shows degradation in throughput.
        final Releasable[] releasables = fireConcurrentRequestsWithUniformDelay(
            NUM_THREADS,
            shardIndexingPressure,
            shardId1,
            10,
            randomIntBetween(50, 100),
            OperationType.COORDINATING
        );

        // Generate a load which breaches both primary parameter
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> shardIndexingPressure.markReplicaOperationStarted(shardId1, 340 * 1024, false)
        );

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaRejections());
        assertEquals(
            0,
            shardIndexingPressure.coldStats()
                .getIndexingPressureShardStats(shardId1)
                .getReplicaThroughputDegradationLimitsBreachedRejections()
        );
        assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaNodeLimitsBreachedRejections());
        assertEquals(
            0,
            shardIndexingPressure.coldStats()
                .getIndexingPressureShardStats(shardId1)
                .getReplicaLastSuccessfulRequestLimitsBreachedRejections()
        );
        assertEquals(384, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    private void fireAndCompleteConcurrentRequests(
        int concurrency,
        ShardIndexingPressure shardIndexingPressure,
        ShardId shardId,
        long bytes,
        OperationType operationType
    ) throws Exception {
        fireAndCompleteConcurrentRequestsWithUniformDelay(
            concurrency,
            shardIndexingPressure,
            shardId,
            bytes,
            randomIntBetween(5, 15),
            operationType
        );
    }

    private void fireAndCompleteConcurrentRequestsWithUniformDelay(
        int concurrency,
        ShardIndexingPressure shardIndexingPressure,
        ShardId shardId,
        long bytes,
        long delay,
        OperationType operationType
    ) throws Exception {
        final Thread[] threads = new Thread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            threads[i] = new Thread(() -> {
                if (operationType == OperationType.COORDINATING) {
                    Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, bytes, false);
                    coordinating.close();
                } else if (operationType == OperationType.PRIMARY) {
                    Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, bytes, false);
                    primary.close();
                } else {
                    Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, bytes, false);
                    replica.close();
                }
            });
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                // Do Nothing
            }
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
    }

    private Releasable[] fireConcurrentRequests(
        int concurrency,
        ShardIndexingPressure shardIndexingPressure,
        ShardId shardId,
        long bytes,
        OperationType operationType
    ) throws Exception {
        return fireConcurrentRequestsWithUniformDelay(concurrency, shardIndexingPressure, shardId, bytes, 0, operationType);
    }

    private Releasable[] fireConcurrentRequestsWithUniformDelay(
        int concurrency,
        ShardIndexingPressure shardIndexingPressure,
        ShardId shardId,
        long bytes,
        long delay,
        OperationType operationType
    ) throws Exception {
        final Thread[] threads = new Thread[concurrency];
        final Releasable[] releasable = new Releasable[concurrency];
        for (int i = 0; i < concurrency; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                if (operationType == OperationType.COORDINATING) {
                    releasable[counter] = shardIndexingPressure.markCoordinatingOperationStarted(shardId, bytes, false);
                } else if (operationType == OperationType.PRIMARY) {
                    releasable[counter] = shardIndexingPressure.markPrimaryOperationStarted(shardId, bytes, false);
                } else {
                    releasable[counter] = shardIndexingPressure.markReplicaOperationStarted(shardId, bytes, false);
                }
                try {
                    Thread.sleep(delay);
                } catch (Exception e) {
                    // Do Nothing
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        return releasable;
    }

    private void fireAllThenCompleteConcurrentRequests(
        int concurrency,
        ShardIndexingPressure shardIndexingPressure,
        ShardId shardId,
        long bytes,
        OperationType operationType
    ) throws Exception {

        fireAllThenCompleteConcurrentRequestsWithUniformDelay(concurrency, shardIndexingPressure, shardId, bytes, 0, operationType);
    }

    private void fireAllThenCompleteConcurrentRequestsWithUniformDelay(
        int concurrency,
        ShardIndexingPressure shardIndexingPressure,
        ShardId shardId,
        long bytes,
        long delay,
        OperationType operationType
    ) throws Exception {

        final Releasable[] releasable = fireConcurrentRequestsWithUniformDelay(
            concurrency,
            shardIndexingPressure,
            shardId,
            bytes,
            delay,
            operationType
        );
        for (int i = 0; i < concurrency; i++) {
            releasable[i].close();
        }
    }

    private void fireConcurrentAndParallelRequestsForUniformThroughPut(
        int concurrency,
        ShardIndexingPressure shardIndexingPressure,
        ShardId shardId,
        long bytes,
        long delay,
        OperationType operationType
    ) throws Exception {
        final Thread[] threads = new Thread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < randomIntBetween(400, 500); j++) {
                    Releasable releasable;
                    if (operationType == OperationType.COORDINATING) {
                        releasable = shardIndexingPressure.markCoordinatingOperationStarted(shardId, bytes, false);
                    } else if (operationType == OperationType.PRIMARY) {
                        releasable = shardIndexingPressure.markPrimaryOperationStarted(shardId, bytes, false);
                    } else {
                        releasable = shardIndexingPressure.markReplicaOperationStarted(shardId, bytes, false);
                    }
                    try {
                        Thread.sleep(delay);
                    } catch (Exception e) {
                        // Do Nothing
                    }
                    releasable.close();
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    private Matcher<Double> isInOperatingFactorRange() {
        return allOf(
            greaterThan(ShardIndexingPressureMemoryManager.LOWER_OPERATING_FACTOR.get(settings)),
            lessThanOrEqualTo(ShardIndexingPressureMemoryManager.UPPER_OPERATING_FACTOR.get(settings))
        );
    }
}

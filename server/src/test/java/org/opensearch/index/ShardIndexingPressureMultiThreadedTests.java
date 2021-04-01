/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

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
import org.hamcrest.Matchers;

import java.util.concurrent.atomic.AtomicInteger;

public class ShardIndexingPressureMultiThreadedTests extends OpenSearchTestCase {

    private final Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
        .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
        .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), 20)
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
        .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 100)
        .build();

    final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    final ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

    public void testCoordinatingPrimaryThreadedUpdateToShardLimits() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 500);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                if(randomBoolean){
                    releasables[counter] = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 15, false);
                } else {
                    releasables[counter] = shardIndexingPressure.markPrimaryOperationStarted(shardId1, 15, false);
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        if(randomBoolean) {
            assertEquals(NUM_THREADS * 15, shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
        } else {
            assertEquals(NUM_THREADS * 15, shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
        }
        assertEquals(NUM_THREADS * 15, shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertTrue((double) (NUM_THREADS * 15) / shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits() < 0.95);
        assertTrue((double) (NUM_THREADS * 15) / shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits() > 0.75);

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        if(randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
        }
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits());
    }

    public void testReplicaThreadedUpdateToShardLimits() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 500);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                releasables[counter] = shardIndexingPressure.markReplicaOperationStarted(shardId1, 15, false);
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        assertEquals(NUM_THREADS * 15, shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertTrue((double)(NUM_THREADS * 15) / shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits() < 0.95);
        assertTrue((double)(NUM_THREADS * 15) / shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits() > 0.75);

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryThreadedSimultaneousUpdateToShardLimits() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 500);
        final Thread[] threads = new Thread[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(() -> {
                if(randomBoolean) {
                    Releasable coodinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 100, false);
                    coodinating.close();
                } else {
                    Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId1, 100, false);
                    primary.close();
                }
            });
            try {
                Thread.sleep(randomIntBetween(5, 15));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);
        if(randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
        }
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits());
    }

    public void testReplicaThreadedSimultaneousUpdateToShardLimits() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 500);
        final Thread[] threads = new Thread[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(() -> {
                Releasable coodinating = shardIndexingPressure.markReplicaOperationStarted(shardId1, 100, false);
                coodinating.close();
            });
            try {
                Thread.sleep(randomIntBetween(5, 15));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryThreadedUpdateToShardLimitsWithRandomBytes() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 400);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                if(randomBoolean) {
                    releasables[counter] = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, scaledRandomIntBetween(1, 20), false);
                } else {
                    releasables[counter] = shardIndexingPressure.markPrimaryOperationStarted(shardId1, scaledRandomIntBetween(1, 20), false);
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        if(randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
        }
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits());
    }

    public void testReplicaThreadedUpdateToShardLimitsWithRandomBytes() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 400);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                releasables[counter] = shardIndexingPressure.markReplicaOperationStarted(shardId1, scaledRandomIntBetween(1, 20), false);
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryThreadedUpdateToShardLimitsAndRejections() throws Exception {
        final int NUM_THREADS = 100;
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        AtomicInteger rejectionCount = new AtomicInteger();
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                try {
                    if(randomBoolean) {
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

        IndexingPressureStats nodeStats = indexingPressure.stats();
        ShardIndexingPressureStats shardStats = shardIndexingPressure.stats();
        if(randomBoolean) {
            assertEquals(rejectionCount.get(), nodeStats.getCoordinatingRejections());
            assertTrue(shardStats.getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes() < 50 * 200);
        } else {
            assertTrue(shardStats.getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes() < 50 * 200);
            assertEquals(rejectionCount.get(), nodeStats.getPrimaryRejections());
        }
        assertTrue(nodeStats.getCurrentCombinedCoordinatingAndPrimaryBytes() < 50 * 200);
        assertTrue(shardStats.getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes() < 50 * 200);

        for (int i = 0; i < NUM_THREADS - rejectionCount.get(); i++) {
            releasables[i].close();
        }

        nodeStats = indexingPressure.stats();
        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);
        shardStats = shardIndexingPressure.coldStats();
        if(randomBoolean) {
            assertEquals(rejectionCount.get(), nodeStats.getCoordinatingRejections());
            assertEquals(rejectionCount.get(), shardStats.getIndexingPressureShardStats(shardId1).getCoordinatingNodeLimitsBreachedRejections());
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
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
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

        IndexingPressureStats nodeStats = indexingPressure.stats();
        assertEquals(rejectionCount.get(), nodeStats.getReplicaRejections());
        assertTrue(nodeStats.getCurrentReplicaBytes() < 50 * 300);

        ShardIndexingPressureStats shardStats = shardIndexingPressure.stats();
        assertTrue(shardStats.getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes() < 50 * 300);

        for (int i = 0; i < releasables.length - 1; i++) {
            if(releasables[i] != null) {
                releasables[i].close();
            }
        }

        nodeStats = indexingPressure.stats();
        assertEquals(rejectionCount.get(), nodeStats.getReplicaRejections());
        assertEquals(0, nodeStats.getCurrentReplicaBytes());

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        shardStats = shardIndexingPressure.coldStats();
        assertEquals(rejectionCount.get(), shardStats.getIndexingPressureShardStats(shardId1).getReplicaNodeLimitsBreachedRejections());
        assertEquals(0, shardStats.getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardStats.getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryConcurrentUpdatesOnShardIndexingPressureTrackerObjects() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 400);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "new_uuid");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                if(randomBoolean) {
                    releasables[counter] = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, scaledRandomIntBetween(1, 20), false);
                } else {
                    releasables[counter] = shardIndexingPressure.markPrimaryOperationStarted(shardId1, scaledRandomIntBetween(1, 20), false);
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertThat(shardStoreStats.getCurrentPrimaryAndCoordinatingLimits(), Matchers.greaterThan(100l));

        CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);
        IndexingPressurePerShardStats shardStoreStats2 = shardIndexingPressure.stats(statsFlag).getIndexingPressureShardStats(shardId1);;
        assertEquals(shardStoreStats.getCurrentPrimaryAndCoordinatingLimits(), shardStoreStats2.getCurrentPrimaryAndCoordinatingLimits());

        statsFlag.includeOnlyTopIndexingPressureMetrics(true);
        assertNull(shardIndexingPressure.stats(statsFlag).getIndexingPressureShardStats(shardId1));
        statsFlag.includeOnlyTopIndexingPressureMetrics(false);

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        //No object in host store as no active shards
        shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        if(randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
        }
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits());

        shardStoreStats2 = shardIndexingPressure.stats(statsFlag).getIndexingPressureShardStats(shardId1);
        assertEquals(shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits(),
            shardStoreStats2.getCurrentPrimaryAndCoordinatingLimits());

        statsFlag.includeAllShardIndexingPressureTrackers(false);
        assertNull(shardIndexingPressure.stats(statsFlag).getIndexingPressureShardStats(shardId1));
    }

    public void testReplicaConcurrentUpdatesOnShardIndexingPressureTrackerObjects() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(100, 400);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "new_uuid");
        ShardId shardId1 = new ShardId(index, 0);
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                releasables[counter] = shardIndexingPressure.markReplicaOperationStarted(shardId1, scaledRandomIntBetween(1, 20), false);
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertThat(shardStoreStats.getCurrentReplicaLimits(), Matchers.greaterThan(100l));

        CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);
        IndexingPressurePerShardStats shardStoreStats2 = shardIndexingPressure.stats(statsFlag).getIndexingPressureShardStats(shardId1);;
        assertEquals(shardStoreStats.getCurrentReplicaLimits(), shardStoreStats2.getCurrentReplicaLimits());

        statsFlag.includeOnlyTopIndexingPressureMetrics(true);
        assertNull(shardIndexingPressure.stats(statsFlag).getIndexingPressureShardStats(shardId1));
        statsFlag.includeOnlyTopIndexingPressureMetrics(false);

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        //No object in host store as no active shards
        shardStoreStats = shardIndexingPressure.stats().getIndexingPressureShardStats(shardId1);
        assertNull(shardStoreStats);

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());

        shardStoreStats2 = shardIndexingPressure.stats(statsFlag).getIndexingPressureShardStats(shardId1);;
        assertEquals(shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits(),
            shardStoreStats2.getCurrentReplicaLimits());

        statsFlag.includeAllShardIndexingPressureTrackers(false);
        assertNull(shardIndexingPressure.stats(statsFlag).getIndexingPressureShardStats(shardId1));
    }

    public void testCoordinatingPrimaryThreadedThroughputDegradationAndRejection() throws Exception {
        Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "15KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 100)
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(100, 120);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();

        //Generating a load to have a fair throughput
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < randomIntBetween(400, 500); j++) {
                    Releasable releasable;
                    if(randomBoolean) {
                        releasable = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 100, false);
                    } else {
                        releasable = shardIndexingPressure.markPrimaryOperationStarted(shardId1, 100, false);
                    }
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                        //Do Nothing
                    }
                    releasable.close();
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        //Generating a load to such that the requests in the window shows degradation in throughput.
        for (int i = 0; i < ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.get(settings).intValue(); i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                if(randomBoolean) {
                    releasables[counter] = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 100, false);
                } else {
                    releasables[counter] = shardIndexingPressure.markPrimaryOperationStarted(shardId1, 100, false);
                }
                try {
                    Thread.sleep(200);
                } catch (Exception e) {
                    //Do Nothing
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        for (int i = 0; i < ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.get(settings).intValue(); i++) {
            releasables[i].close();
        }

        //Generate a load which breaches both primary parameter
        if(randomBoolean) {
            expectThrows(OpenSearchRejectedExecutionException.class, () -> shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 11 * 1024, false));

            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingRejections());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingThroughputDegradationLimitsBreachedRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingNodeLimitsBreachedRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());
        } else {
            expectThrows(OpenSearchRejectedExecutionException.class, () -> shardIndexingPressure.markPrimaryOperationStarted(shardId1, 11 * 1024, false));

            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryRejections());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryThroughputDegradationLimitsBreachedRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryNodeLimitsBreachedRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryLastSuccessfulRequestLimitsBreachedRejections());
        }
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits());
    }

    public void testReplicaThreadedThroughputDegradationAndRejection() throws Exception {
        Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 100)
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(100, 120);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);

        //Generating a load to have a fair throughput
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < randomIntBetween(400, 500); j++) {
                    Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId1,  100, false);
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                        //Do Nothing
                    }
                    replica.close();
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        //Generating a load to such that the requests in the window shows degradation in throughput.
        for (int i = 0; i < ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.get(settings).intValue(); i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                releasables[counter] = shardIndexingPressure.markReplicaOperationStarted(shardId1, 100, false);
                try {
                    Thread.sleep(200);
                } catch (Exception e) {
                    //Do Nothing
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        for (int i = 0; i < ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.get(settings).intValue(); i++) {
            releasables[i].close();
        }

        //Generate a load which breaches both primary parameter
        expectThrows(OpenSearchRejectedExecutionException.class, () -> shardIndexingPressure.markReplicaOperationStarted(shardId1, 11 * 1024, false));

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(15, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
        assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaRejections());
        assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaNodeLimitsBreachedRejections());
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaLastSuccessfulRequestLimitsBreachedRejections());
    }

    public void testCoordinatingPrimaryThreadedLastSuccessfulRequestsAndRejection() throws Exception {
        Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "250KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 100)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), 20)
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(100, 150);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();

        //One request being successful
        if(randomBoolean) {
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 10, false);
            coordinating.close();
        } else {
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId1, 10, false);
            primary.close();
        }

        //Generating a load such that requests are blocked requests.
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                if(randomBoolean) {
                    releasables[counter] = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 10, false);
                } else {
                    releasables[counter] = shardIndexingPressure.markPrimaryOperationStarted(shardId1, 10, false);
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        //Mimic the time elapsed after requests being stuck
        Thread.sleep(randomIntBetween(50, 100));

        //Generate a load which breaches both primary parameter
        if(randomBoolean) {
            expectThrows(OpenSearchRejectedExecutionException.class, () -> shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 200 * 1024, false));
        } else {
            expectThrows(OpenSearchRejectedExecutionException.class, () -> shardIndexingPressure.markPrimaryOperationStarted(shardId1, 200 * 1024, false));
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        if(randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingThroughputDegradationLimitsBreachedRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingNodeLimitsBreachedRejections());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryThroughputDegradationLimitsBreachedRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryNodeLimitsBreachedRejections());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryLastSuccessfulRequestLimitsBreachedRejections());
        }
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(256, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits());
    }

    public void testReplicaThreadedLastSuccessfulRequestsAndRejection() throws Exception {
        Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "250KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 100)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), 20)
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(100, 150);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);

        //One request being successful
        Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId1, 10, false);
        replica.close();

        //Generating a load such that requests are blocked requests.
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                releasables[counter] = shardIndexingPressure.markReplicaOperationStarted(shardId1, 10, false);
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        //Mimic the time elapsed after requests being stuck
        Thread.sleep(randomIntBetween(50, 100));

        //Generate a load which breaches both primary parameter
        expectThrows(OpenSearchRejectedExecutionException.class, () -> shardIndexingPressure.markReplicaOperationStarted(shardId1, 300 * 1024, false));


        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaRejections());
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaNodeLimitsBreachedRejections());
        assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(384, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryThreadedNodeLimitsAndRejection() throws Exception {
        Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "250KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 100)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), 20)
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(100, 150);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();

        //Generating a load to such that the requests in the window shows degradation in throughput.
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                if(randomBoolean) {
                    releasables[counter] = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 10, false);
                } else {
                    releasables[counter] = shardIndexingPressure.markPrimaryOperationStarted(shardId1, 10, false);
                }
                try {
                    Thread.sleep(randomIntBetween(50, 100));
                } catch (Exception e) {
                    //Do Nothing
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        //Generate a load which breaches both primary parameter
        if(randomBoolean) {
            expectThrows(OpenSearchRejectedExecutionException.class, () -> shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 240 * 1024, false));
        } else {
            expectThrows(OpenSearchRejectedExecutionException.class, () -> shardIndexingPressure.markPrimaryOperationStarted(shardId1, 240 * 1024, false));
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        if(randomBoolean) {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingThroughputDegradationLimitsBreachedRejections());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingNodeLimitsBreachedRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());
        } else {
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryBytes());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryThroughputDegradationLimitsBreachedRejections());
            assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryNodeLimitsBreachedRejections());
            assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getPrimaryLastSuccessfulRequestLimitsBreachedRejections());
        }
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(256, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits());
    }

    public void testReplicaThreadedNodeLimitsAndRejection() throws Exception {
        Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "250KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 100)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), 20)
            .build();
        final int NUM_THREADS = scaledRandomIntBetween(100, 150);
        final Thread[] threads = new Thread[NUM_THREADS];
        final Releasable[] releasables = new Releasable[NUM_THREADS];
        IndexingPressure indexingPressure = new IndexingPressure(settings, clusterService);
        ShardIndexingPressure shardIndexingPressure = indexingPressure.getShardIndexingPressure();
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);

        //One request being successful
        Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId1, 10, false);
        replica.close();

        //Generating a load to such that the requests in the window shows degradation in throughput.
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                releasables[counter] = shardIndexingPressure.markReplicaOperationStarted(shardId1, 10, false);
                try {
                    Thread.sleep(randomIntBetween(50, 100));
                } catch (Exception e) {
                    //Do Nothing
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        //Generate a load which breaches both primary parameter
        expectThrows(OpenSearchRejectedExecutionException.class, () -> shardIndexingPressure.markReplicaOperationStarted(shardId1, 340 * 1024, false));


        for (int i = 0; i < NUM_THREADS; i++) {
            releasables[i].close();
        }

        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaBytes());
        assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaRejections());
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaNodeLimitsBreachedRejections());
        assertEquals(1, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getReplicaLastSuccessfulRequestLimitsBreachedRejections());
        assertEquals(384, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1).getCurrentReplicaLimits());
    }

}

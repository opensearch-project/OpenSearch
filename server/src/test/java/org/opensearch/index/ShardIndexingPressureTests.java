/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.stats.IndexingPressurePerShardStats;
import org.opensearch.index.stats.IndexingPressureStats;
import org.opensearch.test.OpenSearchTestCase;

public class ShardIndexingPressureTests extends OpenSearchTestCase {
    private final Settings settings = Settings.builder()
        .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
        .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
        .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
        .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 100)
        .build();

    final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    final ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

    public void testMemoryBytesMarkedAndReleased() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 10, false);
            Releasable coordinating2 = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 50, false);
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 15, true);
            Releasable primary2 = shardIndexingPressure.markPrimaryOperationStarted(shardId, 5, false);
            Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 25, true);
            Releasable replica2 = shardIndexingPressure.markReplicaOperationStarted(shardId, 10, false)
        ) {
            IndexingPressureStats nodeStats = shardIndexingPressure.stats();
            assertEquals(60, nodeStats.getCurrentCoordinatingBytes());
            assertEquals(20, nodeStats.getCurrentPrimaryBytes());
            assertEquals(80, nodeStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(35, nodeStats.getCurrentReplicaBytes());

            IndexingPressurePerShardStats shardStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
            assertEquals(60, shardStats.getCurrentCoordinatingBytes());
            assertEquals(20, shardStats.getCurrentPrimaryBytes());
            assertEquals(80, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(35, shardStats.getCurrentReplicaBytes());

        }
        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        assertEquals(0, nodeStats.getCurrentCoordinatingBytes());
        assertEquals(0, nodeStats.getCurrentPrimaryBytes());
        assertEquals(0, nodeStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, nodeStats.getCurrentReplicaBytes());
        assertEquals(60, nodeStats.getTotalCoordinatingBytes());
        assertEquals(20, nodeStats.getTotalPrimaryBytes());
        assertEquals(80, nodeStats.getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(35, nodeStats.getTotalReplicaBytes());

        IndexingPressurePerShardStats shardHotStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
        assertNull(shardHotStoreStats);

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getCurrentCoordinatingBytes());
        assertEquals(0, shardStats.getCurrentPrimaryBytes());
        assertEquals(0, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(60, shardStats.getTotalCoordinatingBytes());
        assertEquals(20, shardStats.getTotalPrimaryBytes());
        assertEquals(80, shardStats.getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(35, shardStats.getTotalReplicaBytes());
    }

    public void testAvoidDoubleAccounting() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 10, false);
            Releasable primary = shardIndexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(shardId, 15)
        ) {
            IndexingPressureStats nodeStats = shardIndexingPressure.stats();
            assertEquals(10, nodeStats.getCurrentCoordinatingBytes());
            assertEquals(15, nodeStats.getCurrentPrimaryBytes());
            assertEquals(10, nodeStats.getCurrentCombinedCoordinatingAndPrimaryBytes());

            IndexingPressurePerShardStats shardStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
            assertEquals(10, shardStats.getCurrentCoordinatingBytes());
            assertEquals(15, shardStats.getCurrentPrimaryBytes());
            assertEquals(10, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        }
        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        assertEquals(0, nodeStats.getCurrentCoordinatingBytes());
        assertEquals(0, nodeStats.getCurrentPrimaryBytes());
        assertEquals(0, nodeStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, nodeStats.getTotalCoordinatingBytes());
        assertEquals(15, nodeStats.getTotalPrimaryBytes());
        assertEquals(10, nodeStats.getTotalCombinedCoordinatingAndPrimaryBytes());

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
        assertNull(shardStoreStats);

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getCurrentCoordinatingBytes());
        assertEquals(0, shardStats.getCurrentPrimaryBytes());
        assertEquals(0, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardStats.getTotalCoordinatingBytes());
        assertEquals(15, shardStats.getTotalPrimaryBytes());
        assertEquals(10, shardStats.getTotalCombinedCoordinatingAndPrimaryBytes());
    }

    public void testCoordinatingPrimaryRejections() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1024 * 3, false);
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1024 * 3, false);
            Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 1024 * 3, false)
        ) {
            if (randomBoolean()) {
                expectThrows(
                    OpenSearchRejectedExecutionException.class,
                    () -> shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1024 * 2, false)
                );
                IndexingPressureStats nodeStats = shardIndexingPressure.stats();
                assertEquals(1, nodeStats.getCoordinatingRejections());
                assertEquals(1024 * 6, nodeStats.getCurrentCombinedCoordinatingAndPrimaryBytes());

                IndexingPressurePerShardStats shardStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
                assertEquals(1, shardStats.getCoordinatingRejections());
                assertEquals(1024 * 6, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(1, shardStats.getCoordinatingNodeLimitsBreachedRejections());
            } else {
                expectThrows(
                    OpenSearchRejectedExecutionException.class,
                    () -> shardIndexingPressure.markPrimaryOperationStarted(shardId, 1024 * 2, false)
                );
                IndexingPressureStats nodeStats = shardIndexingPressure.stats();
                assertEquals(1, nodeStats.getPrimaryRejections());
                assertEquals(1024 * 6, nodeStats.getCurrentCombinedCoordinatingAndPrimaryBytes());

                IndexingPressurePerShardStats shardStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
                assertEquals(1, shardStats.getPrimaryRejections());
                assertEquals(1024 * 6, nodeStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(1, shardStats.getPrimaryNodeLimitsBreachedRejections());
            }
            long preForceRejections = shardIndexingPressure.stats().getPrimaryRejections();
            long preForcedShardRejections = shardIndexingPressure.shardStats()
                .getIndexingPressureShardStats(shardId)
                .getPrimaryRejections();
            // Primary can be forced
            Releasable forced = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1024 * 2, true);
            assertEquals(preForceRejections, shardIndexingPressure.stats().getPrimaryRejections());
            assertEquals(1024 * 8, shardIndexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());

            assertEquals(
                preForcedShardRejections,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getPrimaryRejections()
            );
            assertEquals(
                1024 * 8,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                preForcedShardRejections,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getPrimaryNodeLimitsBreachedRejections()
            );
            forced.close();

            // Local to coordinating node primary actions not rejected
            IndexingPressureStats preLocalNodeStats = shardIndexingPressure.stats();
            IndexingPressurePerShardStats preLocalShardStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
            Releasable local = shardIndexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(shardId, 1024 * 2);
            assertEquals(preLocalNodeStats.getPrimaryRejections(), shardIndexingPressure.stats().getPrimaryRejections());
            assertEquals(1024 * 6, shardIndexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(preLocalNodeStats.getCurrentPrimaryBytes() + 1024 * 2, shardIndexingPressure.stats().getCurrentPrimaryBytes());

            assertEquals(
                preLocalShardStats.getPrimaryRejections(),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getPrimaryRejections()
            );
            assertEquals(
                1024 * 6,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                preLocalShardStats.getCurrentPrimaryBytes() + 1024 * 2,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryBytes()
            );
            assertEquals(
                preLocalShardStats.getPrimaryNodeLimitsBreachedRejections(),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getPrimaryNodeLimitsBreachedRejections()
            );
            local.close();
        }

        assertEquals(1024 * 8, shardIndexingPressure.stats().getTotalCombinedCoordinatingAndPrimaryBytes());
        assertNull(shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId));
        assertEquals(
            1024 * 8,
            shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId).getTotalCombinedCoordinatingAndPrimaryBytes()
        );
    }

    public void testReplicaRejections() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1024 * 3, false);
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1024 * 3, false);
            Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 1024 * 3, false)
        ) {
            // Replica will not be rejected until replica bytes > 15KB
            Releasable replica2 = shardIndexingPressure.markReplicaOperationStarted(shardId, 1024 * 9, false);
            assertEquals(1024 * 12, shardIndexingPressure.stats().getCurrentReplicaBytes());
            assertEquals(1024 * 12, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaBytes());
            // Replica will be rejected once we cross 15KB Shard Limit
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> shardIndexingPressure.markReplicaOperationStarted(shardId, 1024 * 2, false)
            );
            IndexingPressureStats nodeStats = shardIndexingPressure.stats();
            assertEquals(1, nodeStats.getReplicaRejections());
            assertEquals(1024 * 12, nodeStats.getCurrentReplicaBytes());

            IndexingPressurePerShardStats shardStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
            assertEquals(1, shardStats.getReplicaRejections());
            assertEquals(1024 * 12, shardStats.getCurrentReplicaBytes());
            assertEquals(1, shardStats.getReplicaNodeLimitsBreachedRejections());

            // Replica can be forced
            Releasable forced = shardIndexingPressure.markReplicaOperationStarted(shardId, 1024 * 2, true);
            assertEquals(1, shardIndexingPressure.stats().getReplicaRejections());
            assertEquals(1024 * 14, shardIndexingPressure.stats().getCurrentReplicaBytes());

            assertEquals(1, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getReplicaRejections());
            assertEquals(1024 * 14, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaBytes());
            assertEquals(1, shardStats.getReplicaNodeLimitsBreachedRejections());
            forced.close();

            replica2.close();
        }

        assertEquals(1024 * 14, shardIndexingPressure.stats().getTotalReplicaBytes());
        assertNull(shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId));
        assertEquals(1024 * 14, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId).getTotalReplicaBytes());
    }

    public void testCoordinatingPrimaryShardLimitIncrease() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        try (
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 2, false);
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 2, false)
        ) {
            assertEquals(2, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCoordinatingBytes());
            assertEquals(
                4,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                10,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits()
            ); // Base Limit
            if (randomBoolean) {
                Releasable coordinating1 = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 6, false);
                assertEquals(8, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCoordinatingBytes());
                assertEquals(
                    10,
                    shardIndexingPressure.shardStats()
                        .getIndexingPressureShardStats(shardId)
                        .getCurrentCombinedCoordinatingAndPrimaryBytes()
                );
                assertEquals(
                    11,
                    shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits()
                ); // Increased Limit
                coordinating1.close();
            } else {
                Releasable primary1 = shardIndexingPressure.markPrimaryOperationStarted(shardId, 6, false);
                assertEquals(8, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryBytes());
                assertEquals(
                    10,
                    shardIndexingPressure.shardStats()
                        .getIndexingPressureShardStats(shardId)
                        .getCurrentCombinedCoordinatingAndPrimaryBytes()
                );
                assertEquals(
                    11,
                    shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits()
                ); // Increased Limit
                primary1.close();
            }
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
        assertNull(shardStoreStats);

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        if (randomBoolean) {
            assertEquals(0, shardStats.getCurrentCoordinatingBytes());
            assertEquals(8, shardStats.getTotalCoordinatingBytes());
        } else {
            assertEquals(0, shardStats.getCurrentPrimaryBytes());
            assertEquals(8, shardStats.getTotalPrimaryBytes());
        }
        assertEquals(0, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardStats.getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardStats.getCurrentPrimaryAndCoordinatingLimits());
    }

    public void testReplicaShardLimitIncrease() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 2, false)) {
            assertEquals(2, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaBytes());
            assertEquals(15, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaLimits()); // Base
                                                                                                                                   // Limit

            Releasable replica1 = shardIndexingPressure.markReplicaOperationStarted(shardId, 14, false);
            assertEquals(16, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaBytes());
            assertEquals(18, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaLimits()); // Increased
                                                                                                                                   // Limit
            replica1.close();
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
        assertNull(shardStoreStats);

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(16, shardStats.getTotalReplicaBytes());
        assertEquals(15, shardStats.getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryShardLimitIncreaseEvaluateSecondaryParam() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 4 * 1024, false);
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 4 * 1024, false)
        ) {
            assertEquals(4 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCoordinatingBytes());
            assertEquals(4 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryBytes());
            assertEquals(
                8 * 1024,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                (long) (8 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits()
            );
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
        assertNull(shardStoreStats);

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getCurrentCoordinatingBytes());
        assertEquals(0, shardStats.getCurrentPrimaryBytes());
        assertEquals(0, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(4 * 1024, shardStats.getTotalCoordinatingBytes());
        assertEquals(4 * 1024, shardStats.getTotalPrimaryBytes());
        assertEquals(8 * 1024, shardStats.getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardStats.getCurrentPrimaryAndCoordinatingLimits());
    }

    public void testReplicaShardLimitIncreaseEvaluateSecondaryParam() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 11 * 1024, false)) {
            assertEquals(11 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaBytes());
            assertEquals(
                (long) (11 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaLimits()
            );
        }

        IndexingPressurePerShardStats shardStoreStats = shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId);
        assertNull(shardStoreStats);

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(11 * 1024, shardStats.getTotalReplicaBytes());
        assertEquals(15, shardStats.getCurrentReplicaLimits());
    }

    public void testCoordinatingPrimaryShardRejectionViaSuccessfulRequestsParam() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .build();
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        try (
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1 * 1024, false);
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1 * 1024, false)
        ) {
            assertEquals(1 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCoordinatingBytes());
            assertEquals(1 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryBytes());
            assertEquals(
                2 * 1024,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                (long) (2 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits()
            );
        }

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getCurrentCoordinatingBytes());
        assertEquals(0, shardStats.getCurrentPrimaryBytes());
        assertEquals(0, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(1 * 1024, shardStats.getTotalCoordinatingBytes());
        assertEquals(1 * 1024, shardStats.getTotalPrimaryBytes());
        assertEquals(2 * 1024, shardStats.getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardStats.getCurrentPrimaryAndCoordinatingLimits());

        Thread.sleep(25);
        // Total Bytes are 9*1024 and node limit is 10*1024
        if (randomBoolean) {
            try (
                Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 7 * 1024, false);
                Releasable coordinating1 = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1 * 1024, false)
            ) {
                expectThrows(
                    OpenSearchRejectedExecutionException.class,
                    () -> shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1 * 1024, false)
                );
            }
        } else {
            try (
                Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 7 * 1024, false);
                Releasable primary1 = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1 * 1024, false)
            ) {
                expectThrows(
                    OpenSearchRejectedExecutionException.class,
                    () -> shardIndexingPressure.markPrimaryOperationStarted(shardId, 1 * 1024, false)
                );
            }
        }

        shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        if (randomBoolean) {
            assertEquals(1, shardStats.getCoordinatingRejections());
            assertEquals(0, shardStats.getCurrentCoordinatingBytes());
            assertEquals(1, shardStats.getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());
        } else {
            assertEquals(1, shardStats.getPrimaryRejections());
            assertEquals(0, shardStats.getCurrentPrimaryBytes());
            assertEquals(1, shardStats.getPrimaryLastSuccessfulRequestLimitsBreachedRejections());
        }
        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        if (randomBoolean) {
            assertEquals(1, nodeStats.getCoordinatingRejections());
            assertEquals(0, nodeStats.getCurrentCoordinatingBytes());
        } else {
            assertEquals(1, nodeStats.getPrimaryRejections());
            assertEquals(0, nodeStats.getCurrentPrimaryBytes());
        }
    }

    public void testReplicaShardRejectionViaSuccessfulRequestsParam() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .build();
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 1 * 1024, false)) {
            assertEquals(1 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaBytes());
            assertEquals(
                (long) (1 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaLimits()
            );
        }

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(1 * 1024, shardStats.getTotalReplicaBytes());
        assertEquals(15, shardStats.getCurrentReplicaLimits());

        Thread.sleep(25);
        // Total Bytes are 14*1024 and node limit is 15*1024
        try (
            Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 10 * 1024, false);
            Releasable replica1 = shardIndexingPressure.markReplicaOperationStarted(shardId, 2 * 1024, false)
        ) {
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> shardIndexingPressure.markReplicaOperationStarted(shardId, 2 * 1024, false)
            );
        }

        shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(1, shardStats.getReplicaRejections());
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(1, shardStats.getReplicaLastSuccessfulRequestLimitsBreachedRejections());

        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        assertEquals(1, nodeStats.getReplicaRejections());
        assertEquals(0, nodeStats.getCurrentReplicaBytes());
    }

    public void testCoordinatingPrimaryShardRejectionSkippedInShadowModeViaSuccessfulRequestsParam() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), false)
            .build();
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        try (
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1 * 1024, false);
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1 * 1024, false)
        ) {
            assertEquals(1 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCoordinatingBytes());
            assertEquals(1 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryBytes());
            assertEquals(
                2 * 1024,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                (long) (2 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits()
            );
        }

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getCurrentCoordinatingBytes());
        assertEquals(0, shardStats.getCurrentPrimaryBytes());
        assertEquals(0, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(1 * 1024, shardStats.getTotalCoordinatingBytes());
        assertEquals(1 * 1024, shardStats.getTotalPrimaryBytes());
        assertEquals(2 * 1024, shardStats.getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardStats.getCurrentPrimaryAndCoordinatingLimits());

        Thread.sleep(25);
        // Total Bytes are 9*1024 and node limit is 10*1024
        if (randomBoolean) {
            try (
                Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 7 * 1024, false);
                Releasable coordinating1 = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1 * 1024, false)
            ) {
                Releasable coordinating2 = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1 * 1024, false);
                coordinating2.close();
            }
        } else {
            try (
                Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 7 * 1024, false);
                Releasable primary1 = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1 * 1024, false)
            ) {
                Releasable primary2 = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1 * 1024, false);
                primary2.close();
            }
        }

        shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        if (randomBoolean) {
            assertEquals(0, shardStats.getCoordinatingRejections());
            assertEquals(0, shardStats.getCurrentCoordinatingBytes());
            assertEquals(1, shardStats.getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());
        } else {
            assertEquals(0, shardStats.getPrimaryRejections());
            assertEquals(0, shardStats.getCurrentPrimaryBytes());
            assertEquals(1, shardStats.getPrimaryLastSuccessfulRequestLimitsBreachedRejections());
        }
        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        if (randomBoolean) {
            assertEquals(0, nodeStats.getCoordinatingRejections());
            assertEquals(0, nodeStats.getCurrentCoordinatingBytes());
        } else {
            assertEquals(0, nodeStats.getPrimaryRejections());
            assertEquals(0, nodeStats.getCurrentPrimaryBytes());
        }
    }

    public void testReplicaShardRejectionSkippedInShadowModeViaSuccessfulRequestsParam() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), false)
            .build();
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 1 * 1024, false)) {
            assertEquals(1 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaBytes());
            assertEquals(
                (long) (1 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaLimits()
            );
        }

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(1 * 1024, shardStats.getTotalReplicaBytes());
        assertEquals(15, shardStats.getCurrentReplicaLimits());

        Thread.sleep(25);
        // Total Bytes are 14*1024 and node limit is 15*1024
        try (
            Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 10 * 1024, false);
            Releasable replica1 = shardIndexingPressure.markReplicaOperationStarted(shardId, 2 * 1024, false)
        ) {
            Releasable replica2 = shardIndexingPressure.markReplicaOperationStarted(shardId, 2 * 1024, false);
            replica2.close();
        }

        shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getReplicaRejections());
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(1, shardStats.getReplicaLastSuccessfulRequestLimitsBreachedRejections());

        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        assertEquals(0, nodeStats.getReplicaRejections());
        assertEquals(0, nodeStats.getCurrentReplicaBytes());
    }

    public void testCoordinatingPrimaryShardRejectionViaThroughputDegradationParam() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 1)
            .build();
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        try (
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1 * 1024, false);
            Releasable coordinating1 = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 3 * 1024, false);
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1 * 1024, false);
            Releasable primary1 = shardIndexingPressure.markPrimaryOperationStarted(shardId, 3 * 1024, false)
        ) {
            assertEquals(4 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCoordinatingBytes());
            assertEquals(4 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryBytes());
            assertEquals(
                8 * 1024,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                (long) (8 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits()
            );
            // Adding delay in the current in flight request to mimic throughput degradation
            Thread.sleep(100);
        }
        if (randomBoolean) {
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> shardIndexingPressure.markCoordinatingOperationStarted(shardId, 8 * 1024, false)
            );
        } else {
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> shardIndexingPressure.markPrimaryOperationStarted(shardId, 8 * 1024, false)
            );
        }

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        if (randomBoolean) {
            assertEquals(1, shardStats.getCoordinatingRejections());
            assertEquals(1, shardStats.getCoordinatingThroughputDegradationLimitsBreachedRejections());
            assertEquals(0, shardStats.getCurrentCoordinatingBytes());
            assertEquals(4 * 1024, shardStats.getTotalCoordinatingBytes());
        } else {
            assertEquals(1, shardStats.getPrimaryRejections());
            assertEquals(1, shardStats.getPrimaryThroughputDegradationLimitsBreachedRejections());
            assertEquals(0, shardStats.getCurrentPrimaryBytes());
            assertEquals(4 * 1024, shardStats.getTotalPrimaryBytes());
        }

        assertEquals(10, shardStats.getCurrentPrimaryAndCoordinatingLimits());
        assertEquals(0, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(8 * 1024, shardStats.getTotalCombinedCoordinatingAndPrimaryBytes());

        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        if (randomBoolean) {
            assertEquals(1, nodeStats.getCoordinatingRejections());
            assertEquals(0, nodeStats.getCurrentCoordinatingBytes());
        } else {
            assertEquals(1, nodeStats.getPrimaryRejections());
            assertEquals(0, nodeStats.getCurrentPrimaryBytes());
        }
    }

    public void testReplicaShardRejectionViaThroughputDegradationParam() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 1)
            .build();
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (
            Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 1 * 1024, false);
            Releasable replica1 = shardIndexingPressure.markReplicaOperationStarted(shardId, 3 * 1024, false)
        ) {
            assertEquals(4 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaBytes());
            assertEquals(
                (long) (4 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaLimits()
            );
            // Adding delay in the current in flight request to mimic throughput degradation
            Thread.sleep(100);
        }

        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> shardIndexingPressure.markReplicaOperationStarted(shardId, 12 * 1024, false)
        );

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(1, shardStats.getReplicaRejections());
        assertEquals(1, shardStats.getReplicaThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(4 * 1024, shardStats.getTotalReplicaBytes());
        assertEquals(15, shardStats.getCurrentReplicaLimits());

        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        assertEquals(1, nodeStats.getReplicaRejections());
        assertEquals(0, nodeStats.getCurrentReplicaBytes());
    }

    public void testCoordinatingPrimaryShardRejectionSkippedInShadowModeViaThroughputDegradationParam() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), false)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 1)
            .build();
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        boolean randomBoolean = randomBoolean();
        try (
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1 * 1024, false);
            Releasable coordinating1 = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 3 * 1024, false);
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1 * 1024, false);
            Releasable primary1 = shardIndexingPressure.markPrimaryOperationStarted(shardId, 3 * 1024, false)
        ) {
            assertEquals(4 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCoordinatingBytes());
            assertEquals(4 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryBytes());
            assertEquals(
                8 * 1024,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                (long) (8 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits()
            );
            // Adding delay in the current in flight request to mimic throughput degradation
            Thread.sleep(100);
        }
        if (randomBoolean) {
            Releasable coordinating = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 8 * 1024, false);
            coordinating.close();
        } else {
            Releasable primary = shardIndexingPressure.markPrimaryOperationStarted(shardId, 8 * 1024, false);
            primary.close();
        }

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        if (randomBoolean) {
            assertEquals(0, shardStats.getCoordinatingRejections());
            assertEquals(1, shardStats.getCoordinatingThroughputDegradationLimitsBreachedRejections());
            assertEquals(0, shardStats.getCurrentCoordinatingBytes());
            assertEquals(12 * 1024, shardStats.getTotalCoordinatingBytes());
        } else {
            assertEquals(0, shardStats.getPrimaryRejections());
            assertEquals(1, shardStats.getPrimaryThroughputDegradationLimitsBreachedRejections());
            assertEquals(0, shardStats.getCurrentPrimaryBytes());
            assertEquals(12 * 1024, shardStats.getTotalPrimaryBytes());
        }

        assertEquals(10, shardStats.getCurrentPrimaryAndCoordinatingLimits());
        assertEquals(0, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(16 * 1024, shardStats.getTotalCombinedCoordinatingAndPrimaryBytes());

        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        if (randomBoolean) {
            assertEquals(0, nodeStats.getCoordinatingRejections());
            assertEquals(0, nodeStats.getCurrentCoordinatingBytes());
        } else {
            assertEquals(0, nodeStats.getPrimaryRejections());
            assertEquals(0, nodeStats.getCurrentPrimaryBytes());
        }
    }

    public void testReplicaShardRejectionSkippedInShadowModeViaThroughputDegradationParam() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), false)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 1)
            .build();
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        try (
            Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 1 * 1024, false);
            Releasable replica1 = shardIndexingPressure.markReplicaOperationStarted(shardId, 3 * 1024, false)
        ) {
            assertEquals(4 * 1024, shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaBytes());
            assertEquals(
                (long) (4 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentReplicaLimits()
            );
            // Adding delay in the current in flight request to mimic throughput degradation
            Thread.sleep(100);
        }

        Releasable replica = shardIndexingPressure.markReplicaOperationStarted(shardId, 12 * 1024, false);
        replica.close();

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId);
        assertEquals(0, shardStats.getReplicaRejections());
        assertEquals(1, shardStats.getReplicaThroughputDegradationLimitsBreachedRejections());
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(16 * 1024, shardStats.getTotalReplicaBytes());
        assertEquals(15, shardStats.getCurrentReplicaLimits());

        IndexingPressureStats nodeStats = shardIndexingPressure.stats();
        assertEquals(0, nodeStats.getReplicaRejections());
        assertEquals(0, nodeStats.getCurrentReplicaBytes());
    }

    public void testShardLimitIncreaseMultipleShards() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId1 = new ShardId(index, 0);
        ShardId shardId2 = new ShardId(index, 1);
        try (
            Releasable coordinating1 = shardIndexingPressure.markCoordinatingOperationStarted(shardId1, 4 * 1024, false);
            Releasable coordinating2 = shardIndexingPressure.markCoordinatingOperationStarted(shardId2, 4 * 1024, false);
        ) {
            assertEquals(
                4 * 1024,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1).getCurrentCoordinatingBytes()
            );
            assertEquals(
                4 * 1024,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1).getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                (long) (4 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId1).getCurrentPrimaryAndCoordinatingLimits()
            );
            assertEquals(
                4 * 1024,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId2).getCurrentCoordinatingBytes()
            );
            assertEquals(
                4 * 1024,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId2).getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                (long) (4 * 1024 / 0.85),
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId2).getCurrentPrimaryAndCoordinatingLimits()
            );
        }

        IndexingPressurePerShardStats shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId1);
        assertEquals(0, shardStats.getCurrentCoordinatingBytes());
        assertEquals(0, shardStats.getCurrentPrimaryBytes());
        assertEquals(0, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(4 * 1024, shardStats.getTotalCoordinatingBytes());
        assertEquals(4 * 1024, shardStats.getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardStats.getCurrentPrimaryAndCoordinatingLimits());

        shardStats = shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId2);
        assertEquals(0, shardStats.getCurrentCoordinatingBytes());
        assertEquals(0, shardStats.getCurrentPrimaryBytes());
        assertEquals(0, shardStats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, shardStats.getCurrentReplicaBytes());
        assertEquals(4 * 1024, shardStats.getTotalCoordinatingBytes());
        assertEquals(4 * 1024, shardStats.getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, shardStats.getCurrentPrimaryAndCoordinatingLimits());
    }

    public void testForceExecutionOnCoordinating() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1024 * 11, false)
        );
        try (Releasable ignore = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 11 * 1024, true)) {
            assertEquals(
                11 * 1024,
                shardIndexingPressure.shardStats().getIndexingPressureShardStats(shardId).getCurrentCoordinatingBytes()
            );
        }
        assertEquals(0, shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId).getCurrentCoordinatingBytes());
    }

    public void testAssertionOnReleaseExecutedTwice() {
        ShardIndexingPressure shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        String assertionMessage = "ShardIndexingPressure Release is called twice";

        Releasable releasable = shardIndexingPressure.markCoordinatingOperationStarted(shardId, 1024, false);
        releasable.close();
        expectThrows(AssertionError.class, assertionMessage, releasable::close);

        releasable = shardIndexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(shardId, 1024);
        releasable.close();
        expectThrows(AssertionError.class, assertionMessage, releasable::close);

        releasable = shardIndexingPressure.markPrimaryOperationStarted(shardId, 1024, false);
        releasable.close();
        expectThrows(AssertionError.class, assertionMessage, releasable::close);

        releasable = shardIndexingPressure.markReplicaOperationStarted(shardId, 1024, false);
        releasable.close();
        expectThrows(AssertionError.class, assertionMessage, releasable::close);
    }
}

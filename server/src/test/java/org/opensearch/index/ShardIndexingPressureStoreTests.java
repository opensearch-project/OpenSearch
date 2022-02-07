/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.junit.Before;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class ShardIndexingPressureStoreTests extends OpenSearchTestCase {

    private final Settings settings = Settings.builder().put(ShardIndexingPressureStore.MAX_COLD_STORE_SIZE.getKey(), 200).build();
    private final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final ShardIndexingPressureSettings shardIndexingPressureSettings = new ShardIndexingPressureSettings(
        new ClusterService(settings, clusterSettings, null),
        settings,
        IndexingPressure.MAX_INDEXING_BYTES.get(settings).getBytes()
    );
    private ShardIndexingPressureStore store;
    private ShardId testShardId;

    @Before
    public void beforeTest() {
        store = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
        testShardId = new ShardId("index", "uuid", 0);
    }

    public void testShardIndexingPressureStoreGet() {
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(testShardId);
        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(testShardId);
        assertEquals(tracker1, tracker2);
    }

    public void testGetVerifyTrackerInHotStore() {
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(testShardId);

        Map<ShardId, ShardIndexingPressureTracker> hostStoreTrackers = store.getShardIndexingPressureHotStore();
        assertEquals(1, hostStoreTrackers.size());
        ShardIndexingPressureTracker hotStoreTracker = hostStoreTrackers.get(testShardId);
        assertEquals(tracker, hotStoreTracker);
    }

    public void testTrackerCleanupFromHotStore() {
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(testShardId);
        Map<ShardId, ShardIndexingPressureTracker> hostStoreTrackers = store.getShardIndexingPressureHotStore();
        assertEquals(1, hostStoreTrackers.size());

        store.tryTrackerCleanupFromHotStore(tracker, () -> true);

        hostStoreTrackers = store.getShardIndexingPressureHotStore();
        assertEquals(0, hostStoreTrackers.size());

        Map<ShardId, ShardIndexingPressureTracker> coldStoreTrackers = store.getShardIndexingPressureColdStore();
        assertEquals(1, coldStoreTrackers.size());
        ShardIndexingPressureTracker coldStoreTracker = coldStoreTrackers.get(testShardId);
        assertEquals(tracker, coldStoreTracker);
    }

    public void testTrackerCleanupSkippedFromHotStore() {
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(testShardId);
        Map<ShardId, ShardIndexingPressureTracker> hostStoreTrackers = store.getShardIndexingPressureHotStore();
        assertEquals(1, hostStoreTrackers.size());

        store.tryTrackerCleanupFromHotStore(tracker, () -> false);

        hostStoreTrackers = store.getShardIndexingPressureHotStore();
        assertEquals(1, hostStoreTrackers.size());
        ShardIndexingPressureTracker coldStoreTracker = hostStoreTrackers.get(testShardId);
        assertEquals(tracker, coldStoreTracker);
    }

    public void testTrackerRestoredToHotStorePostCleanup() {
        ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(testShardId);
        Map<ShardId, ShardIndexingPressureTracker> hostStoreTrackers = store.getShardIndexingPressureHotStore();
        assertEquals(1, hostStoreTrackers.size());

        store.tryTrackerCleanupFromHotStore(tracker1, () -> true);

        hostStoreTrackers = store.getShardIndexingPressureHotStore();
        assertEquals(0, hostStoreTrackers.size());

        ShardIndexingPressureTracker tracker2 = store.getShardIndexingPressureTracker(testShardId);
        hostStoreTrackers = store.getShardIndexingPressureHotStore();
        assertEquals(1, hostStoreTrackers.size());
        assertEquals(tracker1, tracker2);
    }

    public void testTrackerEvictedFromColdStore() {
        for (int i = 0; i <= ShardIndexingPressureStore.MAX_COLD_STORE_SIZE.get(settings); i++) {
            ShardId shardId = new ShardId("index", "uuid", i);
            ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId);
            store.tryTrackerCleanupFromHotStore(tracker, () -> true);
            assertEquals(i + 1, store.getShardIndexingPressureColdStore().size());
        }

        // Verify cold store size is maximum
        assertEquals(ShardIndexingPressureStore.MAX_COLD_STORE_SIZE.get(settings) + 1, store.getShardIndexingPressureColdStore().size());

        // get and remove one more tracker object from hot store
        ShardId shardId = new ShardId("index", "uuid", ShardIndexingPressureStore.MAX_COLD_STORE_SIZE.get(settings) + 1);
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId);
        store.tryTrackerCleanupFromHotStore(tracker, () -> true);

        // Verify all trackers objects purged from cold store except the last
        assertEquals(1, store.getShardIndexingPressureColdStore().size());
        assertEquals(tracker, store.getShardIndexingPressureColdStore().get(shardId));
    }

    public void testShardIndexingPressureStoreConcurrentGet() throws Exception {
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(testShardId);
        final int NUM_THREADS = scaledRandomIntBetween(100, 500);
        final Thread[] threads = new Thread[NUM_THREADS];
        final ShardIndexingPressureTracker[] trackers = new ShardIndexingPressureTracker[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> { trackers[counter] = store.getShardIndexingPressureTracker(testShardId); });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            assertEquals(tracker, trackers[i]);
        }
        assertEquals(1, store.getShardIndexingPressureHotStore().size());
        assertEquals(1, store.getShardIndexingPressureColdStore().size());
        assertEquals(tracker, store.getShardIndexingPressureHotStore().get(testShardId));
        assertEquals(tracker, store.getShardIndexingPressureColdStore().get(testShardId));
    }

    public void testShardIndexingPressureStoreConcurrentGetAndCleanup() throws Exception {
        ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(testShardId);
        final int NUM_THREADS = scaledRandomIntBetween(100, 500);
        final Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(() -> {
                ShardIndexingPressureTracker tracker1 = store.getShardIndexingPressureTracker(testShardId);
                assertEquals(tracker, tracker1);
                store.tryTrackerCleanupFromHotStore(tracker, () -> true);
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        assertEquals(0, store.getShardIndexingPressureHotStore().size());
        assertEquals(1, store.getShardIndexingPressureColdStore().size());
        assertEquals(tracker, store.getShardIndexingPressureColdStore().get(testShardId));
    }

    public void testTrackerConcurrentEvictionFromColdStore() throws Exception {
        int maxColdStoreSize = ShardIndexingPressureStore.MAX_COLD_STORE_SIZE.get(settings);
        final int NUM_THREADS = scaledRandomIntBetween(maxColdStoreSize * 2, maxColdStoreSize * 8);
        final Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            int counter = i;
            threads[i] = new Thread(() -> {
                ShardId shardId = new ShardId("index", "uuid", counter);
                ShardIndexingPressureTracker tracker = store.getShardIndexingPressureTracker(shardId);
                store.tryTrackerCleanupFromHotStore(tracker, () -> true);
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        assertEquals(0, store.getShardIndexingPressureHotStore().size());
        assertTrue(store.getShardIndexingPressureColdStore().size() <= maxColdStoreSize + 1);
    }
}

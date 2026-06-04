/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.SetOnce;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.refresh.RefreshStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class IndexingMemoryControllerTests extends IndexShardTestCase {

    static class MockController extends IndexingMemoryController {

        // Size of each shard's indexing buffer
        final Map<IndexShard, Long> indexBufferRAMBytesUsed = new HashMap<>();

        // How many bytes this shard is currently moving to disk
        final Map<IndexShard, Long> writingBytes = new HashMap<>();

        // Native memory used by each shard
        final Map<IndexShard, Long> nativeMemoryUsed = new HashMap<>();

        // Shards that are currently throttled
        final Set<IndexShard> throttled = new HashSet<>();

        MockController(Settings settings) {
            super(
                Settings.builder()
                    .put("indices.memory.interval", "200h") // disable it
                    .put(settings)
                    .build(),
                null,
                null
            );
        }

        public void deleteShard(IndexShard shard) {
            indexBufferRAMBytesUsed.remove(shard);
            writingBytes.remove(shard);
        }

        @Override
        protected List<IndexShard> availableShards() {
            return new ArrayList<>(indexBufferRAMBytesUsed.keySet());
        }

        @Override
        protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
            return indexBufferRAMBytesUsed.get(shard) + writingBytes.get(shard);
        }

        @Override
        protected long getShardWritingBytes(IndexShard shard) {
            Long bytes = writingBytes.get(shard);
            if (bytes == null) {
                return 0;
            } else {
                return bytes;
            }
        }

        @Override
        protected long getNativeBytesUsed(IndexShard shard) {
            return nativeMemoryUsed.getOrDefault(shard, 0L);
        }

        @Override
        protected void checkIdle(IndexShard shard, long inactiveTimeNS) {}

        @Override
        public void writeIndexingBufferAsync(IndexShard shard) {
            long bytes = indexBufferRAMBytesUsed.put(shard, 0L);
            writingBytes.put(shard, writingBytes.get(shard) + bytes);
            indexBufferRAMBytesUsed.put(shard, 0L);
            nativeMemoryUsed.put(shard, 0L);
        }

        @Override
        public void activateThrottling(IndexShard shard) {
            assertTrue(throttled.add(shard));
        }

        @Override
        public void deactivateThrottling(IndexShard shard) {
            assertTrue(throttled.remove(shard));
        }

        public void doneWriting(IndexShard shard) {
            writingBytes.put(shard, 0L);
        }

        public void assertBuffer(IndexShard shard, int expectedMB) {
            Long actual = indexBufferRAMBytesUsed.get(shard);
            if (actual == null) {
                actual = 0L;
            }
            assertEquals(expectedMB * 1024 * 1024, actual.longValue());
        }

        public void assertThrottled(IndexShard shard) {
            assertTrue(throttled.contains(shard));
        }

        public void assertNotThrottled(IndexShard shard) {
            assertFalse(throttled.contains(shard));
        }

        public void assertWriting(IndexShard shard, int expectedMB) {
            Long actual = writingBytes.get(shard);
            if (actual == null) {
                actual = 0L;
            }
            assertEquals(expectedMB * 1024 * 1024, actual.longValue());
        }

        public void simulateIndexing(IndexShard shard) {
            Long bytes = indexBufferRAMBytesUsed.get(shard);
            if (bytes == null) {
                bytes = 0L;
                // First time we are seeing this shard:
                writingBytes.put(shard, 0L);
            }
            // Each doc we index takes up a megabyte!
            bytes += 1024 * 1024;
            indexBufferRAMBytesUsed.put(shard, bytes);
            forceCheck();
        }

        @Override
        protected Cancellable scheduleTask(ThreadPool threadPool) {
            return null;
        }
    }

    public void testShardAdditionAndRemoval() throws IOException {

        MockController controller = new MockController(Settings.builder().put("indices.memory.index_buffer_size", "4mb").build());
        IndexShard shard0 = newStartedShard();
        controller.simulateIndexing(shard0);
        controller.assertBuffer(shard0, 1);

        // add another shard
        IndexShard shard1 = newStartedShard();
        controller.simulateIndexing(shard1);
        controller.assertBuffer(shard0, 1);
        controller.assertBuffer(shard1, 1);

        // remove first shard
        controller.deleteShard(shard0);
        controller.forceCheck();
        controller.assertBuffer(shard1, 1);

        // remove second shard
        controller.deleteShard(shard1);
        controller.forceCheck();

        // add a new one
        IndexShard shard2 = newStartedShard();
        controller.simulateIndexing(shard2);
        controller.assertBuffer(shard2, 1);
        closeShards(shard0, shard1, shard2);
    }

    public void testActiveInactive() throws IOException {

        MockController controller = new MockController(Settings.builder().put("indices.memory.index_buffer_size", "5mb").build());

        IndexShard shard0 = newStartedShard();
        controller.simulateIndexing(shard0);
        IndexShard shard1 = newStartedShard();
        controller.simulateIndexing(shard1);

        controller.assertBuffer(shard0, 1);
        controller.assertBuffer(shard1, 1);

        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard1);

        controller.assertBuffer(shard0, 2);
        controller.assertBuffer(shard1, 2);

        // index into one shard only, crosses the 5mb limit, so shard1 is refreshed
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.assertBuffer(shard0, 0);
        controller.assertBuffer(shard1, 2);

        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);
        controller.assertBuffer(shard1, 4);
        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);
        // shard1 crossed 5 mb and is now cleared:
        controller.assertBuffer(shard1, 0);
        closeShards(shard0, shard1);
    }

    public void testMinBufferSizes() {
        MockController controller = new MockController(
            Settings.builder().put("indices.memory.index_buffer_size", "0.001%").put("indices.memory.min_index_buffer_size", "6mb").build()
        );

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
    }

    public void testNegativeMinIndexBufferSize() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new MockController(Settings.builder().put("indices.memory.min_index_buffer_size", "-6mb").build())
        );
        assertEquals("failed to parse setting [indices.memory.min_index_buffer_size] with value [-6mb] as a size in bytes", e.getMessage());

    }

    public void testNegativeInterval() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new MockController(Settings.builder().put("indices.memory.interval", "-42s").build())
        );
        assertEquals(
            "failed to parse setting [indices.memory.interval] with value "
                + "[-42s] as a time value: negative durations are not supported",
            e.getMessage()
        );

    }

    public void testNegativeShardInactiveTime() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new MockController(Settings.builder().put("indices.memory.shard_inactive_time", "-42s").build())
        );
        assertEquals(
            "failed to parse setting [indices.memory.shard_inactive_time] with value "
                + "[-42s] as a time value: negative durations are not supported",
            e.getMessage()
        );

    }

    public void testNegativeMaxIndexBufferSize() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new MockController(Settings.builder().put("indices.memory.max_index_buffer_size", "-6mb").build())
        );
        assertEquals("failed to parse setting [indices.memory.max_index_buffer_size] with value [-6mb] as a size in bytes", e.getMessage());

    }

    public void testMaxBufferSizes() {
        MockController controller = new MockController(
            Settings.builder().put("indices.memory.index_buffer_size", "90%").put("indices.memory.max_index_buffer_size", "6mb").build()
        );

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
    }

    public void testThrottling() throws Exception {

        MockController controller = new MockController(Settings.builder().put("indices.memory.index_buffer_size", "4mb").build());
        IndexShard shard0 = newStartedShard();
        IndexShard shard1 = newStartedShard();
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.assertBuffer(shard0, 3);
        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);

        // We are now using 5 MB, so we should be writing shard0 since it's using the most heap:
        controller.assertWriting(shard0, 3);
        controller.assertWriting(shard1, 0);
        controller.assertBuffer(shard0, 0);
        controller.assertBuffer(shard1, 2);

        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);

        // Now we are still writing 3 MB (shard0), and using 5 MB index buffers, so we should now 1) be writing shard1,
        // and 2) be throttling shard1:
        controller.assertWriting(shard0, 3);
        controller.assertWriting(shard1, 4);
        controller.assertBuffer(shard0, 1);
        controller.assertBuffer(shard1, 0);

        controller.assertNotThrottled(shard0);
        controller.assertThrottled(shard1);

        logger.info("--> Indexing more data");

        // More indexing to shard0
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);

        // Now we are using 5 MB again, so shard0 should also be writing and now also be throttled:
        controller.assertWriting(shard0, 8);
        controller.assertWriting(shard1, 4);
        controller.assertBuffer(shard0, 0);
        controller.assertBuffer(shard1, 0);

        controller.assertThrottled(shard0);
        controller.assertThrottled(shard1);

        // Both shards finally finish writing, and throttling should stop:
        controller.doneWriting(shard0);
        controller.doneWriting(shard1);
        controller.forceCheck();
        controller.assertNotThrottled(shard0);
        controller.assertNotThrottled(shard1);
        closeShards(shard0, shard1);
    }

    public void testTranslogRecoveryWorksWithIMC() throws IOException {
        IndexShard shard = newStartedShard(true);
        for (int i = 0; i < 100; i++) {
            indexDoc(shard, Integer.toString(i), "{\"foo\" : \"bar\"}", MediaTypeRegistry.JSON, null);
        }
        shard.close("simon says", false, false);
        AtomicReference<IndexShard> shardRef = new AtomicReference<>();
        Settings settings = Settings.builder().put("indices.memory.index_buffer_size", "50kb").build();
        Iterable<IndexShard> iterable = () -> (shardRef.get() == null)
            ? Collections.emptyIterator()
            : Collections.singleton(shardRef.get()).iterator();
        AtomicInteger flushes = new AtomicInteger();
        IndexingMemoryController imc = new IndexingMemoryController(settings, threadPool, iterable) {
            @Override
            protected void writeIndexingBufferAsync(IndexShard shard) {
                assertEquals(shard, shardRef.get());
                flushes.incrementAndGet();
                shard.writeIndexingBuffer();
            }
        };
        shard = reinitShard(shard, imc);
        shardRef.set(shard);
        assertEquals(0, imc.availableShards().size());
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        shard.markAsRecovering("store", new RecoveryState(shard.routingEntry(), localNode, null));

        assertEquals(1, imc.availableShards().size());
        assertTrue(recoverFromStore(shard));
        assertThat("we should have flushed in IMC at least once", flushes.get(), greaterThanOrEqualTo(1));
        closeShards(shard);
    }

    EngineConfig configWithRefreshListener(EngineConfig config, ReferenceManager.RefreshListener listener) {
        final List<ReferenceManager.RefreshListener> internalRefreshListener = new ArrayList<>(config.getInternalRefreshListener());
        ;
        internalRefreshListener.add(listener);
        return new EngineConfig.Builder().shardId(config.getShardId())
            .threadPool(config.getThreadPool())
            .indexSettings(config.getIndexSettings())
            .warmer(config.getWarmer())
            .store(config.getStore())
            .mergePolicy(config.getMergePolicy())
            .analyzer(config.getAnalyzer())
            .similarity(config.getSimilarity())
            .codecService(new CodecService(null, config.getIndexSettings(), logger, List.of()))
            .eventListener(config.getEventListener())
            .queryCache(config.getQueryCache())
            .queryCachingPolicy(config.getQueryCachingPolicy())
            .translogConfig(config.getTranslogConfig())
            .flushMergesAfter(config.getFlushMergesAfter())
            .externalRefreshListener(config.getExternalRefreshListener())
            .internalRefreshListener(internalRefreshListener)
            .indexSort(config.getIndexSort())
            .circuitBreakerService(config.getCircuitBreakerService())
            .globalCheckpointSupplier(config.getGlobalCheckpointSupplier())
            .retentionLeasesSupplier(config.retentionLeasesSupplier())
            .primaryTermSupplier(config.getPrimaryTermSupplier())
            .tombstoneDocSupplier(config.getTombstoneDocSupplier())
            .build();
    }

    ThreadPoolStats.Stats getRefreshThreadPoolStats() {
        final ThreadPoolStats stats = threadPool.stats();
        for (ThreadPoolStats.Stats s : stats) {
            if (s.getName().equals(ThreadPool.Names.REFRESH)) {
                return s;
            }
        }
        throw new AssertionError("refresh thread pool stats not found [" + stats + "]");
    }

    public void testSkipRefreshIfShardIsRefreshingAlready() throws Exception {
        SetOnce<CountDownLatch> refreshLatch = new SetOnce<>();
        ReferenceManager.RefreshListener refreshListener = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {
                if (refreshLatch.get() != null) {
                    try {
                        refreshLatch.get().await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            }

            @Override
            public void afterRefresh(boolean didRefresh) {

            }
        };
        IndexShard shard = newStartedShard(
            randomBoolean(),
            Settings.EMPTY,
            new EngineBackedIndexerFactory(config -> new InternalEngine(configWithRefreshListener(config, refreshListener)))
        );
        refreshLatch.set(new CountDownLatch(1)); // block refresh
        final RefreshStats refreshStats = shard.refreshStats();
        final IndexingMemoryController controller = new IndexingMemoryController(
            Settings.builder()
                .put("indices.memory.interval", "200h") // disable it
                .put("indices.memory.index_buffer_size", "1024b")
                .build(),
            threadPool,
            Collections.singleton(shard)
        ) {
            @Override
            protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
                return randomLongBetween(1025, 10 * 1024 * 1024);
            }

            @Override
            protected long getShardWritingBytes(IndexShard shard) {
                return 0L;
            }
        };
        int iterations = randomIntBetween(10, 100);
        ThreadPoolStats.Stats beforeStats = getRefreshThreadPoolStats();
        for (int i = 0; i < iterations; i++) {
            controller.forceCheck();
        }
        assertBusy(() -> {
            ThreadPoolStats.Stats stats = getRefreshThreadPoolStats();
            assertThat(stats.getCompleted(), equalTo(beforeStats.getCompleted() + iterations - 1));
        });
        refreshLatch.get().countDown(); // allow refresh
        assertBusy(() -> {
            ThreadPoolStats.Stats stats = getRefreshThreadPoolStats();
            assertThat(stats.getCompleted(), equalTo(beforeStats.getCompleted() + iterations));
        });
        assertThat(shard.refreshStats().getTotal(), equalTo(refreshStats.getTotal() + 1));
        closeShards(shard);
    }

    // --- Dual-budget (heap + native) tests ---

    public void testNativeUnderBudgetDoesNotTriggerRefresh() throws IOException {
        // Native budget set high (1gb), native usage is under budget — should NOT trigger
        MockController controller = new MockController(
            Settings.builder()
                .put("indices.memory.index_buffer_size", "100kb")
                .put("indices.memory.native_index_buffer_size", "1gb")
                .build()
        );
        IndexShard shard = newShard(true);
        controller.indexBufferRAMBytesUsed.put(shard, 50L); // under 100kb heap budget
        controller.writingBytes.put(shard, 0L);
        controller.nativeMemoryUsed.put(shard, 10_000_000L); // 10MB native — under 1gb budget

        controller.forceCheck();

        assertEquals(50L, (long) controller.indexBufferRAMBytesUsed.get(shard)); // not refreshed
        closeShards(shard);
    }

    public void testNativeBudgetTriggersRefreshWhenExceeded() throws IOException {
        // Native budget = 1MB, heap budget = 100kb
        MockController controller = new MockController(
            Settings.builder()
                .put("indices.memory.index_buffer_size", "100kb")
                .put("indices.memory.native_index_buffer_size", "1mb")
                .build()
        );
        IndexShard shard = newShard(true);
        controller.indexBufferRAMBytesUsed.put(shard, 50L); // well under heap budget
        controller.writingBytes.put(shard, 0L);
        controller.nativeMemoryUsed.put(shard, 2_000_000L); // 2MB > 1MB native budget

        controller.forceCheck();

        // Refresh triggered because native exceeded budget
        assertEquals(0L, (long) controller.indexBufferRAMBytesUsed.get(shard));
        assertEquals(0L, (long) controller.nativeMemoryUsed.get(shard));
        closeShards(shard);
    }

    public void testHeapBudgetStillTriggersRefreshIndependently() throws IOException {
        // Native budget = 10MB (high), heap budget = 100kb
        MockController controller = new MockController(
            Settings.builder()
                .put("indices.memory.index_buffer_size", "100kb")
                .put("indices.memory.native_index_buffer_size", "10mb")
                .build()
        );
        IndexShard shard = newShard(true);
        controller.indexBufferRAMBytesUsed.put(shard, 200_000L); // 200kb > 100kb heap budget
        controller.writingBytes.put(shard, 0L);
        controller.nativeMemoryUsed.put(shard, 500_000L); // 500kb < 10MB native budget

        controller.forceCheck();

        // Refresh triggered because heap exceeded budget (native is fine)
        assertEquals(0L, (long) controller.indexBufferRAMBytesUsed.get(shard));
        closeShards(shard);
    }

    public void testRefreshTargetsSortedByNativeWhenOnlyNativeExceeds() throws IOException {
        // Two shards: shard A has high native, shard B has high heap but low native
        MockController controller = new MockController(
            Settings.builder()
                .put("indices.memory.index_buffer_size", "10mb") // high heap budget
                .put("indices.memory.native_index_buffer_size", "1mb")
                .build()
        );
        IndexShard shardA = newShard(true);
        IndexShard shardB = newShard(true);

        // Shard A: low heap, high native
        controller.indexBufferRAMBytesUsed.put(shardA, 100L);
        controller.writingBytes.put(shardA, 0L);
        controller.nativeMemoryUsed.put(shardA, 900_000L); // 900KB

        // Shard B: higher heap, low native
        controller.indexBufferRAMBytesUsed.put(shardB, 500_000L);
        controller.writingBytes.put(shardB, 0L);
        controller.nativeMemoryUsed.put(shardB, 200_000L); // 200KB

        // Total native = 1.1MB > 1MB budget. Total heap = 500KB < 10MB budget.
        controller.forceCheck();

        // Shard A should be refreshed first (higher native), shard B may or may not be refreshed
        // depending on whether refreshing A alone brings native under budget
        assertEquals(0L, (long) controller.nativeMemoryUsed.get(shardA)); // A was refreshed
        closeShards(shardA, shardB);
    }

    public void testThrottleActivatedOnNativePressure() throws IOException {
        MockController controller = new MockController(
            Settings.builder().put("indices.memory.index_buffer_size", "10mb").put("indices.memory.native_index_buffer_size", "1mb").build()
        );
        IndexShard shard = newShard(true);
        controller.indexBufferRAMBytesUsed.put(shard, 100L);
        controller.writingBytes.put(shard, 0L);
        controller.nativeMemoryUsed.put(shard, 2_000_000L); // 2MB > 1.5 × 1MB = 1.5MB throttle threshold

        controller.forceCheck();

        assertTrue(controller.throttled.contains(shard));
        closeShards(shard);
    }

    public void testNoThrottleWhenNativeUnderBudget() throws IOException {
        MockController controller = new MockController(
            Settings.builder().put("indices.memory.index_buffer_size", "10mb").put("indices.memory.native_index_buffer_size", "1gb").build()
        );
        IndexShard shard = newShard(true);
        controller.indexBufferRAMBytesUsed.put(shard, 100L);
        controller.writingBytes.put(shard, 0L);
        controller.nativeMemoryUsed.put(shard, 100_000_000L); // 100MB native — under 1gb budget

        controller.forceCheck();

        assertFalse(controller.throttled.contains(shard));
        closeShards(shard);
    }

    public void testBothBudgetsExceededSortsByTotal() throws IOException {
        MockController controller = new MockController(
            Settings.builder().put("indices.memory.index_buffer_size", "1kb").put("indices.memory.native_index_buffer_size", "1kb").build()
        );
        IndexShard shardA = newShard(true);
        IndexShard shardB = newShard(true);

        // Shard A: moderate heap + high native = high total
        controller.indexBufferRAMBytesUsed.put(shardA, 5000L);
        controller.writingBytes.put(shardA, 0L);
        controller.nativeMemoryUsed.put(shardA, 8000L);

        // Shard B: high heap + low native = lower total
        controller.indexBufferRAMBytesUsed.put(shardB, 7000L);
        controller.writingBytes.put(shardB, 0L);
        controller.nativeMemoryUsed.put(shardB, 2000L);

        // Both budgets exceeded. Shard A total=13000, Shard B total=9000
        // Shard A should be refreshed first
        controller.forceCheck();

        assertEquals(0L, (long) controller.indexBufferRAMBytesUsed.get(shardA));
        assertEquals(0L, (long) controller.nativeMemoryUsed.get(shardA));
        closeShards(shardA, shardB);
    }

    public void testHeapOnlySortKeyUsedWhenOnlyHeapExceeds() throws IOException {
        // Heap budget = 1kb (exceeded), native budget = 1gb (not exceeded)
        // Shard A: low heap, high native. Shard B: high heap, low native.
        // If sort key is heap-only, shard B is refreshed first.
        MockController controller = new MockController(
            Settings.builder().put("indices.memory.index_buffer_size", "1kb").put("indices.memory.native_index_buffer_size", "1gb").build()
        );
        IndexShard shardA = newShard(true);
        IndexShard shardB = newShard(true);

        controller.indexBufferRAMBytesUsed.put(shardA, 800L);
        controller.writingBytes.put(shardA, 0L);
        controller.nativeMemoryUsed.put(shardA, 1000L);

        controller.indexBufferRAMBytesUsed.put(shardB, 1500L); // heap higher than shardA
        controller.writingBytes.put(shardB, 0L);
        controller.nativeMemoryUsed.put(shardB, 100L); // native lower than shardA

        // Total heap = 2300 > 1024. Only heap over budget.
        // Sort key should be heap-only → shard B (1500) refreshed before shard A (800)
        controller.forceCheck();

        // Shard B must have been refreshed (heap was highest)
        assertEquals(0L, (long) controller.indexBufferRAMBytesUsed.get(shardB));
        // With B refreshed: totalHeap = 2300 - 1500 = 800 < 1024, so A should NOT be refreshed
        assertEquals(800L, (long) controller.indexBufferRAMBytesUsed.get(shardA));
        closeShards(shardA, shardB);
    }

    public void testNativeOnlySortKeyUsedWhenOnlyNativeExceeds() throws IOException {
        // Heap budget = 1gb (not exceeded), native budget = 1kb (exceeded)
        // Shard A: high heap, low native. Shard B: low heap, high native.
        // If sort key is native-only, shard B is refreshed first.
        MockController controller = new MockController(
            Settings.builder().put("indices.memory.index_buffer_size", "1gb").put("indices.memory.native_index_buffer_size", "1kb").build()
        );
        IndexShard shardA = newShard(true);
        IndexShard shardB = newShard(true);

        controller.indexBufferRAMBytesUsed.put(shardA, 1500L);
        controller.writingBytes.put(shardA, 0L);
        controller.nativeMemoryUsed.put(shardA, 100L);

        controller.indexBufferRAMBytesUsed.put(shardB, 800L); // heap lower than ShardA
        controller.writingBytes.put(shardB, 0L);
        controller.nativeMemoryUsed.put(shardB, 1000L); // native higher than ShardA

        // Total native = 1100 > 1024. Only native over budget.
        // Sort key should be native-only → shard B (1000) refreshed before shard A (100)
        controller.forceCheck();

        // Shard B must have been refreshed (native was highest)
        assertEquals(0L, (long) controller.nativeMemoryUsed.get(shardB));
        // Shard A should NOT be refreshed: totalNative = 1100 - 1000 = 100 < 1024
        assertEquals(100L, (long) controller.nativeMemoryUsed.get(shardA));
        closeShards(shardA, shardB);
    }

    public void testCombinedSortKeyUsedWhenBothExceed() throws IOException {
        // Both budgets exceeded. Shard A has lower heap but higher native → higher total.
        // Shard B has higher heap but lower native → lower total.
        // If sort key is heap+native, shard A is refreshed first.
        MockController controller = new MockController(
            Settings.builder().put("indices.memory.index_buffer_size", "1kb").put("indices.memory.native_index_buffer_size", "1kb").build()
        );
        IndexShard shardA = newShard(true);
        IndexShard shardB = newShard(true);

        // Shard A: heap=600, native=600, total=1200
        controller.indexBufferRAMBytesUsed.put(shardA, 600L);
        controller.writingBytes.put(shardA, 0L);
        controller.nativeMemoryUsed.put(shardA, 600L);

        // Shard B: heap=500, native=500, total=1000
        controller.indexBufferRAMBytesUsed.put(shardB, 500L);
        controller.writingBytes.put(shardB, 0L);
        controller.nativeMemoryUsed.put(shardB, 500L);

        // Both budgets exceeded (heap total=1100>1024, native total=1100>1024)
        // Sort key = heap+native → shard A (1200) > shard B (1000), so A refreshed first, B won't be refreshed
        controller.forceCheck();

        assertEquals(0L, (long) controller.indexBufferRAMBytesUsed.get(shardA));
        assertEquals(0L, (long) controller.nativeMemoryUsed.get(shardA));
        assertEquals(500L, (long) controller.indexBufferRAMBytesUsed.get(shardB));
        assertEquals(500L, (long) controller.nativeMemoryUsed.get(shardB));
        closeShards(shardA, shardB);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Integration test for concurrent indexing across multiple indices and shards
 * with the composite engine (Parquet primary format).
 *
 * <p>Validates that the {@link org.opensearch.index.engine.DataFormatAwareEngine}
 * correctly handles concurrent document writes, refresh, flush, and merge across
 * multiple indices with multiple shards, ensuring:
 * <ul>
 *   <li>No documents are lost under concurrent writes</li>
 *   <li>Segment generations are unique within each shard</li>
 *   <li>Row counts in catalog snapshots match the number of indexed documents</li>
 *   <li>Flush produces valid commit data with catalog snapshots</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 *   --tests "*.CompositeConcurrentIndexingIT" -Dsandbox.enabled=true
 * </pre>
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class CompositeConcurrentIndexingIT extends OpenSearchIntegTestCase {

    private static final String[] INDEX_NAMES = { "concurrent-idx-1", "concurrent-idx-2", "concurrent-idx-3" };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            for (String idx : INDEX_NAMES) {
                try {
                    client().admin().indices().prepareDelete(idx).get();
                } catch (Exception e) {
                    // index may not exist
                }
            }
        } finally {
            super.tearDown();
        }
    }

    /**
     * Indexes documents concurrently across multiple indices with multiple shards,
     * then verifies row counts, segment generation uniqueness, and commit integrity.
     */
    public void testConcurrentIndexingAcrossMultipleIndicesAndShards() throws Exception {
        int numShards = randomIntBetween(2, 4);
        int docsPerThread = randomIntBetween(20, 50);
        int threadsPerIndex = randomIntBetween(2, 4);

        // Create indices with multiple shards
        for (String idx : INDEX_NAMES) {
            client().admin()
                .indices()
                .prepareCreate(idx)
                .setSettings(indexSettings(numShards))
                .setMapping("name", "type=keyword", "value", "type=integer")
                .get();
        }
        for (String idx : INDEX_NAMES) {
            ensureGreen(idx);
        }

        // Concurrent indexing across all indices
        int totalThreads = INDEX_NAMES.length * threadsPerIndex;
        CyclicBarrier barrier = new CyclicBarrier(totalThreads);
        AtomicInteger failures = new AtomicInteger(0);
        Thread[] threads = new Thread[totalThreads];

        for (int idxOrd = 0; idxOrd < INDEX_NAMES.length; idxOrd++) {
            String indexName = INDEX_NAMES[idxOrd];
            for (int t = 0; t < threadsPerIndex; t++) {
                int threadId = idxOrd * threadsPerIndex + t;
                threads[threadId] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            IndexResponse response = client().prepareIndex()
                                .setIndex(indexName)
                                .setSource("name", "thread_" + threadId + "_doc_" + d, "value", randomIntBetween(1, 10000))
                                .get();
                            assertEquals(RestStatus.CREATED, response.status());
                        }
                    } catch (Exception e) {
                        logger.error("Indexing failed in thread " + threadId, e);
                        failures.incrementAndGet();
                    }
                }, "indexer-" + threadId);
                threads[threadId].start();
            }
        }

        for (Thread t : threads) {
            t.join(60_000);
            assertFalse("Thread did not finish in time: " + t.getName(), t.isAlive());
        }
        assertEquals("No indexing threads should have failed", 0, failures.get());

        // Refresh and flush all indices
        for (String idx : INDEX_NAMES) {
            client().admin().indices().prepareRefresh(idx).get();
            client().admin().indices().prepareFlush(idx).setForce(true).setWaitIfOngoing(true).get();
        }

        // Verify each index
        int expectedDocsPerIndex = threadsPerIndex * docsPerThread;
        for (String idx : INDEX_NAMES) {
            verifyIndex(idx, numShards, expectedDocsPerIndex);
        }
    }

    /**
     * Interleaves indexing with refresh and flush operations concurrently.
     */
    public void testInterleavedIndexRefreshFlush() throws Exception {
        String indexName = INDEX_NAMES[0];
        int numShards = 2;
        int totalDocs = randomIntBetween(50, 100);

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings(numShards))
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        AtomicInteger failures = new AtomicInteger(0);
        AtomicInteger indexedCount = new AtomicInteger(0);
        CyclicBarrier barrier = new CyclicBarrier(4);

        // 2 indexing threads
        Thread indexer1 = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < totalDocs / 2; i++) {
                    client().prepareIndex().setIndex(indexName).setSource("name", "idx1_" + i, "value", i).get();
                    indexedCount.incrementAndGet();
                    if (i % 10 == 0) {
                        Thread.yield();
                    }
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        }, "indexer-1");

        Thread indexer2 = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < totalDocs / 2; i++) {
                    client().prepareIndex().setIndex(indexName).setSource("name", "idx2_" + i, "value", i + 1000).get();
                    indexedCount.incrementAndGet();
                    if (i % 10 == 0) {
                        Thread.yield();
                    }
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        }, "indexer-2");

        // 1 refresh thread
        Thread refresher = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 5; i++) {
                    Thread.sleep(randomIntBetween(50, 200));
                    client().admin().indices().prepareRefresh(indexName).get();
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        }, "refresher");

        // 1 flush thread
        Thread flusher = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 3; i++) {
                    Thread.sleep(randomIntBetween(100, 300));
                    client().admin().indices().prepareFlush(indexName).setForce(false).setWaitIfOngoing(true).get();
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        }, "flusher");

        indexer1.start();
        indexer2.start();
        refresher.start();
        flusher.start();

        indexer1.join(60_000);
        indexer2.join(60_000);
        refresher.join(60_000);
        flusher.join(60_000);

        assertEquals("No threads should have failed", 0, failures.get());

        // Final refresh + flush
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        verifyIndex(indexName, numShards, indexedCount.get());
    }

    /**
     * Stress test for the flush/refresh interleaving fixed by holding {@code refreshLock}
     * across the entire {@code DataFormatAwareEngine.flush()} body. Without that lock, a
     * refresh that fires between {@code committer.commit()} and {@code updateLastCommitInfo()}
     * advances {@code latestCatalogSnapshot}, leaving the just-committed snapshot with a stale
     * {@code lastCommitFileName} that may point to a {@code segments_<N>} the deletion sweep
     * has already removed — surfacing later as peer-recovery {@code NoSuchFileException}.
     *
     * <p>The test pushes high contention with multiple parallel indexer + flusher + refresher
     * threads. After every flush, each flusher reads back the just-committed snapshot's
     * {@code lastCommitFileName} and asserts it (a) is non-null and (b) references a Lucene
     * commit file that actually exists on disk — the direct invariant the {@code refreshLock}
     * fix protects.
     */
    public void testConcurrentFlushAndRefreshHoldsRefreshLock() throws Exception {
        String indexName = INDEX_NAMES[0];
        int numShards = 1;
        int docsPerIndexer = 50;
        int numIndexers = 4;
        int numFlushers = 3;
        int numRefreshers = 3;
        int flushesPerThread = 8;
        int refreshesPerThread = 8;

        client().admin().indices().prepareCreate(indexName).setSettings(indexSettings(numShards)).get();
        ensureGreen(indexName);

        // Resolve the primary shard once — used by flushers to read back commit-state invariant
        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        IndexShard primaryShard = null;
        for (String node : internalCluster().getDataNodeNames()) {
            try {
                IndexShard s = getIndexShard(node, shardId, indexName);
                if (s != null && s.routingEntry().primary()) {
                    primaryShard = s;
                    break;
                }
            } catch (Exception ignore) {}
        }
        assertNotNull("primary shard for [" + indexName + "][0] must be available", primaryShard);
        final IndexShard primary = primaryShard;

        AtomicInteger failures = new AtomicInteger(0);
        AtomicInteger raceDetected = new AtomicInteger(0);
        AtomicInteger indexedCount = new AtomicInteger(0);
        CyclicBarrier barrier = new CyclicBarrier(numIndexers + numFlushers + numRefreshers);

        Thread[] indexers = new Thread[numIndexers];
        for (int t = 0; t < numIndexers; t++) {
            final int threadId = t;
            indexers[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < docsPerIndexer; i++) {
                        client().prepareIndex()
                            .setIndex(indexName)
                            .setSource("name", "t" + threadId + "_d" + i, "value", threadId * 1000 + i)
                            .get();
                        indexedCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, "race-indexer-" + t);
        }

        Thread[] flushers = new Thread[numFlushers];
        for (int t = 0; t < numFlushers; t++) {
            flushers[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < flushesPerThread; i++) {
                        Thread.sleep(randomIntBetween(20, 80));
                        client().admin().indices().prepareFlush(indexName).setForce(false).setWaitIfOngoing(true).get();

                        // Direct invariant: the just-committed snapshot's lastCommitFileName
                        // must (a) be non-null and (b) reference a segments_<N> that is still
                        // on disk. A refresh racing inside flush() leaves the committed snapshot
                        // with a stale name; once the next deletion sweep runs that file is gone.
                        try (GatedCloseable<CatalogSnapshot> ref = primary.acquireSafeCatalogSnapshot()) {
                            String reported = ref.get().getLastCommitFileName();
                            if (reported == null || !reported.startsWith("segments_")) {
                                raceDetected.incrementAndGet();
                                continue;
                            }
                            primary.store().incRef();
                            try {
                                Set<String> diskFiles = new HashSet<>(Arrays.asList(primary.store().directory().listAll()));
                                if (!diskFiles.contains(reported)) {
                                    raceDetected.incrementAndGet();
                                }
                            } finally {
                                primary.store().decRef();
                            }
                        }
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, "race-flusher-" + t);
        }

        Thread[] refreshers = new Thread[numRefreshers];
        for (int t = 0; t < numRefreshers; t++) {
            refreshers[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < refreshesPerThread; i++) {
                        Thread.sleep(randomIntBetween(10, 40));
                        client().admin().indices().prepareRefresh(indexName).get();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, "race-refresher-" + t);
        }

        for (Thread t : indexers)
            t.start();
        for (Thread t : flushers)
            t.start();
        for (Thread t : refreshers)
            t.start();

        for (Thread t : indexers)
            t.join(60_000);
        for (Thread t : flushers)
            t.join(60_000);
        for (Thread t : refreshers)
            t.join(60_000);

        assertEquals("No worker thread should have failed under concurrent flush/refresh", 0, failures.get());
        assertEquals("Committed snapshot's lastCommitFileName must always match an existing segments_<N> on disk", 0, raceDetected.get());

        // Final flush so committed snapshot reflects all indexed docs
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        verifyIndex(indexName, numShards, indexedCount.get());
    }

    // ══════════════════════════════════════════════════════════════════════
    // Merge-on-refresh tests
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Concurrent indexing with merge-on-refresh enabled (Parquet + Lucene).
     * Multiple threads fill different writers, then a single refresh triggers
     * inline merge. Verifies row count and segment consolidation.
     */
    public void testMergeOnRefreshWithConcurrentWriters() throws Exception {
        String indexName = "merge-on-refresh-concurrent";
        int numThreads = 3;
        int docsPerThread = 10;
        int totalDocs = numThreads * docsPerThread;

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(mergeOnRefreshSettings())
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger failures = new AtomicInteger(0);
        Thread[] threads = new Thread[numThreads];

        for (int t = 0; t < numThreads; t++) {
            int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int d = 0; d < docsPerThread; d++) {
                        client().prepareIndex()
                            .setIndex(indexName)
                            .setSource("name", "t" + threadId + "_d" + d, "value", threadId * 100 + d)
                            .get();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, "mor-indexer-" + t);
            threads[t].start();
        }
        for (Thread t : threads) {
            t.join(30_000);
            assertFalse("Thread did not finish in time: " + t.getName(), t.isAlive());
        }
        assertEquals(0, failures.get());

        // Single refresh triggers merge-on-refresh
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        verifyIndex(indexName, 1, totalDocs);

        // Clean up
        client().admin().indices().prepareDelete(indexName).get();
    }

    /**
     * Concurrent indexing with merge-on-refresh and sorted index (Parquet + Lucene).
     * Verifies RowIdMapping correctness: after inline merge, both formats are aligned.
     */
    public void testMergeOnRefreshSortedWithConcurrentWriters() throws Exception {
        String indexName = "merge-on-refresh-sorted";
        int numThreads = 3;
        int docsPerThread = 10;
        int totalDocs = numThreads * docsPerThread;

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(mergeOnRefreshSortedSettings())
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger failures = new AtomicInteger(0);
        Thread[] threads = new Thread[numThreads];

        for (int t = 0; t < numThreads; t++) {
            int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int d = 0; d < docsPerThread; d++) {
                        client().prepareIndex()
                            .setIndex(indexName)
                            .setSource("name", "t" + threadId + "_d" + d, "value", randomIntBetween(0, 500))
                            .get();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, "mor-sorted-indexer-" + t);
            threads[t].start();
        }
        for (Thread t : threads) {
            t.join(30_000);
            assertFalse("Thread did not finish in time: " + t.getName(), t.isAlive());
        }
        assertEquals(0, failures.get());

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        verifyIndex(indexName, 1, totalDocs);

        // Clean up
        client().admin().indices().prepareDelete(indexName).get();
    }

    /**
     * Verifies that merge-on-refresh and background merge coexist without deadlock.
     * Indexes docs in batches with explicit refreshes (triggering merge-on-refresh),
     * then waits for background merge to further consolidate. Both merge paths
     * use the same IndexWriter and refreshLock — this test ensures they don't deadlock.
     */
    public void testMergeOnRefreshCoexistsWithBackgroundMerge() throws Exception {
        String indexName = "merge-coexistence";

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(mergeOnRefreshSettings())
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        // Multiple refresh cycles with concurrent writers to trigger merge-on-refresh
        int cycles = 5;
        int threadsPerCycle = 3;
        int docsPerThread = 5;
        int totalDocs = cycles * threadsPerCycle * docsPerThread;

        // Run forceMerge concurrently to exercise background merge + merge-on-refresh coexistence
        AtomicInteger mergeFailures = new AtomicInteger(0);
        AtomicInteger forceMergeCount = new AtomicInteger(0);
        Thread forceMerger = new Thread(() -> {
            while (Thread.currentThread().isInterrupted() == false) {
                try {
                    Thread.sleep(randomIntBetween(100, 300));
                    client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
                    forceMergeCount.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // Expected during concurrent indexing — shard may be relocating or refreshing
                }
            }
        }, "force-merger");
        forceMerger.setDaemon(true);
        forceMerger.start();

        for (int cycle = 0; cycle < cycles; cycle++) {
            CyclicBarrier barrier = new CyclicBarrier(threadsPerCycle);
            AtomicInteger failures = new AtomicInteger(0);
            Thread[] threads = new Thread[threadsPerCycle];

            for (int t = 0; t < threadsPerCycle; t++) {
                int id = cycle * threadsPerCycle * docsPerThread + t * docsPerThread;
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            client().prepareIndex().setIndex(indexName).setSource("name", "doc_" + (id + d), "value", id + d).get();
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads) {
                t.join(30_000);
                assertFalse("Thread hung", t.isAlive());
            }
            assertEquals(0, failures.get());

            // Each refresh may trigger merge-on-refresh (consolidating per-writer segments)
            client().admin().indices().prepareRefresh(indexName).get();
        }

        // Stop force merger
        forceMerger.interrupt();
        forceMerger.join(10_000);
        assertFalse("Force merger thread hung", forceMerger.isAlive());

        // Final flush
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        // Verify no data loss — all docs present regardless of which merge path handled them
        verifyIndex(indexName, 1, totalDocs);

        // Verify shard is healthy (not failed due to merge race)
        ensureGreen(indexName);

        // Verify background merge actually ran
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).clear().setMerge(true).get();
        long totalMerges = statsResponse.getIndex(indexName).getTotal().getMerge().getTotal();
        assertTrue(
            "Background merge should have triggered (forceMergeCount=" + forceMergeCount.get() + ", totalMerges=" + totalMerges + ")",
            totalMerges > 0
        );

        client().admin().indices().prepareDelete(indexName).get();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Merge-on-refresh disabled (baseline) tests
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Same concurrent indexing workload as testMergeOnRefreshWithConcurrentWriters but with
     * merge-on-refresh DISABLED (max_size=0). Verifies that the standard per-writer segment
     * path still works correctly — each refresh produces N segments (one per writer), and
     * background merge handles consolidation.
     */
    public void testMergeOnRefreshDisabledWithConcurrentWriters() throws Exception {
        String indexName = "merge-on-refresh-disabled";
        int numThreads = 3;
        int docsPerThread = 10;
        int totalDocs = numThreads * docsPerThread;

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(mergeOnRefreshDisabledSettings())
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger failures = new AtomicInteger(0);
        Thread[] threads = new Thread[numThreads];

        for (int t = 0; t < numThreads; t++) {
            int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int d = 0; d < docsPerThread; d++) {
                        client().prepareIndex()
                            .setIndex(indexName)
                            .setSource("name", "t" + threadId + "_d" + d, "value", threadId * 100 + d)
                            .get();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, "disabled-indexer-" + t);
            threads[t].start();
        }
        for (Thread t : threads) {
            t.join(30_000);
            assertFalse("Thread did not finish in time: " + t.getName(), t.isAlive());
        }
        assertEquals(0, failures.get());

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        // With merge-on-refresh disabled, each writer produces its own segment
        // Verify all data is present
        verifyIndex(indexName, 1, totalDocs);

        client().admin().indices().prepareDelete(indexName).get();
    }

    /**
     * Sorted index with merge-on-refresh DISABLED. Verifies that per-writer segments
     * are produced independently (no inline consolidation) and data is intact.
     * Background merge will eventually consolidate them with RowIdMapping.
     */
    public void testMergeOnRefreshDisabledWithSortedIndex() throws Exception {
        String indexName = "sorted-merge-disabled";
        int numThreads = 3;
        int docsPerThread = 10;
        int totalDocs = numThreads * docsPerThread;

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(mergeOnRefreshDisabledSortedSettings())
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger failures = new AtomicInteger(0);
        Thread[] threads = new Thread[numThreads];

        for (int t = 0; t < numThreads; t++) {
            int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int d = 0; d < docsPerThread; d++) {
                        client().prepareIndex()
                            .setIndex(indexName)
                            .setSource("name", "t" + threadId + "_d" + d, "value", randomIntBetween(0, 500))
                            .get();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, "sorted-disabled-indexer-" + t);
            threads[t].start();
        }
        for (Thread t : threads) {
            t.join(30_000);
            assertFalse("Thread did not finish in time: " + t.getName(), t.isAlive());
        }
        assertEquals(0, failures.get());

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        verifyIndex(indexName, 1, totalDocs);

        client().admin().indices().prepareDelete(indexName).get();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Merge-on-refresh with auto-refresh and force merge
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Auto-refresh enabled (1s interval) with merge-on-refresh. Concurrent indexing
     * triggers periodic auto-refreshes, each potentially consolidating per-writer segments.
     * Verifies no deadlock or data loss under continuous auto-refresh + merge-on-refresh.
     */
    public void testMergeOnRefreshWithAutoRefreshEnabled() throws Exception {
        String indexName = "merge-auto-refresh";
        int numThreads = 3;
        int docsPerThread = 20;
        int totalDocs = numThreads * docsPerThread;

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(mergeOnRefreshAutoRefreshSettings())
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger failures = new AtomicInteger(0);
        Thread[] threads = new Thread[numThreads];

        for (int t = 0; t < numThreads; t++) {
            int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int d = 0; d < docsPerThread; d++) {
                        client().prepareIndex()
                            .setIndex(indexName)
                            .setSource("name", "t" + threadId + "_d" + d, "value", threadId * 100 + d)
                            .get();
                        if (d % 5 == 0) Thread.sleep(randomIntBetween(50, 150));
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, "auto-refresh-indexer-" + t);
            threads[t].start();
        }
        for (Thread t : threads) {
            t.join(60_000);
            assertFalse("Thread did not finish in time: " + t.getName(), t.isAlive());
        }
        assertEquals(0, failures.get());

        // Final explicit refresh + flush
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        verifyIndex(indexName, 1, totalDocs);
        client().admin().indices().prepareDelete(indexName).get();
    }

    /**
     * Force merge to 1 segment on a sorted index with merge-on-refresh ENABLED.
     * Multiple refresh cycles each produce 1 consolidated segment via merge-on-refresh.
     * After 5 cycles → 5 segments. forceMerge(1) must merge them into 1.
     * Verifies data correctness and that force merge actually ran.
     */
    public void testForceMergeAfterMergeOnRefreshSorted() throws Exception {
        String indexName = "force-merge-sorted";
        // Use enough cycles so multiple segments accumulate regardless of writer distribution
        int cycles = 5;
        int threadsPerCycle = 3;
        int docsPerThread = 5;
        int totalDocs = cycles * threadsPerCycle * docsPerThread;

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(mergeOnRefreshSortedSettings())
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        for (int cycle = 0; cycle < cycles; cycle++) {
            CyclicBarrier barrier = new CyclicBarrier(threadsPerCycle);
            AtomicInteger failures = new AtomicInteger(0);
            Thread[] threads = new Thread[threadsPerCycle];

            for (int t = 0; t < threadsPerCycle; t++) {
                int id = cycle * threadsPerCycle * docsPerThread + t * docsPerThread;
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            client().prepareIndex()
                                .setIndex(indexName)
                                .setSource("name", "doc_" + (id + d), "value", randomIntBetween(0, 1000))
                                .get();
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads) {
                t.join(30_000);
                assertFalse("Thread hung", t.isAlive());
            }
            assertEquals(0, failures.get());
            client().admin().indices().prepareRefresh(indexName).get();
        }

        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        // Force merge: 5 cycles produce at least 2 segments (even if some cycles use 1 writer),
        // so forceMerge(1) must actually merge.
        client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        verifyIndex(indexName, 1, totalDocs);

        client().admin().indices().prepareDelete(indexName).get();
    }

    /**
     * Bulk indexing with refresh=wait_for alongside merge-on-refresh.
     * Each bulk request triggers a refresh (making docs immediately searchable),
     * which may trigger merge-on-refresh if multiple writers flushed.
     */
    public void testMergeOnRefreshWithBulkWaitFor() throws Exception {
        String indexName = "merge-bulk-waitfor";
        int bulkBatches = 5;
        int docsPerBatch = 10;
        int totalDocs = bulkBatches * docsPerBatch;

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(mergeOnRefreshSettings())
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        for (int batch = 0; batch < bulkBatches; batch++) {
            var bulkRequest = client().prepareBulk().setRefreshPolicy(org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int d = 0; d < docsPerBatch; d++) {
                int id = batch * docsPerBatch + d;
                bulkRequest.add(client().prepareIndex(indexName).setSource("name", "bulk_" + id, "value", id));
            }
            var response = bulkRequest.get();
            assertFalse("Bulk request should not have failures", response.hasFailures());
        }

        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        verifyIndex(indexName, 1, totalDocs);
        client().admin().indices().prepareDelete(indexName).get();
    }

    /**
     * Concurrent force merge + indexing + explicit refresh on unsorted index.
     * Exercises the lock interaction between merge-on-refresh (inline, holds refreshLock)
     * and force merge (background, warmer needs refreshLock).
     */
    public void testMergeOnRefreshWithConcurrentForceMergeUnsorted() throws Exception {
        String indexName = "merge-concurrent-force-unsorted";
        int totalDocs = 60;

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(mergeOnRefreshSettings())
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        AtomicInteger indexed = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        // Indexer thread: indexes docs and triggers explicit refreshes periodically
        Thread indexer = new Thread(() -> {
            for (int i = 0; i < totalDocs; i++) {
                try {
                    client().prepareIndex(indexName).setSource("name", "doc_" + i, "value", i).get();
                    indexed.incrementAndGet();
                    if (i > 0 && i % 10 == 0) {
                        client().admin().indices().prepareRefresh(indexName).get();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }
        }, "indexer");

        // Force merge thread: runs concurrently
        AtomicInteger forceMergeRuns = new AtomicInteger(0);
        Thread merger = new Thread(() -> {
            while (indexed.get() < totalDocs) {
                try {
                    Thread.sleep(randomIntBetween(100, 200));
                    client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
                    forceMergeRuns.incrementAndGet();
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    // Expected during concurrent operations
                }
            }
        }, "force-merger");

        indexer.start();
        merger.start();

        indexer.join(60_000);
        assertFalse("Indexer hung", indexer.isAlive());
        merger.interrupt();
        merger.join(10_000);
        assertFalse("Merger hung", merger.isAlive());
        assertEquals(0, failures.get());

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        verifyIndex(indexName, 1, totalDocs);
        client().admin().indices().prepareDelete(indexName).get();
    }

    /**
     * Verifies that merge-on-refresh does not leave orphan per-writer Parquet files on disk.
     * After inline merge, only the merged file should remain — the source per-writer files
     * must be deleted. Without proper cleanup, each merge-on-refresh would leak N-1 files.
     */
    public void testMergeOnRefreshDeletesSourceFiles() throws Exception {
        String indexName = "merge-no-orphans";
        int numThreads = 3;
        int docsPerThread = 10;
        int totalDocs = numThreads * docsPerThread;

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(mergeOnRefreshSettings())
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger failures = new AtomicInteger(0);
        Thread[] threads = new Thread[numThreads];

        for (int t = 0; t < numThreads; t++) {
            int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int d = 0; d < docsPerThread; d++) {
                        client().prepareIndex()
                            .setIndex(indexName)
                            .setSource("name", "t" + threadId + "_d" + d, "value", threadId * 100 + d)
                            .get();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, "orphan-test-indexer-" + t);
            threads[t].start();
        }
        for (Thread t : threads) {
            t.join(30_000);
            assertFalse("Thread did not finish in time: " + t.getName(), t.isAlive());
        }
        assertEquals(0, failures.get());

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        // Verify data correctness
        verifyIndex(indexName, 1, totalDocs);

        // Get the primary shard's parquet directory directly via IndexShard
        String nodeName = getClusterState().routingTable().index(indexName).shard(0).primaryShard().currentNodeId();
        String nodeNameResolved = getClusterState().nodes().get(nodeName).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeNameResolved);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        IndexShard shard = indexService.getShard(0);
        java.nio.file.Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");

        assertTrue("Parquet directory must exist: " + parquetDir, java.nio.file.Files.isDirectory(parquetDir));

        // Count actual parquet files on disk
        Set<String> diskFiles;
        try (var stream = java.nio.file.Files.list(parquetDir)) {
            diskFiles = stream.filter(p -> p.toString().endsWith(".parquet"))
                .map(p -> p.getFileName().toString())
                .collect(java.util.stream.Collectors.toSet());
        }
        assertFalse("Expected parquet files on disk", diskFiles.isEmpty());

        // Collect all parquet files referenced by the catalog
        try (GatedCloseable<CatalogSnapshot> gated = shard.getCatalogSnapshot()) {
            DataformatAwareCatalogSnapshot snapshot = (DataformatAwareCatalogSnapshot) gated.get();
            Set<String> catalogFiles = new HashSet<>();
            for (Segment seg : snapshot.getSegments()) {
                WriterFileSet wfs = seg.dfGroupedSearchableFiles().get("parquet");
                if (wfs != null) {
                    catalogFiles.addAll(wfs.files());
                }
            }
            assertFalse("Catalog must reference at least one parquet file", catalogFiles.isEmpty());

            // Every file on disk must be referenced by the catalog (no orphans)
            Set<String> orphans = new HashSet<>(diskFiles);
            orphans.removeAll(catalogFiles);
            assertTrue("Orphan parquet files on disk not referenced by catalog: " + orphans, orphans.isEmpty());
        }

        client().admin().indices().prepareDelete(indexName).get();
    }

    // ── Helpers ──

    private Settings indexSettings(int numShards) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
    }

    private Settings mergeOnRefreshSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put("index.composite.merge_on_refresh_max_size", "10mb")
            .put("index.merge.policy.max_merge_at_once", 3)
            .build();
    }

    private Settings mergeOnRefreshSortedSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put("index.composite.merge_on_refresh_max_size", "10mb")
            .put("index.merge.policy.max_merge_at_once", 3)
            .putList("index.sort.field", "value")
            .putList("index.sort.order", "desc")
            .putList("index.sort.missing", "_last")
            .build();
    }

    private Settings mergeOnRefreshAutoRefreshSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "1s")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put("index.composite.merge_on_refresh_max_size", "10mb")
            .put("index.merge.policy.max_merge_at_once", 3)
            .build();
    }

    private Settings mergeOnRefreshDisabledSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put("index.composite.merge_on_refresh_max_size", "0")
            .build();
    }

    private Settings mergeOnRefreshDisabledSortedSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put("index.composite.merge_on_refresh_max_size", "0")
            .putList("index.sort.field", "value")
            .putList("index.sort.order", "desc")
            .putList("index.sort.missing", "_last")
            .build();
    }

    private void verifyIndex(String indexName, int numShards, int expectedTotalDocs) throws IOException {
        long totalRows = 0;
        Set<Long> allGenerations = new HashSet<>();

        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).clear().setStore(true).get();
        ShardStats[] shardStats = statsResponse.getIndex(indexName).getShards();

        for (ShardStats ss : shardStats) {
            if (ss.getShardRouting().primary() == false) {
                continue;
            }
            CommitStats commitStats = ss.getCommitStats();
            assertNotNull("Commit stats should exist for shard " + ss.getShardRouting().shardId(), commitStats);

            String snapshotKey = DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY;
            assertTrue(
                "Commit data should contain catalog snapshot for shard " + ss.getShardRouting().shardId(),
                commitStats.getUserData().containsKey(snapshotKey)
            );

            DataformatAwareCatalogSnapshot snapshot = DataformatAwareCatalogSnapshot.deserializeFromString(
                commitStats.getUserData().get(snapshotKey),
                Function.identity()
            );

            // Verify segment generation uniqueness within this shard
            Set<Long> shardGenerations = new HashSet<>();
            for (Segment seg : snapshot.getSegments()) {
                assertTrue(
                    "Duplicate generation " + seg.generation() + " in shard " + ss.getShardRouting().shardId(),
                    shardGenerations.add(seg.generation())
                );
                // Every segment must have files
                assertFalse("Segment " + seg.generation() + " has no files", seg.dfGroupedSearchableFiles().isEmpty());
                for (WriterFileSet wfs : seg.dfGroupedSearchableFiles().values()) {
                    assertTrue("WriterFileSet must have positive row count", wfs.numRows() > 0);
                    assertFalse("WriterFileSet must have files", wfs.files().isEmpty());
                }
                // Count rows from the primary format only (avoid double-counting with secondaries)
                WriterFileSet primaryWfs = seg.dfGroupedSearchableFiles().get("parquet");
                if (primaryWfs != null) {
                    totalRows += primaryWfs.numRows();
                }
            }
            allGenerations.addAll(shardGenerations);
        }

        assertEquals("Total rows across all shards of " + indexName + " must match indexed docs", expectedTotalDocs, totalRows);
    }
}

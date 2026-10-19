/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.filteredstats;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The node-level bound on cached visible-subset statistics: eviction under the byte limit, refusal when the breaker has
 * no room, accounting, invalidation and sharing between indices.
 */
public class IndicesFilteredStatsCacheTests extends OpenSearchTestCase {

    private static final Query FILTER = new TermQuery(new Term("dept", "visible"));

    private Directory dir;
    private DirectoryReader reader;
    private LeafReaderContext leaf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = oneSegment();
        reader = DirectoryReader.open(dir);
        leaf = reader.leaves().get(0);
    }

    @Override
    public void tearDown() throws Exception {
        IOUtils.close(reader, dir);
        super.tearDown();
    }

    private static CircuitBreaker fieldDataBreaker(Long limitBytes) {
        final Settings.Builder settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false);
        if (limitBytes != null) {
            settings.put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limitBytes, ByteSizeUnit.BYTES);
        }
        return new HierarchyCircuitBreakerService(
            settings.build(),
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.FIELDDATA);
    }

    private static Settings sized(String size) {
        return Settings.builder().put(IndicesFilteredStatsCache.INDICES_FILTERED_STATS_CACHE_SIZE_SETTING.getKey(), size).build();
    }

    private static IndicesFilteredStatsCache unbreakable(Settings settings) {
        return new IndicesFilteredStatsCache(settings, new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
    }

    private static FilteredStatsCache index(String name, IndicesFilteredStatsCache nodeCache) {
        return new FilteredStatsCache(IndexSettingsModule.newIndexSettings(name, Settings.EMPTY), nodeCache);
    }

    private static Directory oneSegment() throws IOException {
        final Directory dir = newDirectory();
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            final Document doc = new Document();
            doc.add(new StringField("dept", "visible", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();
        }
        return dir;
    }

    public void testManyDistinctTermsStayWithinTheByteBound() throws IOException {
        final long bound = 8 * 1024;
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized(bound + "b"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        for (int i = 0; i < 2_000; i++) {
            cache.getOrComputeTermContribution(leaf, FILTER, new Term("body", "term" + i), () -> new long[] { 1, 1 });
        }
        assertThat(nodeCache.weight(), lessThanOrEqualTo(bound));
        assertThat("older terms must have been evicted", nodeCache.count(), lessThan(2_000));
        assertEquals("the index's share is the whole node cache", nodeCache.weight(), cache.ramBytesUsed());
    }

    public void testManyDistinctFiltersStayWithinTheByteBound() throws IOException {
        final long bound = 64 * 1024;
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized(bound + "b"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        // One restriction per user, each with its own visible bitset, as with per-user document rules.
        for (int user = 0; user < 500; user++) {
            final Query perUser = new TermQuery(new Term("owner", "user" + user));
            cache.getOrComputeVisibleBitSet(leaf, perUser, () -> new FixedBitSet(64 * 1024));
        }
        assertThat(nodeCache.weight(), lessThanOrEqualTo(bound));
        assertThat(nodeCache.count(), lessThan(500));
    }

    public void testEvictedEntryIsRecomputed() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized("1kb"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        final AtomicInteger calls = new AtomicInteger();
        final Term first = new Term("body", "first");
        cache.getOrComputeTermContribution(leaf, FILTER, first, () -> {
            calls.incrementAndGet();
            return new long[] { 3, 4 };
        });
        for (int i = 0; i < 200; i++) {
            cache.getOrComputeTermContribution(leaf, FILTER, new Term("body", "other" + i), () -> new long[] { 1, 1 });
        }
        final long[] again = cache.getOrComputeTermContribution(leaf, FILTER, first, () -> {
            calls.incrementAndGet();
            return new long[] { 3, 4 };
        });
        assertArrayEquals(new long[] { 3, 4 }, again);
        assertEquals("the evicted entry must be computed again", 2, calls.get());
    }

    public void testBreakerWithoutRoomReturnsTheValueWithoutCachingIt() throws IOException {
        final CircuitBreaker breaker = fieldDataBreaker(100L);
        final IndicesFilteredStatsCache nodeCache = new IndicesFilteredStatsCache(Settings.EMPTY, breaker);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final AtomicInteger calls = new AtomicInteger();
        final BitSet large = new FixedBitSet(64 * 1024);
        for (int i = 0; i < 2; i++) {
            final BitSet result = cache.getOrComputeVisibleBitSet(leaf, FILTER, () -> {
                calls.incrementAndGet();
                return large;
            });
            assertSame("the computed value is still returned", large, result);
        }
        assertEquals("nothing was cached, so each call computes", 2, calls.get());
        assertEquals(0, nodeCache.count());
        assertEquals("a refused entry reserves nothing", 0L, breaker.getUsed());
        assertEquals("each refusal shows as a breaker trip", 2L, breaker.getTrippedCount());
    }

    public void testBreakerAccountingFollowsTheCache() throws IOException {
        final CircuitBreaker breaker = fieldDataBreaker(null);
        final IndicesFilteredStatsCache nodeCache = new IndicesFilteredStatsCache(sized("2kb"), breaker);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final Directory dir = oneSegment();
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final LeafReaderContext leaf = reader.leaves().get(0);
            for (int i = 0; i < 200; i++) {
                cache.getOrComputeTermContribution(leaf, FILTER, new Term("body", "term" + i), () -> new long[] { 1, 1 });
            }
            assertThat(nodeCache.weight(), greaterThan(0L));
            assertEquals("evictions return their bytes", nodeCache.weight(), breaker.getUsed());
        }
        assertEquals("closing the segment invalidates its entries", 0, nodeCache.count());
        assertEquals(0L, breaker.getUsed());
        assertEquals(0L, cache.ramBytesUsed());
        dir.close();
    }

    public void testAbsentTermsAreCharged() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        assertNull(cache.getOrComputeTermContribution(leaf, FILTER, new Term("body", "nowhere"), () -> null));
        assertEquals(1, nodeCache.count());
        assertThat("a cached absent answer still holds its key", nodeCache.weight(), greaterThan(0L));
    }

    public void testConcurrentMissesComputeOnce() throws Exception {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final AtomicInteger calls = new AtomicInteger();
        final CountDownLatch start = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                    cache.getOrComputeTermContribution(leaf, FILTER, new Term("body", "hot"), () -> {
                        calls.incrementAndGet();
                        return new long[] { 1, 1 };
                    });
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
            threads.add(thread);
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(1, calls.get());
    }

    public void testClearDropsOnlyThatIndex() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache first = index("first", nodeCache);
        final FilteredStatsCache second = index("second", nodeCache);
        try (
            Directory firstDir = oneSegment();
            Directory secondDir = oneSegment();
            DirectoryReader firstReader = DirectoryReader.open(firstDir);
            DirectoryReader secondReader = DirectoryReader.open(secondDir)
        ) {
            first.getOrComputeFieldContribution(firstReader.leaves().get(0), FILTER, "body", () -> new long[] { 1, 1, 1 });
            second.getOrComputeFieldContribution(secondReader.leaves().get(0), FILTER, "body", () -> new long[] { 1, 1, 1 });
            assertEquals(2, nodeCache.count());

            first.clear("test");
            assertEquals(0L, first.ramBytesUsed());
            assertThat(second.ramBytesUsed(), greaterThan(0L));
            assertEquals(1, nodeCache.count());
        }
    }

    public void testComputationFailureIsRethrownAndNotCached() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final IOException failure = new IOException("postings unreadable");
        final IOException thrown = expectThrows(
            IOException.class,
            () -> cache.getOrComputeVisibleBitSet(leaf, FILTER, () -> { throw failure; })
        );
        assertSame(failure, thrown);
        assertEquals(0, nodeCache.count());
    }

    /**
     * A request waiting on a load that another request's cancellation fails must still succeed. Cache treats most such
     * waiters as a miss; getOrCompute covers the rest, which reach the failed load from inside its own compute.
     */
    public void testWaiterComputesItselfWhenAnotherRequestsLoadFails() throws Exception {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final Term term = new Term("body", "shared");
        final CountDownLatch loading = new CountDownLatch(1);
        final CountDownLatch fail = new CountDownLatch(1);
        final AtomicReference<Throwable> leaderFailure = new AtomicReference<>();
        final Thread leader = new Thread(() -> {
            try {
                cache.getOrComputeTermContribution(leaf, FILTER, term, () -> {
                    loading.countDown();
                    try {
                        fail.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    // What a cancelled or timed-out request's reader throws in the middle of a postings walk.
                    throw new IllegalStateException("leader cancelled");
                });
            } catch (Throwable t) {
                leaderFailure.set(t);
            }
        });
        leader.start();
        loading.await();
        final AtomicReference<long[]> waiterResult = new AtomicReference<>();
        final AtomicReference<Throwable> waiterFailure = new AtomicReference<>();
        final Thread waiter = new Thread(() -> {
            try {
                waiterResult.set(cache.getOrComputeTermContribution(leaf, FILTER, term, () -> new long[] { 5, 6 }));
            } catch (Throwable t) {
                waiterFailure.set(t);
            }
        });
        waiter.start();
        assertBusy(() -> assertEquals(Thread.State.WAITING, waiter.getState()));
        fail.countDown();
        leader.join();
        waiter.join();
        assertEquals("leader cancelled", leaderFailure.get().getMessage());
        assertNull("the leader's failure must not reach the waiter", waiterFailure.get());
        assertArrayEquals(new long[] { 5, 6 }, waiterResult.get());
    }

    public void testRefusedEntriesAreNotTracked() throws IOException {
        final CircuitBreaker breaker = fieldDataBreaker(10L);
        final IndicesFilteredStatsCache nodeCache = new IndicesFilteredStatsCache(Settings.EMPTY, breaker);
        final FilteredStatsCache cache = index("idx", nodeCache);
        for (int i = 0; i < 100; i++) {
            cache.getOrComputeTermContribution(leaf, FILTER, new Term("body", "term" + i), () -> new long[] { 1, 1 });
        }
        assertEquals(100, nodeCache.refusals());
        assertEquals("nothing held for entries that were never cached", 0, nodeCache.trackedKeys());
    }

    public void testZeroSizeTurnsCachingOff() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized("0b"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        final AtomicInteger calls = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            cache.getOrComputeFieldContribution(leaf, FILTER, "body", () -> {
                calls.incrementAndGet();
                return new long[] { 1, 1, 1 };
            });
        }
        assertEquals(3, calls.get());
        assertEquals(0, nodeCache.count());
    }

    public void testEntryLargerThanHalfTheCacheIsNotCachedAndEvictsNothing() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized("16kb"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        for (int i = 0; i < 10; i++) {
            cache.getOrComputeTermContribution(leaf, FILTER, new Term("body", "small" + i), () -> new long[] { 1, 1 });
        }
        final int before = nodeCache.count();
        final BitSet large = new FixedBitSet(80 * 1024);
        assertSame(large, cache.getOrComputeVisibleBitSet(leaf, FILTER, () -> large));
        assertEquals("the small entries survive", before, nodeCache.count());
        assertEquals(1, nodeCache.refusals());
        assertEquals(0L, nodeCache.evictions());
    }

    public void testErrorInALoadDoesNotLeaveTheKeyBlocked() throws Exception {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final Directory dir = oneSegment();
        final DirectoryReader reader = DirectoryReader.open(dir);
        final LeafReaderContext leaf = reader.leaves().get(0);
        final Term term = new Term("body", "fragile");
        final AssertionError error = new AssertionError("allocation failed");
        assertSame(
            error,
            expectThrows(AssertionError.class, () -> cache.getOrComputeTermContribution(leaf, FILTER, term, () -> { throw error; }))
        );
        final AtomicReference<long[]> retried = new AtomicReference<>();
        final Thread retry = new Thread(() -> {
            try {
                retried.set(cache.getOrComputeTermContribution(leaf, FILTER, term, () -> new long[] { 2, 2 }));
                reader.close();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        retry.start();
        retry.join(10_000);
        assertFalse("a later caller and the segment close must not wait on the failed load", retry.isAlive());
        assertArrayEquals(new long[] { 2, 2 }, retried.get());
        assertEquals(0, nodeCache.count());
        dir.close();
    }
}

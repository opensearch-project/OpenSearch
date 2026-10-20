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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The node-level bound on cached visible-subset statistics: eviction under the byte limit, refusal when the breaker has
 * no room, accounting, invalidation, concurrent builds and sharing between indices.
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

    private static Query filter(int i) {
        return new TermQuery(new Term("owner", "user" + i));
    }

    private FilteredSegmentStats segment(FilteredStatsCache cache, Query filter) throws IOException {
        return cache.segment(leaf, filter, () -> {
            final FixedBitSet bits = new FixedBitSet(leaf.reader().maxDoc());
            bits.set(0);
            return bits;
        });
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

    public void testTermsOfOneEntryStayWithinHalfTheBound() throws IOException {
        final long bound = 16 * 1024;
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized(bound + "b"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        final FilteredSegmentStats segment = segment(cache, FILTER);
        for (int i = 0; i < 2_000; i++) {
            segment.term(new Term("body", "term" + i), () -> new long[] { 1, 1 });
        }
        assertThat(nodeCache.stats().getMemorySizeInBytes(), lessThanOrEqualTo(bound / 2));
        assertThat("terms past the entry's share are computed, not kept", nodeCache.refusals(), greaterThan(0L));
        assertEquals("the index's share is the whole node cache", nodeCache.stats().getMemorySizeInBytes(), cache.ramBytesUsed());
    }

    public void testManyDistinctFiltersStayWithinTheByteBound() throws IOException {
        final long bound = 64 * 1024;
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized(bound + "b"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        // One restriction per user, each with its own visible bitset, as with per-user document rules.
        for (int user = 0; user < 500; user++) {
            cache.segment(leaf, filter(user), () -> new FixedBitSet(64 * 1024));
        }
        assertThat(nodeCache.weight(), lessThanOrEqualTo(bound));
        assertThat(nodeCache.stats().getMemorySizeInBytes(), lessThanOrEqualTo(bound));
        assertThat(nodeCache.count(), lessThan(500));
        assertThat(nodeCache.evictions(), greaterThan(0L));
    }

    public void testGrowingEntryIsReweighedSoOthersAreEvicted() throws IOException {
        final long bound = 64 * 1024;
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized(bound + "b"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        // About 45kb of small entries, so the growing entry's 32kb share cannot fit beside them.
        for (int user = 0; user < 80; user++) {
            segment(cache, filter(user));
        }
        final int before = nodeCache.count();
        final FilteredSegmentStats growing = segment(cache, FILTER);
        for (int i = 0; i < 2_000; i++) {
            growing.term(new Term("body", "term" + i), () -> new long[] { 1, 1 });
        }
        assertThat("the LRU sees the entry's growth", nodeCache.weight(), greaterThanOrEqualTo(bound / 4));
        assertThat("growth evicted older entries", nodeCache.count(), lessThan(before + 1));
        assertThat(nodeCache.stats().getMemorySizeInBytes(), lessThanOrEqualTo(bound + bound / 4));
    }

    public void testEvictedEntryIsRebuilt() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized("16kb"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        final AtomicInteger builds = new AtomicInteger();
        final FilteredStatsCache.Computation<BitSet> counted = () -> {
            builds.incrementAndGet();
            return new FixedBitSet(leaf.reader().maxDoc());
        };
        cache.segment(leaf, FILTER, counted);
        for (int user = 0; user < 200; user++) {
            segment(cache, filter(user));
        }
        cache.segment(leaf, FILTER, counted);
        assertEquals("the evicted entry must be built again", 2, builds.get());
    }

    public void testHitsAndMissesAreCountedOnce() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final FilteredSegmentStats first = segment(cache, FILTER);
        first.term(new Term("body", "a"), () -> new long[] { 1, 1 });
        first.term(new Term("body", "a"), () -> new long[] { 1, 1 });
        first.field("body", () -> new long[] { 1, 1, 1 });
        segment(cache, FILTER).field("body", () -> new long[] { 1, 1, 1 });
        final FilteredStatsCacheStats stats = nodeCache.stats();
        // Misses: the entry, the term and the field. Hits: the term, the entry and the field.
        assertEquals(3, stats.getMissCount());
        assertEquals(3, stats.getHitCount());
        assertEquals(1, stats.getEntries());
        assertEquals(0, stats.getEvictions());
    }

    public void testBreakerWithoutRoomReturnsTheValueWithoutCachingIt() throws IOException {
        final CircuitBreaker breaker = fieldDataBreaker(100L);
        final IndicesFilteredStatsCache nodeCache = new IndicesFilteredStatsCache(Settings.EMPTY, breaker);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final AtomicInteger builds = new AtomicInteger();
        final BitSet large = new FixedBitSet(64 * 1024);
        for (int i = 0; i < 2; i++) {
            final FilteredSegmentStats segment = cache.segment(leaf, FILTER, () -> {
                builds.incrementAndGet();
                return large;
            });
            assertSame("the computed value is still returned", large, segment.visibleDocs());
        }
        assertEquals("nothing was cached, so each call builds", 2, builds.get());
        assertEquals(0, nodeCache.count());
        assertEquals("a refused entry reserves nothing", 0L, breaker.getUsed());
        assertEquals("each refusal shows as a breaker trip", 2L, breaker.getTrippedCount());
        assertEquals(2L, nodeCache.refusals());
    }

    public void testBreakerAccountingFollowsTheCache() throws IOException {
        final CircuitBreaker breaker = fieldDataBreaker(null);
        final IndicesFilteredStatsCache nodeCache = new IndicesFilteredStatsCache(sized("16kb"), breaker);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final Directory other = oneSegment();
        try (DirectoryReader otherReader = DirectoryReader.open(other)) {
            final LeafReaderContext otherLeaf = otherReader.leaves().get(0);
            for (int user = 0; user < 100; user++) {
                final FilteredSegmentStats segment = cache.segment(otherLeaf, filter(user), () -> new FixedBitSet(1024));
                segment.term(new Term("body", "t"), () -> new long[] { 1, 1 });
            }
            assertThat(nodeCache.stats().getMemorySizeInBytes(), greaterThan(0L));
            assertEquals("evictions return their bytes", nodeCache.stats().getMemorySizeInBytes(), breaker.getUsed());
        }
        assertEquals("closing the segment invalidates its entries", 0, nodeCache.count());
        assertEquals(0L, breaker.getUsed());
        assertEquals(0L, cache.ramBytesUsed());
        other.close();
    }

    public void testValueAddedAfterTheEntryLeftTheCacheIsNotCharged() throws IOException {
        final CircuitBreaker breaker = fieldDataBreaker(null);
        final IndicesFilteredStatsCache nodeCache = new IndicesFilteredStatsCache(Settings.EMPTY, breaker);
        final FilteredStatsCache cache = index("idx", nodeCache);
        // A request still holds the entry when it is invalidated.
        final FilteredSegmentStats held = segment(cache, FILTER);
        cache.clear("test");
        assertEquals(0L, breaker.getUsed());
        assertArrayEquals(new long[] { 4, 5 }, held.term(new Term("body", "late"), () -> new long[] { 4, 5 }));
        assertEquals("nothing is charged to an entry that has left the cache", 0L, breaker.getUsed());
        assertEquals(0L, cache.ramBytesUsed());
    }

    public void testAbsentTermsAreCharged() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final FilteredSegmentStats segment = segment(cache, FILTER);
        final long before = nodeCache.stats().getMemorySizeInBytes();
        final AtomicInteger calls = new AtomicInteger();
        for (int i = 0; i < 2; i++) {
            assertNull(segment.term(new Term("body", "nowhere"), () -> {
                calls.incrementAndGet();
                return null;
            }));
        }
        assertEquals("the absent answer is kept", 1, calls.get());
        assertThat("and it is charged", nodeCache.stats().getMemorySizeInBytes(), greaterThan(before));
    }

    public void testConcurrentMissesBuildTheEntryOnce() throws Exception {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final AtomicInteger builds = new AtomicInteger();
        final CountDownLatch start = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                    cache.segment(leaf, FILTER, () -> {
                        builds.incrementAndGet();
                        return new FixedBitSet(1);
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
        assertEquals(1, builds.get());
    }

    /**
     * A request waiting on a build that another request's cancellation fails must still succeed. Cache treats most such
     * waiters as a miss; segment() covers the rest, which reach the failed build from inside their own compute.
     */
    public void testWaiterBuildsItselfWhenAnotherRequestsBuildFails() throws Exception {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final CountDownLatch building = new CountDownLatch(1);
        final CountDownLatch fail = new CountDownLatch(1);
        final AtomicReference<Throwable> leaderFailure = new AtomicReference<>();
        final Thread leader = new Thread(() -> {
            try {
                cache.segment(leaf, FILTER, () -> {
                    building.countDown();
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
        building.await();
        final BitSet own = new FixedBitSet(1);
        final AtomicReference<BitSet> waiterResult = new AtomicReference<>();
        final AtomicReference<Throwable> waiterFailure = new AtomicReference<>();
        final Thread waiter = new Thread(() -> {
            try {
                waiterResult.set(cache.segment(leaf, FILTER, () -> own).visibleDocs());
            } catch (Throwable t) {
                waiterFailure.set(t);
            }
        });
        waiter.start();
        try {
            assertBusy(() -> assertEquals(Thread.State.WAITING, waiter.getState()));
        } finally {
            fail.countDown();
            leader.join();
            waiter.join();
        }
        assertEquals("leader cancelled", leaderFailure.get().getMessage());
        assertNull("the leader's failure must not reach the waiter", waiterFailure.get());
        assertSame(own, waiterResult.get());
    }

    public void testClearDropsOnlyThatIndex() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache first = index("first", nodeCache);
        final FilteredStatsCache second = index("second", nodeCache);
        try (Directory secondDir = oneSegment(); DirectoryReader secondReader = DirectoryReader.open(secondDir)) {
            segment(first, FILTER);
            second.segment(secondReader.leaves().get(0), FILTER, () -> new FixedBitSet(1));
            assertEquals(2, nodeCache.count());

            first.clear("test");
            assertEquals(0L, first.ramBytesUsed());
            assertThat(second.ramBytesUsed(), greaterThan(0L));
            assertEquals(1, nodeCache.count());
        }
    }

    public void testRefusedEntriesAreNotTracked() throws IOException {
        final IndicesFilteredStatsCache nodeCache = new IndicesFilteredStatsCache(Settings.EMPTY, fieldDataBreaker(10L));
        final FilteredStatsCache cache = index("idx", nodeCache);
        for (int user = 0; user < 100; user++) {
            segment(cache, filter(user));
        }
        assertEquals(100, nodeCache.refusals());
        assertEquals("nothing held for entries that were never cached", 0, nodeCache.trackedKeys());
    }

    public void testZeroSizeTurnsCachingOff() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized("0b"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        final AtomicInteger calls = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            segment(cache, FILTER).field("body", () -> {
                calls.incrementAndGet();
                return new long[] { 1, 1, 1 };
            });
        }
        assertEquals(3, calls.get());
        assertEquals(0, nodeCache.count());
    }

    public void testEntryLargerThanHalfTheCacheIsNotCachedAndEvictsNothing() throws IOException {
        final IndicesFilteredStatsCache nodeCache = unbreakable(sized("64kb"));
        final FilteredStatsCache cache = index("idx", nodeCache);
        for (int user = 0; user < 10; user++) {
            segment(cache, filter(user));
        }
        final int before = nodeCache.count();
        final BitSet large = new FixedBitSet(320 * 1024);
        assertSame(large, cache.segment(leaf, FILTER, () -> large).visibleDocs());
        assertEquals("the small entries survive", before, nodeCache.count());
        assertEquals(1, nodeCache.refusals());
        assertEquals(0L, nodeCache.evictions());
    }

    public void testComputationFailureIsRethrownAndNotCached() {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final IOException failure = new IOException("postings unreadable");
        final IOException thrown = expectThrows(IOException.class, () -> cache.segment(leaf, FILTER, () -> { throw failure; }));
        assertSame(failure, thrown);
        assertEquals(0, nodeCache.count());
    }

    public void testErrorInABuildDoesNotLeaveTheKeyBlocked() throws Exception {
        final IndicesFilteredStatsCache nodeCache = unbreakable(Settings.EMPTY);
        final FilteredStatsCache cache = index("idx", nodeCache);
        final Directory other = oneSegment();
        final DirectoryReader otherReader = DirectoryReader.open(other);
        final LeafReaderContext otherLeaf = otherReader.leaves().get(0);
        final AssertionError error = new AssertionError("allocation failed");
        assertSame(error, expectThrows(AssertionError.class, () -> cache.segment(otherLeaf, FILTER, () -> { throw error; })));
        final BitSet retried = new FixedBitSet(1);
        final AtomicReference<BitSet> result = new AtomicReference<>();
        final Thread retry = new Thread(() -> {
            try {
                result.set(cache.segment(otherLeaf, FILTER, () -> retried).visibleDocs());
                otherReader.close();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        retry.start();
        retry.join(10_000);
        assertFalse("a later caller and the segment close must not wait on the failed build", retry.isAlive());
        assertSame(retried, result.get());
        assertEquals(0, nodeCache.count());
        other.close();
    }
}

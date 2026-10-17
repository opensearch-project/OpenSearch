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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThan;

public class FilteredStatsCacheTests extends OpenSearchTestCase {

    private FilteredStatsCache newCache() {
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("idx", Settings.EMPTY);
        return new FilteredStatsCache(indexSettings, new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
    }

    private static Directory writeSingleSegment() throws IOException {
        final Directory dir = newDirectory();
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                final Document doc = new Document();
                doc.add(new StringField("dept", i == 0 ? "cardiology" : "oncology", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();
        }
        return dir;
    }

    public void testBitSetComputedOnceThenServedFromCache() throws IOException {
        final Directory dir = writeSingleSegment();
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final LeafReaderContext leaf = reader.leaves().get(0);
            final FilteredStatsCache cache = newCache();
            final Query filter = new TermQuery(new Term("dept", "cardiology"));
            final AtomicInteger calls = new AtomicInteger();

            final BitSet first = new FixedBitSet(leaf.reader().maxDoc());
            first.set(0);
            final BitSet miss = cache.getOrComputeVisibleBitSet(leaf, filter, () -> {
                calls.incrementAndGet();
                return first;
            });
            // second call must not recompute -- the compute would fail the test if invoked.
            final BitSet hit = cache.getOrComputeVisibleBitSet(leaf, filter, () -> {
                calls.incrementAndGet();
                return new FixedBitSet(leaf.reader().maxDoc());
            });

            assertSame(first, miss);
            assertSame(first, hit);
            assertEquals("bitset should be computed exactly once", 1, calls.get());
            assertThat("caching a bitset should account bytes", cache.ramBytesUsed(), greaterThan(0L));
        }
        dir.close();
    }

    public void testNoVisibleDocsCachedAsNullWithoutRecompute() throws IOException {
        final Directory dir = writeSingleSegment();
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final LeafReaderContext leaf = reader.leaves().get(0);
            final FilteredStatsCache cache = newCache();
            final Query filter = new TermQuery(new Term("dept", "nonexistent"));
            final AtomicInteger calls = new AtomicInteger();

            final BitSet miss = cache.getOrComputeVisibleBitSet(leaf, filter, () -> {
                calls.incrementAndGet();
                return null; // no visible docs in this segment
            });
            final BitSet hit = cache.getOrComputeVisibleBitSet(leaf, filter, () -> {
                calls.incrementAndGet();
                return new FixedBitSet(leaf.reader().maxDoc());
            });

            assertNull("no-visible-docs must be returned as null", miss);
            assertNull("the null answer must be served from cache", hit);
            assertEquals("null bitset should be computed exactly once", 1, calls.get());
        }
        dir.close();
    }

    public void testFieldAndTermContributionsAreCached() throws IOException {
        final Directory dir = writeSingleSegment();
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final LeafReaderContext leaf = reader.leaves().get(0);
            final FilteredStatsCache cache = newCache();
            final Query filter = new TermQuery(new Term("dept", "cardiology"));

            final long[] field = cache.getOrComputeFieldContribution(leaf, filter, "content", () -> new long[] { 5, 10, 7 });
            assertArrayEquals(new long[] { 5, 10, 7 }, field);
            // cached: recompute lambda must not run.
            final long[] fieldHit = cache.getOrComputeFieldContribution(leaf, filter, "content", () -> {
                throw new AssertionError("field contribution should be served from cache");
            });
            assertArrayEquals(new long[] { 5, 10, 7 }, fieldHit);

            final Term term = new Term("content", "alpha");
            final long[] termMiss = cache.getOrComputeTermContribution(leaf, filter, term, () -> null); // term absent in visible subset
            assertNull(termMiss);
            // the "absent" answer is cached too.
            final long[] termHit = cache.getOrComputeTermContribution(leaf, filter, term, () -> {
                throw new AssertionError("absent term answer should be served from cache");
            });
            assertNull(termHit);
        }
        dir.close();
    }

    /**
     * Concurrent requests can miss the same cold (segment, filter, term) and each build it -- see the note in
     * {@link FilteredStatsCache#getOrComputeTermContribution}, which accepts that duplicate work rather than holding
     * a map lock across the build. This guards the invariant that decision rests on: whichever build wins, every
     * caller sees the same value and the cache keeps one entry. It deliberately asserts no timing, only agreement.
     */
    public void testConcurrentCallersForOneTermAllSeeTheSameValue() throws Exception {
        final Query filter = new TermQuery(new Term("dept", "cardiology"));
        final FilteredStatsCache cache = newCache();
        final Directory dir = writeSingleSegment();
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final LeafReaderContext leaf = reader.leaves().get(0);
            final Term term = new Term("content", "alpha");
            final int threads = 8;
            final CountDownLatch start = new CountDownLatch(1);
            final AtomicInteger builds = new AtomicInteger();
            final List<Future<long[]>> results = new ArrayList<>();
            final ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                for (int i = 0; i < threads; i++) {
                    results.add(pool.submit(() -> {
                        start.await();
                        return cache.getOrComputeTermContribution(leaf, filter, term, () -> {
                            builds.incrementAndGet();
                            return new long[] { 7L, 11L };
                        });
                    }));
                }
                start.countDown();
                for (Future<long[]> f : results) {
                    assertArrayEquals("every racing caller must see the same statistics", new long[] { 7L, 11L }, f.get());
                }
            } finally {
                pool.shutdownNow();
            }
            // At least one build had to happen, and a later caller is served from the cache without building again.
            assertTrue("expected at least one build, got " + builds.get(), builds.get() >= 1);
            final int afterRace = builds.get();
            assertArrayEquals(new long[] { 7L, 11L }, cache.getOrComputeTermContribution(leaf, filter, term, () -> {
                throw new AssertionError("should be cached");
            }));
            assertEquals("no further builds once the race has settled", afterRace, builds.get());
        }
        dir.close();
    }

    /**
     * Warming a newly visible segment should pre-populate the (filter, field) pairs already in use, so the first query
     * to touch that segment is a cache hit rather than paying the visible-postings build itself.
     */
    public void testWarmPrepopulatesInUseFilterAndField() throws IOException {
        final Query filter = new TermQuery(new Term("dept", "cardiology"));
        final FilteredStatsCache cache = newCache();

        // A first segment: querying it records (filter, field) as in-use.
        final Directory first = writeSingleSegment();
        try (DirectoryReader reader = DirectoryReader.open(first)) {
            final LeafReaderContext leaf = reader.leaves().get(0);
            final FixedBitSet visible = new FixedBitSet(leaf.reader().maxDoc());
            visible.set(0);
            cache.getOrComputeVisibleBitSet(leaf, filter, () -> visible);
            cache.getOrComputeFieldContribution(leaf, filter, "dept", () -> new long[] { 1, 1, 1 });
        }
        first.close();

        // A second, independent segment stands in for one that just became visible after a refresh/merge.
        final Directory second = writeSingleSegment();
        try (DirectoryReader reader = DirectoryReader.open(second)) {
            final LeafReaderContext leaf = reader.leaves().get(0);
            final IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);

            cache.warm(searcher, leaf);

            // Both the bitset and the field contribution must now be served from cache: these computations would
            // fail the test if warming had not already populated them.
            assertNotNull("warming should have built the visible bitset", cache.getOrComputeVisibleBitSet(leaf, filter, () -> {
                throw new AssertionError("bitset should be warm");
            }));
            assertNotNull(
                "warming should have built the field contribution",
                cache.getOrComputeFieldContribution(leaf, filter, "dept", () -> {
                    throw new AssertionError("field should be warm");
                })
            );
        }
        second.close();
    }

    /**
     * Cache memory scales with the number of distinct views (filters), not with how selective they are: a visible-doc
     * bitset is {@code maxDoc} bits per (segment, filter) whatever its cardinality. This pins that down so the
     * capacity-planning claim stays honest -- if the representation ever becomes sparse or shared, this test should be
     * updated deliberately rather than silently drifting.
     */
    public void testMemoryScalesWithNumberOfFiltersNotSelectivity() throws IOException {
        final Directory dir = writeSingleSegment();
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final LeafReaderContext leaf = reader.leaves().get(0);
            final int maxDoc = leaf.reader().maxDoc();
            final FilteredStatsCache cache = newCache();

            // One very selective filter (a single visible doc) and one unselective filter (all docs visible).
            final FixedBitSet sparse = new FixedBitSet(maxDoc);
            sparse.set(0);
            final FixedBitSet dense = new FixedBitSet(maxDoc);
            dense.set(0, maxDoc);

            cache.getOrComputeVisibleBitSet(leaf, new TermQuery(new Term("v", "selective")), () -> sparse);
            final long afterSelective = cache.ramBytesUsed();
            cache.getOrComputeVisibleBitSet(leaf, new TermQuery(new Term("v", "unselective")), () -> dense);
            final long afterBoth = cache.ramBytesUsed();

            final long selectiveCost = afterSelective;
            final long unselectiveCost = afterBoth - afterSelective;
            assertEquals(
                "a 1-doc view and an all-docs view must cost the same: cost tracks maxDoc, not cardinality",
                selectiveCost,
                unselectiveCost
            );

            // Each additional distinct view adds another bitset of the same size -- memory is linear in view count.
            for (int i = 0; i < 8; i++) {
                final FixedBitSet bits = new FixedBitSet(maxDoc);
                bits.set(i % maxDoc);
                cache.getOrComputeVisibleBitSet(leaf, new TermQuery(new Term("v", "view" + i)), () -> bits);
            }
            assertEquals("memory should be linear in the number of distinct views", selectiveCost * 10, cache.ramBytesUsed());
        }
        dir.close();
    }

    public void testEntriesEvictedWhenSegmentCloses() throws IOException {
        final Directory dir = writeSingleSegment();
        final FilteredStatsCache cache = newCache();
        final Query filter = new TermQuery(new Term("dept", "cardiology"));
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final LeafReaderContext leaf = reader.leaves().get(0);
            final BitSet bitSet = new FixedBitSet(leaf.reader().maxDoc());
            bitSet.set(0);
            cache.getOrComputeVisibleBitSet(leaf, filter, () -> bitSet);
            assertThat(cache.ramBytesUsed(), greaterThan(0L));
        }
        // Closing the reader closes the segment core, which fires the eviction listener.
        assertEquals("closing the segment must evict its cached entry and return its accounted bytes", 0L, cache.ramBytesUsed());
        dir.close();
    }
}

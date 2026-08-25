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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
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

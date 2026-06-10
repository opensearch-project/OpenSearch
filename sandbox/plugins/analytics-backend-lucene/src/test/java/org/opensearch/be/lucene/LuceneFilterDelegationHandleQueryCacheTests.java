/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that the node-level query cache is wired into
 * {@link LuceneFilterDelegationHandle} so that delegated queries
 * (e.g. MATCH producing a TermQuery) get their per-segment DocIdSets
 * cached across repeated createProvider/scorer calls.
 */
public class LuceneFilterDelegationHandleQueryCacheTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter writer;
    private DirectoryReader reader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        writer = new IndexWriter(directory, new IndexWriterConfig());
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new StringField("tag", i % 2 == 0 ? "hello" : "goodbye", Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();
        reader = DirectoryReader.open(writer);
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        writer.close();
        directory.close();
        super.tearDown();
    }

    /**
     * Verifies that when the delegation handle's searcher has a query cache,
     * the first scorer call produces a miss (populates cache) and the second
     * scorer call on the same segment/query produces a hit.
     *
     * <p>Uses the exact same flow as the real delegation path: createWeight →
     * scorer(leaf) → iterate. The LRUQueryCache counts hits/misses at
     * scorer() time, not createWeight() time.
     */
    public void testDelegationHandleCacheMissAndHit() throws Exception {
        LRUQueryCache cache = new LRUQueryCache(100, 10 * 1024 * 1024, context -> true, 256);
        AlwaysCachePolicy policy = new AlwaysCachePolicy();

        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(cache);
        searcher.setQueryCachingPolicy(policy);

        var leaf = reader.leaves().get(0);
        assertNotNull("Leaf must have a core cache helper for LRUQueryCache to work", leaf.reader().getCoreCacheHelper());

        Query query = new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("tag", "hello"));

        // First scorer — cache miss, populates cache
        Weight weight1 = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        assertTrue("Weight must be cacheable for LRUQueryCache", weight1.isCacheable(leaf));
        Scorer scorer1 = weight1.scorer(leaf);
        assertNotNull(scorer1);
        while (scorer1.iterator().nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        }

        long missAfterFirst = cache.getMissCount();
        long hitAfterFirst = cache.getHitCount();
        assertTrue(
            "Should have at least 1 cache miss after first scorer call, got: "
                + missAfterFirst
                + " (cacheSize="
                + cache.getCacheSize()
                + ", cacheCount="
                + cache.getCacheCount()
                + ")",
            missAfterFirst >= 1
        );
        assertEquals("No hits expected on first call", 0, hitAfterFirst);

        // Second scorer on same (query, segment) — should hit cache
        Weight weight2 = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        Scorer scorer2 = weight2.scorer(leaf);
        assertNotNull(scorer2);
        while (scorer2.iterator().nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        }

        long hitAfterSecond = cache.getHitCount();
        assertTrue("Should have cache hit on second scorer call, got hits: " + hitAfterSecond, hitAfterSecond >= 1);

        assertEquals("Miss count should not increase on cache hit", missAfterFirst, cache.getMissCount());
    }

    /**
     * Verifies that a cached weight still produces correct bitset results.
     * The first scorer call populates the cache, the second hits it — both
     * must return the same 50 matching documents.
     */
    public void testCachedWeightProducesCorrectResults() throws Exception {
        LRUQueryCache cache = new LRUQueryCache(100, 10 * 1024 * 1024, context -> true, 256);
        AlwaysCachePolicy policy = new AlwaysCachePolicy();

        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(cache);
        searcher.setQueryCachingPolicy(policy);

        var leaf = reader.leaves().get(0);
        assertNotNull("Leaf must have a cache helper", leaf.reader().getCoreCacheHelper());

        Query query = new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("tag", "hello"));

        // First scorer — populates cache
        Weight weight1 = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        int count1 = countMatchesOnLeaf(weight1, leaf);
        assertEquals("50 even-indexed docs should match tag=hello", 50, count1);
        assertTrue("Cache miss expected on first call", cache.getMissCount() >= 1);

        // Second scorer — should hit cache and produce same result
        long hitBefore = cache.getHitCount();
        Weight weight2 = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        int count2 = countMatchesOnLeaf(weight2, leaf);
        assertEquals("Cached result should also return 50 docs", 50, count2);
        assertTrue("Cache hit expected on second call", cache.getHitCount() > hitBefore);
    }

    /**
     * Verifies that without a cache set, the searcher's createWeight path
     * does NOT populate any cache (baseline sanity check).
     */
    public void testNoCacheByDefault() throws Exception {
        IndexSearcher searcher = new IndexSearcher(reader);
        // No cache set — this is what the old code did

        Query query = new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("tag", "hello"));
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        int count = countMatchesOnLeaf(weight, reader.leaves().get(0));
        assertEquals(50, count);
    }

    /**
     * Regression for the self-union (multisearch / append) node crash: two separate
     * {@code IndexSearcher} instances over the SAME reader, both wired to one shared query cache,
     * cache a {@code Weight} against searcher A's reader-context then evaluate it via searcher B —
     * tripping Lucene's "top-reader used to create Weight is not the same as the current reader's
     * top-reader" assertion (fatal across the FFM upcall). {@link LuceneReader#searcher} hands out
     * ONE shared searcher per reader, so both delegated scans share a consistent reader-context.
     */
    public void testSharedSearcherSurvivesTwoScansWithSharedCache() throws Exception {
        LRUQueryCache cache = new LRUQueryCache(100, 10 * 1024 * 1024, context -> true, 256);
        AlwaysCachePolicy policy = new AlwaysCachePolicy();
        LuceneReader luceneReader = buildLuceneReader();
        Query query = new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("tag", "hello"));
        var leaf = reader.leaves().get(0);

        // Branch A — populates the shared cache against the shared searcher's reader-context.
        IndexSearcher searcherA = luceneReader.searcher(cache, policy);
        Weight weightA = searcherA.createWeight(searcherA.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        assertEquals("branch A matches 50 docs", 50, countMatchesOnLeaf(weightA, leaf));

        // Branch B — second delegated scan over the same reader. Must be the SAME searcher instance,
        // so reusing the cached Weight does not trip the top-reader assertion.
        IndexSearcher searcherB = luceneReader.searcher(cache, policy);
        assertSame("self-union scans must share one searcher per reader", searcherA, searcherB);
        Weight weightB = searcherB.createWeight(searcherB.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        assertEquals("branch B (cache hit) matches the same 50 docs", 50, countMatchesOnLeaf(weightB, leaf));
        assertTrue("second scan should hit the shared cache", cache.getHitCount() >= 1);
    }

    /**
     * Multi-partition safety: DataFusion fans a scan across partitions that concurrently call
     * {@code scorer(leaf)} on the shared searcher's Weight. Each gets its own Scorer and sees the
     * same matches, with no exception escaping.
     */
    public void testSharedSearcherConcurrentScorersAcrossPartitions() throws Exception {
        LRUQueryCache cache = new LRUQueryCache(100, 10 * 1024 * 1024, context -> true, 256);
        AlwaysCachePolicy policy = new AlwaysCachePolicy();
        IndexSearcher searcher = buildLuceneReader().searcher(cache, policy);
        Query query = new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("tag", "hello"));
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        var leaf = reader.leaves().get(0);

        int partitions = 8;
        ExecutorService pool = Executors.newFixedThreadPool(partitions);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Integer>> futures = new ArrayList<>();
            for (int p = 0; p < partitions; p++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return countMatchesOnLeaf(weight, leaf);
                }));
            }
            start.countDown();
            for (Future<Integer> f : futures) {
                assertEquals("each concurrent partition must see all 50 matches", 50, (int) f.get());
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private int countMatchesOnLeaf(Weight weight, org.apache.lucene.index.LeafReaderContext leaf) throws IOException {
        Scorer scorer = weight.scorer(leaf);
        if (scorer == null) return 0;
        int count = 0;
        var it = scorer.iterator();
        int doc = it.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            count++;
            doc = it.nextDoc();
        }
        return count;
    }

    // ── Helpers ──

    private QueryShardContext mockQueryShardContext(IndexSearcher searcher) {
        QueryShardContext qsc = mock(QueryShardContext.class);
        when(qsc.searcher()).thenReturn(searcher);

        org.opensearch.index.mapper.MappedFieldType fieldType = mock(org.opensearch.index.mapper.MappedFieldType.class);
        when(fieldType.termQuery(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            Object value = invocation.getArgument(0);
            return new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("tag", value.toString()));
        });
        when(qsc.fieldMapper("tag")).thenReturn(fieldType);
        return qsc;
    }

    private LuceneReader buildLuceneReader() {
        Map<Long, String> genMap = new java.util.HashMap<>();
        for (var leaf : reader.leaves()) {
            org.apache.lucene.index.SegmentReader sr = (org.apache.lucene.index.SegmentReader) leaf.reader();
            genMap.put(1L, sr.getSegmentInfo().info.name);
        }
        return new LuceneReader(reader, genMap);
    }

    private CatalogSnapshot mockCatalogSnapshot() {
        return mock(CatalogSnapshot.class);
    }

    private byte[] serializeQueryBuilder(org.opensearch.index.query.QueryBuilder qb, NamedWriteableRegistry registry) throws IOException {
        org.opensearch.common.io.stream.BytesStreamOutput out = new org.opensearch.common.io.stream.BytesStreamOutput();
        out.writeNamedWriteable(qb);
        return org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes());
    }

    private static class AlwaysCachePolicy implements QueryCachingPolicy {
        @Override
        public void onUse(Query query) {}

        @Override
        public boolean shouldCache(Query query) {
            return true;
        }
    }
}

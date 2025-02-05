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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class IndicesQueryCacheTests extends OpenSearchTestCase {

    private static class DummyQuery extends Query {

        private final int id;

        DummyQuery(int id) {
            this.id = id;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && id == ((DummyQuery) obj).id;
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + id;
        }

        @Override
        public String toString(String field) {
            return "dummy";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long l) throws IOException {
                            return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
                        }

                        @Override
                        public long cost() {
                            return 0;
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }

    }

    private static QueryCachingPolicy alwaysCachePolicy() {
        return new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) {

            }

            @Override
            public boolean shouldCache(Query query) {
                return true;
            }
        };
    }

    public void testBasics() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        w.addDocument(new Document());
        DirectoryReader r = DirectoryReader.open(w);
        w.close();
        ShardId shard = new ShardId("index", "_na_", 0);
        r = OpenSearchDirectoryReader.wrap(r, shard);
        IndexSearcher s = new IndexSearcher(r);
        s.setQueryCachingPolicy(alwaysCachePolicy());

        Settings settings = Settings.builder()
            .put(IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING.getKey(), 10)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
        IndicesQueryCache cache = new IndicesQueryCache(settings);
        s.setQueryCache(cache);

        QueryCacheStats stats = cache.getStats(shard);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(0L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(0L, stats.getMissCount());
        assertEquals(0L, stats.getMemorySizeInBytes());

        assertEquals(1, s.count(new DummyQuery(0)));

        stats = cache.getStats(shard);
        assertEquals(1L, stats.getCacheSize());
        assertEquals(1L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(2L, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() > 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);

        for (int i = 1; i < 20; ++i) {
            assertEquals(1, s.count(new DummyQuery(i)));
        }

        stats = cache.getStats(shard);
        assertEquals(10L, stats.getCacheSize());
        assertEquals(20L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(40L, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() > 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);

        s.count(new DummyQuery(10));

        stats = cache.getStats(shard);
        assertEquals(10L, stats.getCacheSize());
        assertEquals(20L, stats.getCacheCount());
        assertEquals(1L, stats.getHitCount());
        assertEquals(40L, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() > 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);

        IOUtils.close(r, dir);

        // got emptied, but no changes to other metrics
        stats = cache.getStats(shard);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(20L, stats.getCacheCount());
        assertEquals(1L, stats.getHitCount());
        assertEquals(40L, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() > 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);

        cache.onClose(shard);

        // forgot everything
        stats = cache.getStats(shard);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(0L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(0L, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() >= 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);

        cache.close(); // this triggers some assertions
    }

    public void testTwoShards() throws IOException {
        Directory dir1 = newDirectory();
        IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
        w1.addDocument(new Document());
        DirectoryReader r1 = DirectoryReader.open(w1);
        w1.close();
        ShardId shard1 = new ShardId("index", "_na_", 0);
        r1 = OpenSearchDirectoryReader.wrap(r1, shard1);
        IndexSearcher s1 = new IndexSearcher(r1);
        s1.setQueryCachingPolicy(alwaysCachePolicy());

        Directory dir2 = newDirectory();
        IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig());
        w2.addDocument(new Document());
        DirectoryReader r2 = DirectoryReader.open(w2);
        w2.close();
        ShardId shard2 = new ShardId("index", "_na_", 1);
        r2 = OpenSearchDirectoryReader.wrap(r2, shard2);
        IndexSearcher s2 = new IndexSearcher(r2);
        s2.setQueryCachingPolicy(alwaysCachePolicy());

        Settings settings = Settings.builder()
            .put(IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING.getKey(), 10)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
        IndicesQueryCache cache = new IndicesQueryCache(settings);
        s1.setQueryCache(cache);
        s2.setQueryCache(cache);

        assertEquals(1, s1.count(new DummyQuery(0)));

        QueryCacheStats stats1 = cache.getStats(shard1);
        assertEquals(1L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());
        assertTrue(stats1.getMemorySizeInBytes() >= 0L && stats1.getMemorySizeInBytes() < Long.MAX_VALUE);

        QueryCacheStats stats2 = cache.getStats(shard2);
        assertEquals(0L, stats2.getCacheSize());
        assertEquals(0L, stats2.getCacheCount());
        assertEquals(0L, stats2.getHitCount());
        assertEquals(0L, stats2.getMissCount());
        assertTrue(stats2.getMemorySizeInBytes() >= 0L && stats2.getMemorySizeInBytes() < Long.MAX_VALUE);

        assertEquals(1, s2.count(new DummyQuery(0)));

        stats1 = cache.getStats(shard1);
        assertEquals(1L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());
        assertTrue(stats1.getMemorySizeInBytes() >= 0L && stats1.getMemorySizeInBytes() < Long.MAX_VALUE);

        stats2 = cache.getStats(shard2);
        assertEquals(1L, stats2.getCacheSize());
        assertEquals(1L, stats2.getCacheCount());
        assertEquals(0L, stats2.getHitCount());
        assertEquals(2L, stats2.getMissCount());
        assertTrue(stats2.getMemorySizeInBytes() >= 0L && stats2.getMemorySizeInBytes() < Long.MAX_VALUE);

        for (int i = 0; i < 20; ++i) {
            assertEquals(1, s2.count(new DummyQuery(i)));
        }

        stats1 = cache.getStats(shard1);
        assertEquals(0L, stats1.getCacheSize()); // evicted
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());
        assertTrue(stats1.getMemorySizeInBytes() >= 0L && stats1.getMemorySizeInBytes() < Long.MAX_VALUE);

        stats2 = cache.getStats(shard2);
        assertEquals(10L, stats2.getCacheSize());
        assertEquals(20L, stats2.getCacheCount());
        assertEquals(1L, stats2.getHitCount());
        assertEquals(40L, stats2.getMissCount());
        assertTrue(stats2.getMemorySizeInBytes() >= 0L && stats2.getMemorySizeInBytes() < Long.MAX_VALUE);

        IOUtils.close(r1, dir1);

        // no changes
        stats1 = cache.getStats(shard1);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());
        assertTrue(stats1.getMemorySizeInBytes() >= 0L && stats1.getMemorySizeInBytes() < Long.MAX_VALUE);

        stats2 = cache.getStats(shard2);
        assertEquals(10L, stats2.getCacheSize());
        assertEquals(20L, stats2.getCacheCount());
        assertEquals(1L, stats2.getHitCount());
        assertEquals(40L, stats2.getMissCount());
        assertTrue(stats2.getMemorySizeInBytes() >= 0L && stats2.getMemorySizeInBytes() < Long.MAX_VALUE);

        cache.onClose(shard1);

        // forgot everything about shard1
        stats1 = cache.getStats(shard1);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(0L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(0L, stats1.getMissCount());
        assertTrue(stats1.getMemorySizeInBytes() >= 0L && stats1.getMemorySizeInBytes() < Long.MAX_VALUE);

        stats2 = cache.getStats(shard2);
        assertEquals(10L, stats2.getCacheSize());
        assertEquals(20L, stats2.getCacheCount());
        assertEquals(1L, stats2.getHitCount());
        assertEquals(40L, stats2.getMissCount());
        assertTrue(stats2.getMemorySizeInBytes() >= 0L && stats2.getMemorySizeInBytes() < Long.MAX_VALUE);

        IOUtils.close(r2, dir2);
        cache.onClose(shard2);

        // forgot everything about shard2
        stats1 = cache.getStats(shard1);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(0L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(0L, stats1.getMissCount());
        assertTrue(stats1.getMemorySizeInBytes() >= 0L && stats1.getMemorySizeInBytes() < Long.MAX_VALUE);

        stats2 = cache.getStats(shard2);
        assertEquals(0L, stats2.getCacheSize());
        assertEquals(0L, stats2.getCacheCount());
        assertEquals(0L, stats2.getHitCount());
        assertEquals(0L, stats2.getMissCount());
        assertTrue(stats2.getMemorySizeInBytes() >= 0L && stats2.getMemorySizeInBytes() < Long.MAX_VALUE);

        cache.close(); // this triggers some assertions
    }

    // Make sure the cache behaves correctly when a segment that is associated
    // with an empty cache gets closed. In that particular case, the eviction
    // callback is called with a number of evicted entries equal to 0
    // see https://github.com/elastic/elasticsearch/issues/15043
    public void testStatsOnEviction() throws IOException {
        Directory dir1 = newDirectory();
        IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
        w1.addDocument(new Document());
        DirectoryReader r1 = DirectoryReader.open(w1);
        w1.close();
        ShardId shard1 = new ShardId("index", "_na_", 0);
        r1 = OpenSearchDirectoryReader.wrap(r1, shard1);
        IndexSearcher s1 = new IndexSearcher(r1);
        s1.setQueryCachingPolicy(alwaysCachePolicy());

        Directory dir2 = newDirectory();
        IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig());
        w2.addDocument(new Document());
        DirectoryReader r2 = DirectoryReader.open(w2);
        w2.close();
        ShardId shard2 = new ShardId("index", "_na_", 1);
        r2 = OpenSearchDirectoryReader.wrap(r2, shard2);
        IndexSearcher s2 = new IndexSearcher(r2);
        s2.setQueryCachingPolicy(alwaysCachePolicy());

        Settings settings = Settings.builder()
            .put(IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING.getKey(), 10)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
        IndicesQueryCache cache = new IndicesQueryCache(settings);
        s1.setQueryCache(cache);
        s2.setQueryCache(cache);

        assertEquals(1, s1.count(new DummyQuery(0)));

        for (int i = 1; i <= 20; ++i) {
            assertEquals(1, s2.count(new DummyQuery(i)));
        }

        QueryCacheStats stats1 = cache.getStats(shard1);
        assertEquals(0L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());

        // this used to fail because we were evicting an empty cache on
        // the segment from r1
        IOUtils.close(r1, dir1);
        cache.onClose(shard1);

        IOUtils.close(r2, dir2);
        cache.onClose(shard2);

        cache.close(); // this triggers some assertions
    }

    private static class DummyWeight extends Weight {

        private final Weight weight;
        private final int randCount = randomIntBetween(0, Integer.MAX_VALUE);
        private boolean scorerCalled;
        private boolean scorerSupplierCalled;
        private boolean countCalled;

        DummyWeight(Weight weight) {
            super(weight.getQuery());
            this.weight = weight;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return weight.explain(context, doc);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            scorerSupplierCalled = true;
            return weight.scorerSupplier(context);
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            countCalled = true;
            return randCount;
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }

    }

    public void testDelegatesScorerSupplier() throws Exception {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        w.addDocument(new Document());
        DirectoryReader r = DirectoryReader.open(w);
        w.close();
        ShardId shard = new ShardId("index", "_na_", 0);
        r = OpenSearchDirectoryReader.wrap(r, shard);
        IndexSearcher s = new IndexSearcher(r);
        s.setQueryCachingPolicy(new QueryCachingPolicy() {
            @Override
            public boolean shouldCache(Query query) throws IOException {
                return false; // never cache
            }

            @Override
            public void onUse(Query query) {}
        });

        Settings settings = Settings.builder()
            .put(IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING.getKey(), 10)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
        IndicesQueryCache cache = new IndicesQueryCache(settings);
        s.setQueryCache(cache);
        Query query = new MatchAllDocsQuery();
        final DummyWeight weight = new DummyWeight(s.createWeight(s.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f));
        final Weight cached = cache.doCache(weight, s.getQueryCachingPolicy());
        assertNotSame(weight, cached);
        assertFalse(weight.scorerCalled);
        assertFalse(weight.scorerSupplierCalled);
        cached.scorerSupplier(s.getIndexReader().leaves().get(0));
        assertFalse(weight.scorerCalled);
        assertTrue(weight.scorerSupplierCalled);
        IOUtils.close(r, dir);
        cache.onClose(shard);
        cache.close();
    }

    public void testDelegatesCount() throws Exception {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        w.addDocument(new Document());
        DirectoryReader r = DirectoryReader.open(w);
        w.close();
        ShardId shard = new ShardId("index", "_na_", 0);
        r = OpenSearchDirectoryReader.wrap(r, shard);
        IndexSearcher s = new IndexSearcher(r);
        IndicesQueryCache cache = new IndicesQueryCache(Settings.EMPTY);
        s.setQueryCache(cache);
        Query query = new MatchAllDocsQuery();
        final DummyWeight weight = new DummyWeight(s.createWeight(s.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f));
        final Weight cached = cache.doCache(weight, s.getQueryCachingPolicy());
        assertFalse(weight.countCalled);
        assertEquals(weight.randCount, cached.count(s.getIndexReader().leaves().get(0)));
        assertTrue(weight.countCalled);
        IOUtils.close(r, dir);
        cache.onClose(shard);
        cache.close();
    }
}

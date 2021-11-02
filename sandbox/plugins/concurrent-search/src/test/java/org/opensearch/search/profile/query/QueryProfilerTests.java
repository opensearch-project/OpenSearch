/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.RandomApproximationQuery;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class QueryProfilerTests extends OpenSearchTestCase {

    private Directory dir;
    private IndexReader reader;
    private ContextIndexSearcher searcher;
    private ExecutorService executor;

    @ParametersFactory
    public static Collection<Object[]> concurrency() {
        return Arrays.asList(new Integer[] { 0 }, new Integer[] { 5 });
    }

    public QueryProfilerTests(int concurrency) {
        this.executor = (concurrency > 0) ? Executors.newFixedThreadPool(concurrency) : null;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        final int numDocs = TestUtil.nextInt(random(), 1, 20);
        for (int i = 0; i < numDocs; ++i) {
            final int numHoles = random().nextInt(5);
            for (int j = 0; j < numHoles; ++j) {
                w.addDocument(new Document());
            }
            Document doc = new Document();
            doc.add(new StringField("foo", "bar", Store.NO));
            w.addDocument(doc);
        }
        reader = w.getReader();
        w.close();
        searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            ALWAYS_CACHE_POLICY,
            true,
            executor
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();

        LRUQueryCache cache = (LRUQueryCache) searcher.getQueryCache();
        assertThat(cache.getHitCount(), equalTo(0L));
        assertThat(cache.getCacheCount(), equalTo(0L));
        assertThat(cache.getTotalCount(), equalTo(cache.getMissCount()));
        assertThat(cache.getCacheSize(), equalTo(0L));

        if (executor != null) {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }

        IOUtils.close(reader, dir);
        dir = null;
        reader = null;
        searcher = null;
    }

    public void testBasic() throws IOException {
        QueryProfiler profiler = new QueryProfiler(executor != null);
        searcher.setProfiler(profiler);
        Query query = new TermQuery(new Term("foo", "bar"));
        searcher.search(query, 1);
        List<ProfileResult> results = profiler.getTree();
        assertEquals(1, results.size());
        Map<String, Long> breakdown = results.get(0).getTimeBreakdown();
        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString()), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString()), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString()), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString()), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString()), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString()), equalTo(0L));

        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString() + "_count"), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString() + "_count"), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString() + "_count"), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString() + "_count"), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString() + "_count"), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString() + "_count"), equalTo(0L));

        long rewriteTime = profiler.getRewriteTime();
        assertThat(rewriteTime, greaterThan(0L));
    }

    public void testNoScoring() throws IOException {
        QueryProfiler profiler = new QueryProfiler(executor != null);
        searcher.setProfiler(profiler);
        Query query = new TermQuery(new Term("foo", "bar"));
        searcher.search(query, 1, Sort.INDEXORDER); // scores are not needed
        List<ProfileResult> results = profiler.getTree();
        assertEquals(1, results.size());
        Map<String, Long> breakdown = results.get(0).getTimeBreakdown();
        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString()), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString()), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString()), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString()), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString()), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString()), equalTo(0L));

        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString() + "_count"), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString() + "_count"), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString() + "_count"), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString() + "_count"), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString() + "_count"), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString() + "_count"), equalTo(0L));

        long rewriteTime = profiler.getRewriteTime();
        assertThat(rewriteTime, greaterThan(0L));
    }

    public void testUseIndexStats() throws IOException {
        QueryProfiler profiler = new QueryProfiler(executor != null);
        searcher.setProfiler(profiler);
        Query query = new TermQuery(new Term("foo", "bar"));
        searcher.count(query); // will use index stats
        List<ProfileResult> results = profiler.getTree();
        assertEquals(1, results.size());
        ProfileResult result = results.get(0);
        assertEquals(0, (long) result.getTimeBreakdown().get("build_scorer_count"));

        long rewriteTime = profiler.getRewriteTime();
        assertThat(rewriteTime, greaterThan(0L));
    }

    public void testApproximations() throws IOException {
        QueryProfiler profiler = new QueryProfiler(executor != null);
        searcher.setProfiler(profiler);
        Query query = new RandomApproximationQuery(new TermQuery(new Term("foo", "bar")), random());
        searcher.count(query);
        List<ProfileResult> results = profiler.getTree();
        assertEquals(1, results.size());
        Map<String, Long> breakdown = results.get(0).getTimeBreakdown();
        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString()), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString()), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString()), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString()), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString()), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString()), greaterThan(0L));

        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString() + "_count"), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString() + "_count"), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString() + "_count"), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString() + "_count"), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString() + "_count"), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString() + "_count"), greaterThan(0L));

        long rewriteTime = profiler.getRewriteTime();
        assertThat(rewriteTime, greaterThan(0L));
    }

    public void testCollector() throws IOException {
        TotalHitCountCollector collector = new TotalHitCountCollector();
        ProfileCollector profileCollector = new ProfileCollector(collector);
        assertEquals(0, profileCollector.getTime());
        final LeafCollector leafCollector = profileCollector.getLeafCollector(reader.leaves().get(0));
        assertThat(profileCollector.getTime(), greaterThan(0L));
        long time = profileCollector.getTime();
        leafCollector.setScorer(null);
        assertThat(profileCollector.getTime(), greaterThan(time));
        time = profileCollector.getTime();
        leafCollector.collect(0);
        assertThat(profileCollector.getTime(), greaterThan(time));
    }

    private static class DummyQuery extends Query {

        @Override
        public String toString(String field) {
            return getClass().getSimpleName();
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new Weight(this) {
                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    return new ScorerSupplier() {

                        @Override
                        public Scorer get(long loadCost) throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public long cost() {
                            return 42;
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }
    }

    public void testScorerSupplier() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        w.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher s = newSearcher(reader);
        s.setQueryCache(null);
        Weight weight = s.createWeight(s.rewrite(new DummyQuery()), randomFrom(ScoreMode.values()), 1f);
        // exception when getting the scorer
        expectThrows(UnsupportedOperationException.class, () -> weight.scorer(s.getIndexReader().leaves().get(0)));
        // no exception, means scorerSupplier is delegated
        weight.scorerSupplier(s.getIndexReader().leaves().get(0));
        reader.close();
        dir.close();
    }

    private static final QueryCachingPolicy ALWAYS_CACHE_POLICY = new QueryCachingPolicy() {

        @Override
        public void onUse(Query query) {}

        @Override
        public boolean shouldCache(Query query) throws IOException {
            return true;
        }

    };
}

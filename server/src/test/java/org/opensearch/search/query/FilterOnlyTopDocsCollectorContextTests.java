/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.test.TestSearchContext;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the FilterOnlyTopDocsCollectorContext optimization (filter-only queries).
 * Parameterized over single-threaded and concurrent search to cover both postProcess
 * and createManager/reduce paths.
 */
public class FilterOnlyTopDocsCollectorContextTests extends IndexShardTestCase {

    private IndexShard indexShard;
    private final ExecutorService executor;
    private final QueryPhaseSearcher queryPhaseSearcher;

    @ParametersFactory
    public static Collection<Object[]> concurrency() {
        return Arrays.asList(
            new Object[] { 0, QueryPhase.DEFAULT_QUERY_PHASE_SEARCHER },
            new Object[] { 5, new ConcurrentQueryPhaseSearcher() }
        );
    }

    public FilterOnlyTopDocsCollectorContextTests(int concurrency, QueryPhaseSearcher queryPhaseSearcher) {
        this.executor = concurrency > 0 ? Executors.newFixedThreadPool(concurrency) : null;
        this.queryPhaseSearcher = queryPhaseSearcher;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexShard = newShard(true);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (executor != null) {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
        closeShards(indexShard);
    }

    // isConstantZeroScoreQuery detection

    public void testIsConstantZeroScoreQueryDetectsBoostQueryWithZeroBoost() {
        assertTrue(TopDocsCollectorContext.isConstantZeroScoreQuery(new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0f)));
    }

    public void testIsConstantZeroScoreQueryRejectsNonZeroBoost() {
        assertFalse(TopDocsCollectorContext.isConstantZeroScoreQuery(new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 1f)));
    }

    public void testIsConstantZeroScoreQueryRejectsSmallPositiveBoost() {
        assertFalse(TopDocsCollectorContext.isConstantZeroScoreQuery(new BoostQuery(new MatchAllDocsQuery(), 0.0001f)));
    }

    public void testIsConstantZeroScoreQueryRejectsMatchAllDocsQuery() {
        assertFalse(TopDocsCollectorContext.isConstantZeroScoreQuery(new MatchAllDocsQuery()));
    }

    public void testIsConstantZeroScoreQueryRejectsTermQuery() {
        assertFalse(TopDocsCollectorContext.isConstantZeroScoreQuery(new TermQuery(new Term("f", "v"))));
    }

    public void testIsConstantZeroScoreQueryRejectsConstantScoreQueryDirectly() {
        assertFalse(TopDocsCollectorContext.isConstantZeroScoreQuery(new ConstantScoreQuery(new MatchAllDocsQuery())));
    }

    // Correct results — matches baseline SimpleTopDocsCollectorContext

    public void testFilterOnlyReturnsCorrectDocumentsMatchingBaseline() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
        for (int i = 0; i < 20; i++) {
            Document doc = new Document();
            if (i % 2 == 0) doc.add(new StringField("tag", "target", Store.NO));
            w.addDocument(doc);
        }
        w.close();

        IndexReader reader = DirectoryReader.open(dir);

        TestSearchContext filterCtx = buildContext(
            reader,
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("tag", "target"))), 0f),
            20,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
        QueryPhase.executeInternal(filterCtx.withCleanQueryResult(), queryPhaseSearcher);

        TestSearchContext baselineCtx = buildContext(
            reader,
            new ConstantScoreQuery(new TermQuery(new Term("tag", "target"))),
            20,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
        QueryPhase.executeInternal(baselineCtx.withCleanQueryResult(), queryPhaseSearcher);

        long filterHits = filterCtx.queryResult().topDocs().topDocs.totalHits.value();
        long baselineHits = baselineCtx.queryResult().topDocs().topDocs.totalHits.value();
        assertThat(filterHits, equalTo(baselineHits));
        assertThat((long) filterCtx.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(filterHits));

        reader.close();
        dir.close();
    }

    public void testFilterOnlyDocumentsHaveZeroScore() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(5, 30);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("f", "v", Store.NO));
            w.addDocument(doc);
        }
        w.close();

        IndexReader reader = DirectoryReader.open(dir);
        TestSearchContext ctx = buildContext(
            reader,
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("f", "v"))), 0f),
            numDocs,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        for (ScoreDoc sd : ctx.queryResult().topDocs().topDocs.scoreDocs) {
            assertThat(sd.score, equalTo(0.0f));
        }
        assertThat(ctx.queryResult().topDocs().maxScore, equalTo(0.0f));

        reader.close();
        dir.close();
    }

    public void testFilterOnlyMaxScoreIsNaNWhenNoResults() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        w.addDocument(new Document());
        w.close();

        IndexReader reader = DirectoryReader.open(dir);
        TestSearchContext ctx = buildContext(
            reader,
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("f", "missing"))), 0f),
            10,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        assertThat(ctx.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(0));
        assertTrue(Float.isNaN(ctx.queryResult().topDocs().maxScore));

        reader.close();
        dir.close();
    }

    // Multi-segment / concurrent collector reduction

    public void testMultiSegmentReduceSumsTotalHitsAndOrdersByDocId() throws Exception {
        Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
        final int segmentCount = 4;
        final int docsPerSegment = 10;
        for (int s = 0; s < segmentCount; s++) {
            for (int d = 0; d < docsPerSegment; d++) {
                Document doc = new Document();
                doc.add(new StringField("tag", "hit", Store.NO));
                iw.addDocument(doc);
            }
            iw.commit();
        }
        iw.close();

        IndexReader reader = DirectoryReader.open(dir);
        int totalExpected = segmentCount * docsPerSegment;
        TestSearchContext ctx = buildContext(
            reader,
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("tag", "hit"))), 0f),
            totalExpected,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.value(), equalTo((long) totalExpected));
        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.relation(), equalTo(TotalHits.Relation.EQUAL_TO));

        ScoreDoc[] scoreDocs = ctx.queryResult().topDocs().topDocs.scoreDocs;
        for (int i = 1; i < scoreDocs.length; i++) {
            assertThat(scoreDocs[i].doc, greaterThan(scoreDocs[i - 1].doc));
        }

        reader.close();
        dir.close();
    }

    public void testSizeCapIsAppliedAfterMultiSegmentReduction() throws Exception {
        Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
        final int total = 50;
        for (int i = 0; i < total; i++) {
            Document doc = new Document();
            doc.add(new StringField("f", "v", Store.NO));
            iw.addDocument(doc);
        }
        iw.commit();
        iw.close();

        IndexReader reader = DirectoryReader.open(dir);
        int size = 10;
        TestSearchContext ctx = buildContext(
            reader,
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("f", "v"))), 0f),
            size,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.value(), equalTo((long) total));
        assertThat(ctx.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(size));

        reader.close();
        dir.close();
    }

    // Total hits tracking modes

    public void testTrackTotalHitsAccurateGivesEqualToRelation() throws Exception {
        int numDocs = randomIntBetween(10, 50);
        Directory dir = buildIndexWithDocs(numDocs);
        IndexReader reader = DirectoryReader.open(dir);

        TestSearchContext ctx = buildContext(
            reader,
            new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0f),
            numDocs,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.value(), equalTo((long) numDocs));
        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.relation(), equalTo(TotalHits.Relation.EQUAL_TO));

        reader.close();
        dir.close();
    }

    public void testTrackTotalHitsDisabledGivesGreaterThanOrEqualToRelation() throws Exception {
        int numDocs = randomIntBetween(10, 50);
        Directory dir = buildIndexWithDocs(numDocs);
        IndexReader reader = DirectoryReader.open(dir);

        TestSearchContext ctx = buildContext(
            reader,
            new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0f),
            numDocs,
            SearchContext.TRACK_TOTAL_HITS_DISABLED
        );
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.relation(), equalTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));

        reader.close();
        dir.close();
    }

    public void testTrackTotalHitsThresholdExceededGivesGreaterThanOrEqualTo() throws Exception {
        Directory dir = buildIndexWithDocs(100);
        IndexReader reader = DirectoryReader.open(dir);

        TestSearchContext ctx = buildContext(reader, new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0f), 10, 50);
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        TotalHits th = ctx.queryResult().topDocs().topDocs.totalHits;
        assertThat(th.value(), greaterThan(0L));
        assertThat(th.relation(), equalTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));

        reader.close();
        dir.close();
    }

    public void testTrackTotalHitsThresholdNotExceededGivesEqualTo() throws Exception {
        Directory dir = buildIndexWithDocs(10);
        IndexReader reader = DirectoryReader.open(dir);

        TestSearchContext ctx = buildContext(reader, new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0f), 10, 50);
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        TotalHits th = ctx.queryResult().topDocs().topDocs.totalHits;
        assertThat(th.value(), equalTo(10L));
        assertThat(th.relation(), equalTo(TotalHits.Relation.EQUAL_TO));

        reader.close();
        dir.close();
    }

    // Query shapes: when optimization IS and IS NOT activated

    public void testBoolFilterOnlyQueryActivatesOptimizationAfterRewrite() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(5, 20);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("status", "active", Store.NO));
            w.addDocument(doc);
        }
        w.addDocument(new Document());
        w.close();

        IndexReader reader = DirectoryReader.open(dir);
        ContextIndexSearcher searcher = newContextSearcher(reader);

        Query rawBoolFilter = new BooleanQuery.Builder().add(new TermQuery(new Term("status", "active")), Occur.FILTER).build();
        Query rewritten = searcher.rewrite(rawBoolFilter);
        assertTrue(TopDocsCollectorContext.isConstantZeroScoreQuery(rewritten));

        TestSearchContext ctx = new TestSearchContext(null, indexShard, searcher);
        ctx.parsedQuery(new ParsedQuery(rewritten));
        ctx.setSize(numDocs + 1);
        ctx.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.value(), equalTo((long) numDocs));

        reader.close();
        dir.close();
    }

    public void testNonZeroBoostQueryBypassesOptimizationAndScoresCorrectly() throws Exception {
        Directory dir = buildIndexWithDocs(5);
        IndexReader reader = DirectoryReader.open(dir);

        Query q = new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 2f);
        assertFalse(TopDocsCollectorContext.isConstantZeroScoreQuery(q));

        TestSearchContext ctx = buildContext(reader, q, 10, SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.value(), equalTo(5L));
        for (ScoreDoc sd : ctx.queryResult().topDocs().topDocs.scoreDocs) {
            assertThat(sd.score, equalTo(2.0f));
        }

        reader.close();
        dir.close();
    }

    public void testSortBypassesOptimizationButStillWorks() throws Exception {
        Directory dir = newDirectory();
        Sort sort = new Sort(new SortField("rank", SortField.Type.INT));
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig().setIndexSort(sort));
        for (int i = 0; i < 10; i++)
            w.addDocument(new Document());
        w.close();

        IndexReader reader = DirectoryReader.open(dir);
        ContextIndexSearcher searcher = newContextSearcher(reader);
        Query q = searcher.rewrite(new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0f));

        TestSearchContext ctx = new TestSearchContext(null, indexShard, searcher);
        ctx.parsedQuery(new ParsedQuery(q));
        ctx.setSize(10);
        ctx.sort(new SortAndFormats(sort, new DocValueFormat[] { DocValueFormat.RAW }));
        ctx.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.value(), equalTo(10L));

        reader.close();
        dir.close();
    }

    public void testFilterOnlyWithNoMatchingDocumentsReturnsZeroHits() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        w.addDocument(new Document());
        w.close();

        IndexReader reader = DirectoryReader.open(dir);
        ContextIndexSearcher searcher = newContextSearcher(reader);
        // TermQuery on a non-existent field rewrites cleanly and matches nothing
        Query q = searcher.rewrite(new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("f", "missing"))), 0f));

        TestSearchContext ctx = new TestSearchContext(null, indexShard, searcher);
        ctx.parsedQuery(new ParsedQuery(q));
        ctx.setSize(10);
        ctx.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.value(), equalTo(0L));
        assertThat(ctx.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(0));

        reader.close();
        dir.close();
    }

    // from + size pagination

    public void testFromPlusSizePaginationCollectsCorrectWindow() throws Exception {
        Directory dir = buildIndexWithDocs(30);
        IndexReader reader = DirectoryReader.open(dir);

        ContextIndexSearcher searcher = newContextSearcher(reader);
        Query q = searcher.rewrite(new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0f));
        TestSearchContext ctx = new TestSearchContext(null, indexShard, searcher);
        ctx.parsedQuery(new ParsedQuery(q));
        ctx.from(10);
        ctx.setSize(5);
        ctx.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        QueryPhase.executeInternal(ctx.withCleanQueryResult(), queryPhaseSearcher);

        assertThat(ctx.queryResult().topDocs().topDocs.totalHits.value(), equalTo(30L));
        assertThat(ctx.queryResult().topDocs().topDocs.scoreDocs.length, lessThanOrEqualTo(15));

        reader.close();
        dir.close();
    }

    // helpers

    private TestSearchContext buildContext(IndexReader reader, Query query, int size, int trackTotalHitsUpTo) throws IOException {
        ContextIndexSearcher searcher = newContextSearcher(reader);
        Query rewritten = searcher.rewrite(query);
        TestSearchContext ctx = new TestSearchContext(null, indexShard, searcher);
        ctx.parsedQuery(new ParsedQuery(rewritten));
        ctx.setSize(size);
        ctx.trackTotalHitsUpTo(trackTotalHitsUpTo);
        ctx.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        return ctx;
    }

    private Directory buildIndexWithDocs(int count) throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        for (int i = 0; i < count; i++)
            w.addDocument(new Document());
        w.close();
        return dir;
    }

    private ContextIndexSearcher newContextSearcher(IndexReader reader) throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        IndexShard shard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(shard);
        when(shard.getSearchOperationListener()).thenReturn(new SearchOperationListener() {
        });
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(executor != null);
        if (executor != null) {
            when(searchContext.getTargetMaxSliceCount()).thenReturn(randomIntBetween(0, 2));
        } else {
            when(searchContext.getTargetMaxSliceCount()).thenThrow(IllegalStateException.class);
        }
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            executor,
            searchContext
        );
    }
}

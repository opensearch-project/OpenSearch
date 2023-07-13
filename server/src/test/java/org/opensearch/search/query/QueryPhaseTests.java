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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.search.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.search.OpenSearchToParentBlockJoinQuery;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.lucene.queries.MinDocQuery;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.tasks.TaskCancelledException;
import org.opensearch.test.TestSearchContext;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.opensearch.search.query.TopDocsCollectorContext.hasInfMaxScore;

public class QueryPhaseTests extends IndexShardTestCase {
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

    public QueryPhaseTests(int concurrency, QueryPhaseSearcher queryPhaseSearcher) {
        this.executor = (concurrency > 0) ? Executors.newFixedThreadPool(concurrency) : null;
        this.queryPhaseSearcher = queryPhaseSearcher;
    }

    @Override
    public Settings threadPoolSettings() {
        return Settings.builder().put(super.threadPoolSettings()).put("thread_pool.search.min_queue_size", 10).build();
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

    private void countTestCase(Query query, IndexReader reader, boolean shouldCollectSearch, boolean shouldCollectCount) throws Exception {
        ContextIndexSearcher searcher = shouldCollectSearch
            ? newContextSearcher(reader, executor)
            : newEarlyTerminationContextSearcher(reader, 0, executor);
        TestSearchContext context = new TestSearchContext(null, indexShard, searcher);
        context.parsedQuery(new ParsedQuery(query));
        context.setSize(0);
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        final boolean rescore = QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertFalse(rescore);

        ContextIndexSearcher countSearcher = shouldCollectCount
            ? newContextSearcher(reader, executor)
            : newEarlyTerminationContextSearcher(reader, 0, executor);
        assertEquals(countSearcher.count(query), context.queryResult().topDocs().topDocs.totalHits.value);
    }

    private void countTestCase(boolean withDeletions) throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(600, 900);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            if (randomBoolean()) {
                doc.add(new StringField("foo", "bar", Store.NO));
                doc.add(new SortedSetDocValuesField("foo", new BytesRef("bar")));
                doc.add(new SortedSetDocValuesField("docValuesOnlyField", new BytesRef("bar")));
                doc.add(new LatLonDocValuesField("latLonDVField", 1.0, 1.0));
                doc.add(new LatLonPoint("latLonDVField", 1.0, 1.0));
            }
            if (randomBoolean()) {
                doc.add(new StringField("foo", "baz", Store.NO));
                doc.add(new SortedSetDocValuesField("foo", new BytesRef("baz")));
            }
            if (withDeletions && (rarely() || i == 0)) {
                doc.add(new StringField("delete", "yes", Store.NO));
            }
            w.addDocument(doc);
        }
        if (withDeletions) {
            w.deleteDocuments(new Term("delete", "yes"));
        }
        final IndexReader reader = w.getReader();
        Query matchAll = new MatchAllDocsQuery();
        Query matchAllCsq = new ConstantScoreQuery(matchAll);
        Query tq = new TermQuery(new Term("foo", "bar"));
        Query tCsq = new ConstantScoreQuery(tq);
        Query dvfeq = new DocValuesFieldExistsQuery("foo");
        Query dvfeq_points = new DocValuesFieldExistsQuery("latLonDVField");
        Query dvfeqCsq = new ConstantScoreQuery(dvfeq);
        // field with doc-values but not indexed will need to collect
        Query dvOnlyfeq = new DocValuesFieldExistsQuery("docValuesOnlyField");
        BooleanQuery bq = new BooleanQuery.Builder().add(matchAll, Occur.SHOULD).add(tq, Occur.MUST).build();

        countTestCase(matchAll, reader, false, false);
        countTestCase(matchAllCsq, reader, false, false);
        countTestCase(tq, reader, withDeletions, withDeletions);
        countTestCase(tCsq, reader, withDeletions, withDeletions);
        countTestCase(dvfeq, reader, withDeletions, true);
        countTestCase(dvfeq_points, reader, withDeletions, true);
        countTestCase(dvfeqCsq, reader, withDeletions, true);
        countTestCase(dvOnlyfeq, reader, true, true);
        countTestCase(bq, reader, true, true);
        reader.close();
        w.close();
        dir.close();
    }

    public void testCountWithoutDeletions() throws Exception {
        countTestCase(false);
    }

    public void testCountWithDeletions() throws Exception {
        countTestCase(true);
    }

    public void testPostFilterDisablesCountOptimization() throws Exception {
        Directory dir = newDirectory();
        final Sort sort = new Sort(new SortField("rank", SortField.Type.INT));
        IndexWriterConfig iwc = newIndexWriterConfig().setIndexSort(sort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        Document doc = new Document();
        w.addDocument(doc);
        w.close();

        IndexReader reader = DirectoryReader.open(dir);

        TestSearchContext context = new TestSearchContext(null, indexShard, newEarlyTerminationContextSearcher(reader, 0, executor));
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));

        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertEquals(1, context.queryResult().topDocs().topDocs.totalHits.value);

        context.setSearcher(newContextSearcher(reader, executor));
        context.parsedPostFilter(new ParsedQuery(new MatchNoDocsQuery()));
        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertEquals(0, context.queryResult().topDocs().topDocs.totalHits.value);
        reader.close();
        dir.close();
    }

    public void testTerminateAfterWithFilter() throws Exception {
        Directory dir = newDirectory();
        final Sort sort = new Sort(new SortField("rank", SortField.Type.INT));
        IndexWriterConfig iwc = newIndexWriterConfig().setIndexSort(sort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        Document doc = new Document();
        for (int i = 0; i < 10; i++) {
            doc.add(new StringField("foo", Integer.toString(i), Store.NO));
        }
        w.addDocument(doc);
        w.close();

        IndexReader reader = DirectoryReader.open(dir);

        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, executor));
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));

        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        context.terminateAfter(1);
        context.setSize(10);
        for (int i = 0; i < 10; i++) {
            context.parsedPostFilter(new ParsedQuery(new TermQuery(new Term("foo", Integer.toString(i)))));
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertEquals(1, context.queryResult().topDocs().topDocs.totalHits.value);
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
        }
        reader.close();
        dir.close();
    }

    public void testMinScoreDisablesCountOptimization() throws Exception {
        Directory dir = newDirectory();
        final Sort sort = new Sort(new SortField("rank", SortField.Type.INT));
        IndexWriterConfig iwc = newIndexWriterConfig().setIndexSort(sort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        Document doc = new Document();
        w.addDocument(doc);
        w.close();

        IndexReader reader = DirectoryReader.open(dir);
        TestSearchContext context = new TestSearchContext(null, indexShard, newEarlyTerminationContextSearcher(reader, 0, executor));
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        context.setSize(0);
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertEquals(1, context.queryResult().topDocs().topDocs.totalHits.value);

        context.minimumScore(100);
        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertEquals(0, context.queryResult().topDocs().topDocs.totalHits.value);
        assertEquals(TotalHits.Relation.EQUAL_TO, context.queryResult().topDocs().topDocs.totalHits.relation);
        reader.close();
        dir.close();
    }

    public void testQueryCapturesThreadPoolStats() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(600, 900);
        for (int i = 0; i < numDocs; ++i) {
            w.addDocument(new Document());
        }
        w.close();
        IndexReader reader = DirectoryReader.open(dir);
        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, executor));
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));

        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        QuerySearchResult results = context.queryResult();
        assertThat(results.serviceTimeEWMA(), greaterThanOrEqualTo(0L));
        assertThat(results.nodeQueueSize(), greaterThanOrEqualTo(0));
        reader.close();
        dir.close();
    }

    public void testInOrderScrollOptimization() throws Exception {
        Directory dir = newDirectory();
        final Sort sort = new Sort(new SortField("rank", SortField.Type.INT));
        IndexWriterConfig iwc = newIndexWriterConfig().setIndexSort(sort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(600, 900);
        for (int i = 0; i < numDocs; ++i) {
            w.addDocument(new Document());
        }
        w.close();
        IndexReader reader = DirectoryReader.open(dir);
        ScrollContext scrollContext = new ScrollContext();
        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, executor), scrollContext);
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        context.sort(new SortAndFormats(sort, new DocValueFormat[] { DocValueFormat.RAW }));
        scrollContext.lastEmittedDoc = null;
        scrollContext.maxScore = Float.NaN;
        scrollContext.totalHits = null;
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        int size = randomIntBetween(2, 5);
        context.setSize(size);

        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertNull(context.queryResult().terminatedEarly());
        assertThat(context.terminateAfter(), equalTo(0));
        assertThat(context.queryResult().getTotalHits().value, equalTo((long) numDocs));

        context.setSearcher(newEarlyTerminationContextSearcher(reader, size, executor));
        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertThat(context.queryResult().getTotalHits().value, equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0].doc, greaterThanOrEqualTo(size));
        reader.close();
        dir.close();
    }

    public void testTerminateAfterEarlyTermination() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(600, 900);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            if (randomBoolean()) {
                doc.add(new StringField("foo", "bar", Store.NO));
            }
            if (randomBoolean()) {
                doc.add(new StringField("foo", "baz", Store.NO));
            }
            doc.add(new NumericDocValuesField("rank", numDocs - i));
            w.addDocument(doc);
        }
        w.close();
        final IndexReader reader = DirectoryReader.open(dir);
        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, executor));
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        if (this.executor != null) {
            context.setConcurrentSegmentSearchEnabled(true);
        }

        context.terminateAfter(numDocs);
        {
            context.setSize(10);
            final TestTotalHitCountCollectorManager manager = TestTotalHitCountCollectorManager.create(executor);
            context.queryCollectorManagers().put(TotalHitCountCollector.class, manager);
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertFalse(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(10));
            assertThat(manager.getTotalHits(), equalTo(numDocs));
        }

        context.terminateAfter(1);
        {
            context.setSize(1);
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));

            context.setSize(0);
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(0));
        }

        {
            context.setSize(1);
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
        }
        {
            context.setSize(1);
            BooleanQuery bq = new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
                .build();
            context.parsedQuery(new ParsedQuery(bq));
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));

            context.setSize(0);
            context.parsedQuery(new ParsedQuery(bq));
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(0));
        }
        {
            context.setSize(1);
            final TestTotalHitCountCollectorManager manager = TestTotalHitCountCollectorManager.create(executor, 1);
            context.queryCollectorManagers().put(TotalHitCountCollector.class, manager);
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertThat(manager.getTotalHits(), equalTo(1));
            context.queryCollectorManagers().clear();
        }
        {
            context.setSize(0);
            final TestTotalHitCountCollectorManager manager = TestTotalHitCountCollectorManager.create(executor, 1);
            context.queryCollectorManagers().put(TotalHitCountCollector.class, manager);
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(0));
            assertThat(manager.getTotalHits(), equalTo(1));
        }

        // tests with trackTotalHits and terminateAfter
        context.terminateAfter(10);
        context.setSize(0);
        for (int trackTotalHits : new int[] { -1, 3, 76, 100 }) {
            context.trackTotalHitsUpTo(trackTotalHits);
            final TestTotalHitCountCollectorManager manager = TestTotalHitCountCollectorManager.create(executor);
            context.queryCollectorManagers().put(TotalHitCountCollector.class, manager);
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            if (trackTotalHits == -1) {
                assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(0L));
            } else {
                assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) Math.min(trackTotalHits, 10)));
            }
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(0));
            // The concurrent search terminates the collection when the number of hits is reached by each
            // concurrent collector. In this case, in general, the number of results are multiplied by the number of
            // slices (as the unit of concurrency). To address that, we have to use the shared global state,
            // much as HitsThresholdChecker does.
            if (executor == null) {
                assertThat(manager.getTotalHits(), equalTo(10));
            }
        }

        context.terminateAfter(7);
        context.setSize(10);
        for (int trackTotalHits : new int[] { -1, 3, 75, 100 }) {
            context.trackTotalHitsUpTo(trackTotalHits);
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            if (trackTotalHits == -1) {
                assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(0L));
            } else {
                assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(7L));
            }
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(7));
        }
        reader.close();
        dir.close();
    }

    public void testIndexSortingEarlyTermination() throws Exception {
        Directory dir = newDirectory();
        final Sort sort = new Sort(new SortField("rank", SortField.Type.INT));
        IndexWriterConfig iwc = newIndexWriterConfig().setIndexSort(sort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(600, 900);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            if (randomBoolean()) {
                doc.add(new StringField("foo", "bar", Store.NO));
            }
            if (randomBoolean()) {
                doc.add(new StringField("foo", "baz", Store.NO));
            }
            doc.add(new NumericDocValuesField("rank", numDocs - i));
            w.addDocument(doc);
        }
        w.close();

        final IndexReader reader = DirectoryReader.open(dir);
        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, executor));
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        context.setSize(1);
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.sort(new SortAndFormats(sort, new DocValueFormat[] { DocValueFormat.RAW }));

        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
        assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0], instanceOf(FieldDoc.class));
        FieldDoc fieldDoc = (FieldDoc) context.queryResult().topDocs().topDocs.scoreDocs[0];
        assertThat(fieldDoc.fields[0], equalTo(1));

        {
            context.parsedPostFilter(new ParsedQuery(new MinDocQuery(1)));
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(numDocs - 1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));
            context.parsedPostFilter(null);

            final TestTotalHitCountCollectorManager manager = TestTotalHitCountCollectorManager.create(executor, sort);
            context.queryCollectorManagers().put(TotalHitCountCollector.class, manager);
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));
            // When searching concurrently, each executors short-circuits when "size" is reached,
            // including total hits collector
            assertThat(manager.getTotalHits(), lessThanOrEqualTo(numDocs));
            context.queryCollectorManagers().clear();
        }

        {
            context.setSearcher(newEarlyTerminationContextSearcher(reader, 1, executor));
            context.trackTotalHitsUpTo(SearchContext.TRACK_TOTAL_HITS_DISABLED);
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));

            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));
        }
        reader.close();
        dir.close();
    }

    public void testIndexSortScrollOptimization() throws Exception {
        Directory dir = newDirectory();
        final Sort indexSort = new Sort(new SortField("rank", SortField.Type.INT), new SortField("tiebreaker", SortField.Type.INT));
        IndexWriterConfig iwc = newIndexWriterConfig().setIndexSort(indexSort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(600, 900);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("rank", random().nextInt()));
            doc.add(new NumericDocValuesField("tiebreaker", i));
            w.addDocument(doc);
        }
        if (randomBoolean()) {
            w.forceMerge(randomIntBetween(1, 10));
        }
        w.close();

        final IndexReader reader = DirectoryReader.open(dir);
        List<SortAndFormats> searchSortAndFormats = new ArrayList<>();
        searchSortAndFormats.add(new SortAndFormats(indexSort, new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW }));
        // search sort is a prefix of the index sort
        searchSortAndFormats.add(new SortAndFormats(new Sort(indexSort.getSort()[0]), new DocValueFormat[] { DocValueFormat.RAW }));
        for (SortAndFormats searchSortAndFormat : searchSortAndFormats) {
            ScrollContext scrollContext = new ScrollContext();
            TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, executor), scrollContext);
            context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
            scrollContext.lastEmittedDoc = null;
            scrollContext.maxScore = Float.NaN;
            scrollContext.totalHits = null;
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            context.setSize(10);
            context.sort(searchSortAndFormat);

            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.terminateAfter(), equalTo(0));
            assertThat(context.queryResult().getTotalHits().value, equalTo((long) numDocs));
            int sizeMinus1 = context.queryResult().topDocs().topDocs.scoreDocs.length - 1;
            FieldDoc lastDoc = (FieldDoc) context.queryResult().topDocs().topDocs.scoreDocs[sizeMinus1];

            context.setSearcher(newEarlyTerminationContextSearcher(reader, 10, executor));
            QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
            assertThat(context.terminateAfter(), equalTo(0));
            assertThat(context.queryResult().getTotalHits().value, equalTo((long) numDocs));
            FieldDoc firstDoc = (FieldDoc) context.queryResult().topDocs().topDocs.scoreDocs[0];
            for (int i = 0; i < searchSortAndFormat.sort.getSort().length; i++) {
                @SuppressWarnings("unchecked")
                FieldComparator<Object> comparator = (FieldComparator<Object>) searchSortAndFormat.sort.getSort()[i].getComparator(
                    1,
                    false
                );
                int cmp = comparator.compareValues(firstDoc.fields[i], lastDoc.fields[i]);
                if (cmp == 0) {
                    continue;
                }
                assertThat(cmp, equalTo(1));
                break;
            }
        }
        reader.close();
        dir.close();
    }

    public void testDisableTopScoreCollection() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new StandardAnalyzer());
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        Document doc = new Document();
        final int numDocs = 2 * scaledRandomIntBetween(50, 450);
        for (int i = 0; i < numDocs; i++) {
            doc.clear();
            if (i % 2 == 0) {
                doc.add(new TextField("title", "foo bar", Store.NO));
            } else {
                doc.add(new TextField("title", "foo", Store.NO));
            }
            w.addDocument(doc);
        }
        w.close();

        IndexReader reader = DirectoryReader.open(dir);
        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, executor));
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        Query q = new SpanNearQuery.Builder("title", true).addClause(new SpanTermQuery(new Term("title", "foo")))
            .addClause(new SpanTermQuery(new Term("title", "bar")))
            .build();

        context.parsedQuery(new ParsedQuery(q));
        context.setSize(3);
        context.trackTotalHitsUpTo(3);
        TopDocsCollectorContext topDocsContext = TopDocsCollectorContext.createTopDocsCollectorContext(context, false);
        assertEquals(topDocsContext.create(null).scoreMode(), org.apache.lucene.search.ScoreMode.COMPLETE);
        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertEquals(numDocs / 2, context.queryResult().topDocs().topDocs.totalHits.value);
        assertEquals(context.queryResult().topDocs().topDocs.totalHits.relation, TotalHits.Relation.EQUAL_TO);
        assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(3));

        context.sort(new SortAndFormats(new Sort(new SortField("other", SortField.Type.INT)), new DocValueFormat[] { DocValueFormat.RAW }));
        topDocsContext = TopDocsCollectorContext.createTopDocsCollectorContext(context, false);
        assertEquals(topDocsContext.create(null).scoreMode(), org.apache.lucene.search.ScoreMode.TOP_DOCS);
        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertEquals(numDocs / 2, context.queryResult().topDocs().topDocs.totalHits.value);
        assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(3));
        assertEquals(context.queryResult().topDocs().topDocs.totalHits.relation, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);

        reader.close();
        dir.close();
    }

    public void testEnhanceSortOnNumeric() throws Exception {
        final String fieldNameLong = "long-field";
        final String fieldNameDate = "date-field";
        MappedFieldType fieldTypeLong = new NumberFieldMapper.NumberFieldType(fieldNameLong, NumberFieldMapper.NumberType.LONG);
        MappedFieldType fieldTypeDate = new DateFieldMapper.DateFieldType(fieldNameDate);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType(fieldNameLong)).thenReturn(fieldTypeLong);
        when(mapperService.fieldType(fieldNameDate)).thenReturn(fieldTypeDate);
        // enough docs to have a tree with several leaf nodes
        final int numDocs = 3500 * 5;
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(null));
        long firstValue = randomLongBetween(-10000000L, 10000000L);
        long longValue = firstValue;
        long dateValue = randomLongBetween(0, 3000000000000L);
        for (int i = 1; i <= numDocs; ++i) {
            Document doc = new Document();

            doc.add(new LongPoint(fieldNameLong, longValue));
            doc.add(new NumericDocValuesField(fieldNameLong, longValue));

            doc.add(new LongPoint(fieldNameDate, dateValue));
            doc.add(new NumericDocValuesField(fieldNameDate, dateValue));
            writer.addDocument(doc);
            longValue++;
            dateValue++;
            if (i % 3500 == 0) writer.commit();
        }
        writer.close();
        final IndexReader reader = DirectoryReader.open(dir);
        final SortField sortFieldLong = new SortField(fieldNameLong, SortField.Type.LONG);
        sortFieldLong.setMissingValue(Long.MAX_VALUE);
        final SortField sortFieldDate = new SortField(fieldNameDate, SortField.Type.LONG);
        sortFieldDate.setMissingValue(Long.MAX_VALUE);
        DocValueFormat dateFormat = fieldTypeDate.docValueFormat(null, null);
        final Sort longSort = new Sort(sortFieldLong);
        final Sort longDateSort = new Sort(sortFieldLong, sortFieldDate);
        final Sort dateSort = new Sort(sortFieldDate);
        final Sort dateLongSort = new Sort(sortFieldDate, sortFieldLong);
        SortAndFormats longSortAndFormats = new SortAndFormats(longSort, new DocValueFormat[] { DocValueFormat.RAW });
        SortAndFormats longDateSortAndFormats = new SortAndFormats(longDateSort, new DocValueFormat[] { DocValueFormat.RAW, dateFormat });
        SortAndFormats dateSortAndFormats = new SortAndFormats(dateSort, new DocValueFormat[] { dateFormat });
        SortAndFormats dateLongSortAndFormats = new SortAndFormats(dateLongSort, new DocValueFormat[] { dateFormat, DocValueFormat.RAW });
        ParsedQuery query = new ParsedQuery(new MatchAllDocsQuery());
        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());

        // 1. Test a sort on long field
        {
            TestSearchContext searchContext = spy(new TestSearchContext(null, indexShard, newContextSearcher(reader, executor)));
            when(searchContext.mapperService()).thenReturn(mapperService);
            searchContext.sort(longSortAndFormats);
            searchContext.parsedQuery(query);
            searchContext.setTask(task);
            searchContext.setSize(10);
            QueryPhase.executeInternal(searchContext.withCleanQueryResult(), queryPhaseSearcher);
            assertSortResults(searchContext.queryResult().topDocs().topDocs, (long) numDocs, false);
        }

        // 2. Test a sort on long field + date field
        {
            TestSearchContext searchContext = spy(new TestSearchContext(null, indexShard, newContextSearcher(reader, executor)));
            when(searchContext.mapperService()).thenReturn(mapperService);
            searchContext.sort(longDateSortAndFormats);
            searchContext.parsedQuery(query);
            searchContext.setTask(task);
            searchContext.setSize(10);
            QueryPhase.executeInternal(searchContext.withCleanQueryResult(), queryPhaseSearcher);
            assertSortResults(searchContext.queryResult().topDocs().topDocs, (long) numDocs, true);
        }

        // 3. Test a sort on date field
        {
            TestSearchContext searchContext = spy(new TestSearchContext(null, indexShard, newContextSearcher(reader, executor)));
            when(searchContext.mapperService()).thenReturn(mapperService);
            searchContext.sort(dateSortAndFormats);
            searchContext.parsedQuery(query);
            searchContext.setTask(task);
            searchContext.setSize(10);
            QueryPhase.executeInternal(searchContext.withCleanQueryResult(), queryPhaseSearcher);
            assertSortResults(searchContext.queryResult().topDocs().topDocs, (long) numDocs, false);
        }

        // 4. Test a sort on date field + long field
        {
            TestSearchContext searchContext = spy(new TestSearchContext(null, indexShard, newContextSearcher(reader, executor)));
            when(searchContext.mapperService()).thenReturn(mapperService);
            searchContext.sort(dateLongSortAndFormats);
            searchContext.parsedQuery(query);
            searchContext.setTask(task);
            searchContext.setSize(10);
            QueryPhase.executeInternal(searchContext, queryPhaseSearcher);
            assertSortResults(searchContext.queryResult().topDocs().topDocs, (long) numDocs, true);
        }

        // 5. Test that sort optimization is run when from > 0 and size = 0
        {
            TestSearchContext searchContext = spy(new TestSearchContext(null, indexShard, newContextSearcher(reader, executor)));
            when(searchContext.mapperService()).thenReturn(mapperService);
            searchContext.sort(longSortAndFormats);
            searchContext.parsedQuery(query);
            searchContext.setTask(task);
            searchContext.from(5);
            searchContext.setSize(0);
            QueryPhase.executeInternal(searchContext.withCleanQueryResult(), queryPhaseSearcher);
            assertSortResults(searchContext.queryResult().topDocs().topDocs, (long) numDocs, false);
        }

        // 6. Test that sort optimization works with from = 0 and size= 0
        {
            TestSearchContext searchContext = spy(new TestSearchContext(null, indexShard, newContextSearcher(reader, executor)));
            when(searchContext.mapperService()).thenReturn(mapperService);
            searchContext.sort(longSortAndFormats);
            searchContext.parsedQuery(query);
            searchContext.setTask(task);
            searchContext.setSize(0);
            QueryPhase.executeInternal(searchContext, queryPhaseSearcher);
        }

        // 7. Test that sort optimization works with search after
        {
            TestSearchContext searchContext = spy(new TestSearchContext(null, indexShard, newContextSearcher(reader, executor)));
            when(searchContext.mapperService()).thenReturn(mapperService);
            int afterDocument = (int) randomLongBetween(0, 50);
            long afterValue = firstValue + afterDocument;
            FieldDoc after = new FieldDoc(afterDocument, Float.NaN, new Long[] { afterValue });
            searchContext.searchAfter(after);
            searchContext.sort(longSortAndFormats);
            searchContext.parsedQuery(query);
            searchContext.setTask(task);
            searchContext.setSize(10);
            QueryPhase.executeInternal(searchContext.withCleanQueryResult(), queryPhaseSearcher);
            final TopDocs topDocs = searchContext.queryResult().topDocs().topDocs;
            long topValue = (long) ((FieldDoc) topDocs.scoreDocs[0]).fields[0];
            assertThat(topValue, greaterThan(afterValue));
            assertSortResults(topDocs, (long) numDocs, false);

            final TotalHits totalHits = topDocs.totalHits;
            assertEquals(TotalHits.Relation.EQUAL_TO, totalHits.relation);
            assertEquals(numDocs, totalHits.value);
        }

        reader.close();
        dir.close();
    }

    public void testMaxScoreQueryVisitor() {
        BitSetProducer producer = context -> new FixedBitSet(1);
        Query query = new OpenSearchToParentBlockJoinQuery(new MatchAllDocsQuery(), producer, ScoreMode.Avg, "nested");
        assertTrue(hasInfMaxScore(query));

        query = new OpenSearchToParentBlockJoinQuery(new MatchAllDocsQuery(), producer, ScoreMode.None, "nested");
        assertFalse(hasInfMaxScore(query));

        for (Occur occur : Occur.values()) {
            query = new BooleanQuery.Builder().add(
                new OpenSearchToParentBlockJoinQuery(new MatchAllDocsQuery(), producer, ScoreMode.Avg, "nested"),
                occur
            ).build();
            if (occur == Occur.MUST) {
                assertTrue(hasInfMaxScore(query));
            } else {
                assertFalse(hasInfMaxScore(query));
            }

            query = new BooleanQuery.Builder().add(
                new BooleanQuery.Builder().add(
                    new OpenSearchToParentBlockJoinQuery(new MatchAllDocsQuery(), producer, ScoreMode.Avg, "nested"),
                    occur
                ).build(),
                occur
            ).build();
            if (occur == Occur.MUST) {
                assertTrue(hasInfMaxScore(query));
            } else {
                assertFalse(hasInfMaxScore(query));
            }

            query = new BooleanQuery.Builder().add(
                new BooleanQuery.Builder().add(
                    new OpenSearchToParentBlockJoinQuery(new MatchAllDocsQuery(), producer, ScoreMode.Avg, "nested"),
                    occur
                ).build(),
                Occur.FILTER
            ).build();
            assertFalse(hasInfMaxScore(query));

            query = new BooleanQuery.Builder().add(
                new BooleanQuery.Builder().add(new SpanTermQuery(new Term("field", "foo")), occur)
                    .add(new OpenSearchToParentBlockJoinQuery(new MatchAllDocsQuery(), producer, ScoreMode.Avg, "nested"), occur)
                    .build(),
                occur
            ).build();
            if (occur == Occur.MUST) {
                assertTrue(hasInfMaxScore(query));
            } else {
                assertFalse(hasInfMaxScore(query));
            }
        }
    }

    // assert score docs are in order and their number is as expected
    private void assertSortResults(TopDocs topDocs, long expectedNumDocs, boolean isDoubleSort) {
        if (topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
            assertThat(topDocs.totalHits.value, lessThanOrEqualTo(expectedNumDocs));
        } else {
            assertEquals(topDocs.totalHits.value, expectedNumDocs);
        }
        long cur1, cur2;
        long prev1 = Long.MIN_VALUE;
        long prev2 = Long.MIN_VALUE;
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            cur1 = (long) ((FieldDoc) scoreDoc).fields[0];
            assertThat(cur1, greaterThanOrEqualTo(prev1)); // test that docs are properly sorted on the first sort
            if (isDoubleSort) {
                cur2 = (long) ((FieldDoc) scoreDoc).fields[1];
                if (cur1 == prev1) {
                    assertThat(cur2, greaterThanOrEqualTo(prev2)); // test that docs are properly sorted on the secondary sort
                }
                prev2 = cur2;
            }
            prev1 = cur1;
        }
    }

    public void testMinScore() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("foo", "bar", Store.NO));
            doc.add(new StringField("filter", "f1", Store.NO));
            w.addDocument(doc);
        }
        w.close();

        IndexReader reader = DirectoryReader.open(dir);
        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, executor));
        context.parsedQuery(
            new ParsedQuery(
                new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.MUST)
                    .add(new TermQuery(new Term("filter", "f1")), Occur.SHOULD)
                    .build()
            )
        );
        context.minimumScore(0.01f);
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.setSize(1);
        context.trackTotalHitsUpTo(5);

        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertEquals(10, context.queryResult().topDocs().topDocs.totalHits.value);

        reader.close();
        dir.close();
    }

    public void testMaxScore() throws Exception {
        Directory dir = newDirectory();
        final Sort sort = new Sort(new SortField("filter", SortField.Type.STRING));
        IndexWriterConfig iwc = newIndexWriterConfig().setIndexSort(sort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        final int numDocs = scaledRandomIntBetween(600, 900);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("foo", "bar", Store.NO));
            doc.add(new StringField("filter", "f1" + ((i > 0) ? " " + Integer.toString(i) : ""), Store.NO));
            doc.add(new SortedDocValuesField("filter", newBytesRef("f1" + ((i > 0) ? " " + Integer.toString(i) : ""))));
            w.addDocument(doc);
        }
        w.close();

        IndexReader reader = DirectoryReader.open(dir);
        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, executor));
        context.trackScores(true);
        context.parsedQuery(
            new ParsedQuery(
                new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.MUST)
                    .add(new TermQuery(new Term("filter", "f1")), Occur.SHOULD)
                    .build()
            )
        );
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.setSize(1);
        context.trackTotalHitsUpTo(5);

        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertFalse(Float.isNaN(context.queryResult().getMaxScore()));
        assertEquals(1, context.queryResult().topDocs().topDocs.scoreDocs.length);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, greaterThanOrEqualTo(6L));

        context.sort(new SortAndFormats(sort, new DocValueFormat[] { DocValueFormat.RAW }));
        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertFalse(Float.isNaN(context.queryResult().getMaxScore()));
        assertEquals(1, context.queryResult().topDocs().topDocs.scoreDocs.length);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, greaterThanOrEqualTo(6L));

        context.trackScores(false);
        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertTrue(Float.isNaN(context.queryResult().getMaxScore()));
        assertEquals(1, context.queryResult().topDocs().topDocs.scoreDocs.length);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, greaterThanOrEqualTo(6L));

        reader.close();
        dir.close();
    }

    public void testCollapseQuerySearchResults() throws Exception {
        Directory dir = newDirectory();
        final Sort sort = new Sort(new SortField("user", SortField.Type.INT));
        IndexWriterConfig iwc = newIndexWriterConfig().setIndexSort(sort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        // Always end up with uneven buckets so collapsing is predictable
        final int numDocs = 2 * scaledRandomIntBetween(600, 900) - 1;
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("foo", "bar", Store.NO));
            doc.add(new NumericDocValuesField("user", i & 1));
            w.addDocument(doc);
        }
        w.close();

        IndexReader reader = DirectoryReader.open(dir);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.fieldMapper("user")).thenReturn(
            new NumberFieldType("user", NumberType.INTEGER, true, false, true, false, null, Collections.emptyMap())
        );

        TestSearchContext context = new TestSearchContext(queryShardContext, indexShard, newContextSearcher(reader, executor));
        context.collapse(new CollapseBuilder("user").build(context.getQueryShardContext()));
        context.trackScores(true);
        context.parsedQuery(new ParsedQuery(new TermQuery(new Term("foo", "bar"))));
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.setSize(2);
        context.trackTotalHitsUpTo(5);

        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertFalse(Float.isNaN(context.queryResult().getMaxScore()));
        assertEquals(2, context.queryResult().topDocs().topDocs.scoreDocs.length);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().topDocs, instanceOf(CollapseTopFieldDocs.class));

        CollapseTopFieldDocs topDocs = (CollapseTopFieldDocs) context.queryResult().topDocs().topDocs;
        assertThat(topDocs.collapseValues.length, equalTo(2));
        assertThat(topDocs.collapseValues[0], equalTo(0L)); // user == 0
        assertThat(topDocs.collapseValues[1], equalTo(1L)); // user == 1

        context.sort(new SortAndFormats(sort, new DocValueFormat[] { DocValueFormat.RAW }));
        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertFalse(Float.isNaN(context.queryResult().getMaxScore()));
        assertEquals(2, context.queryResult().topDocs().topDocs.scoreDocs.length);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().topDocs, instanceOf(CollapseTopFieldDocs.class));

        topDocs = (CollapseTopFieldDocs) context.queryResult().topDocs().topDocs;
        assertThat(topDocs.collapseValues.length, equalTo(2));
        assertThat(topDocs.collapseValues[0], equalTo(0L)); // user == 0
        assertThat(topDocs.collapseValues[1], equalTo(1L)); // user == 1

        context.trackScores(false);
        QueryPhase.executeInternal(context.withCleanQueryResult(), queryPhaseSearcher);
        assertTrue(Float.isNaN(context.queryResult().getMaxScore()));
        assertEquals(2, context.queryResult().topDocs().topDocs.scoreDocs.length);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().topDocs, instanceOf(CollapseTopFieldDocs.class));

        topDocs = (CollapseTopFieldDocs) context.queryResult().topDocs().topDocs;
        assertThat(topDocs.collapseValues.length, equalTo(2));
        assertThat(topDocs.collapseValues[0], equalTo(0L)); // user == 0
        assertThat(topDocs.collapseValues[1], equalTo(1L)); // user == 1

        reader.close();
        dir.close();
    }

    public void testCancellationDuringPreprocess() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {

            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < i; j++) {
                    sb.append('a');
                }
                doc.add(new StringField("foo", sb.toString(), Store.NO));
                w.addDocument(doc);
            }
            w.flush();
            w.close();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                TestSearchContext context = new TestSearchContextWithRewriteAndCancellation(
                    null,
                    indexShard,
                    newContextSearcher(reader, executor)
                );
                PrefixQuery prefixQuery = new PrefixQuery(new Term("foo", "a"), MultiTermQuery.SCORING_BOOLEAN_REWRITE);
                context.parsedQuery(new ParsedQuery(prefixQuery));
                SearchShardTask task = mock(SearchShardTask.class);
                when(task.isCancelled()).thenReturn(true);
                context.setTask(task);
                expectThrows(TaskCancelledException.class, () -> new QueryPhase().preProcess(context));
            }
        }
    }

    public void testQueryTimeoutChecker() throws Exception {
        long timeCacheLifespan = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        long timeTolerance = timeCacheLifespan / 20;

        // should throw time exceed exception for sure after timeCacheLifespan*2+timeTolerance (next's next cached time is available)
        assertThrows(
            QueryPhase.TimeExceededException.class,
            () -> createTimeoutCheckerThenWaitThenRun(timeCacheLifespan, timeCacheLifespan * 2 + timeTolerance, true, false)
        );

        // should not throw time exceed exception after timeCacheLifespan+timeTolerance because new cached time - init time < timeout
        createTimeoutCheckerThenWaitThenRun(timeCacheLifespan, timeCacheLifespan + timeTolerance, true, false);

        // should not throw time exceed exception after timeout < timeCacheLifespan when cached time didn't change
        createTimeoutCheckerThenWaitThenRun(timeCacheLifespan / 2, timeCacheLifespan / 2 + timeTolerance, false, true);
        createTimeoutCheckerThenWaitThenRun(timeCacheLifespan / 4, timeCacheLifespan / 2 + timeTolerance, false, true);
    }

    private void createTimeoutCheckerThenWaitThenRun(
        long timeout,
        long sleepAfterCreation,
        boolean checkCachedTimeChanged,
        boolean checkCachedTimeHasNotChanged
    ) throws Exception {
        long timeCacheLifespan = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        long timeTolerance = timeCacheLifespan / 20;
        long currentTimeDiffWithCachedTime = TimeValue.nsecToMSec(System.nanoTime()) - threadPool.relativeTimeInMillis();
        // need to run this test approximately at the start of cached time window
        long timeToAlignTimeWithCachedTimeOffset = timeCacheLifespan - currentTimeDiffWithCachedTime + timeTolerance;
        Thread.sleep(timeToAlignTimeWithCachedTimeOffset);

        long initialRelativeCachedTime = threadPool.relativeTimeInMillis();
        SearchContext mockedSearchContext = mock(SearchContext.class);
        when(mockedSearchContext.timeout()).thenReturn(TimeValue.timeValueMillis(timeout));
        when(mockedSearchContext.getRelativeTimeInMillis()).thenAnswer(invocation -> threadPool.relativeTimeInMillis());
        when(mockedSearchContext.getRelativeTimeInMillis(eq(false))).thenCallRealMethod();
        Runnable queryTimeoutChecker = QueryPhase.createQueryTimeoutChecker(mockedSearchContext);
        // make sure next time slot become available
        Thread.sleep(sleepAfterCreation);
        if (checkCachedTimeChanged) {
            assertNotEquals(initialRelativeCachedTime, threadPool.relativeTimeInMillis());
        }
        if (checkCachedTimeHasNotChanged) {
            assertEquals(initialRelativeCachedTime, threadPool.relativeTimeInMillis());
        }
        queryTimeoutChecker.run();
        verify(mockedSearchContext, times(1)).timeout();
        verify(mockedSearchContext, times(1)).getRelativeTimeInMillis(eq(false));
        verify(mockedSearchContext, atLeastOnce()).getRelativeTimeInMillis();
        verifyNoMoreInteractions(mockedSearchContext);
    }

    private static class TestSearchContextWithRewriteAndCancellation extends TestSearchContext {

        private TestSearchContextWithRewriteAndCancellation(
            QueryShardContext queryShardContext,
            IndexShard indexShard,
            ContextIndexSearcher searcher
        ) {
            super(queryShardContext, indexShard, searcher);
        }

        @Override
        public void preProcess(boolean rewrite) {
            try {
                searcher().rewrite(query());
            } catch (IOException e) {
                fail("IOException shouldn't be thrown");
            }
        }

        @Override
        public boolean lowLevelCancellation() {
            return true;
        }
    }

    private static ContextIndexSearcher newContextSearcher(IndexReader reader, ExecutorService executor) throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
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

    private static ContextIndexSearcher newEarlyTerminationContextSearcher(IndexReader reader, int size, ExecutorService executor)
        throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            executor,
            searchContext
        ) {

            @Override
            public void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                final Collector in = new AssertingEarlyTerminationFilterCollector(collector, size);
                super.search(leaves, weight, in);
            }
        };
    }

    private static class TestTotalHitCountCollectorManager extends TotalHitCountCollectorManager {
        private int totalHits;
        private final TotalHitCountCollector collector;
        private final Integer teminateAfter;

        static TestTotalHitCountCollectorManager create(final ExecutorService executor) {
            return create(executor, null, null);
        }

        static TestTotalHitCountCollectorManager create(final ExecutorService executor, final Integer teminateAfter) {
            return create(executor, null, teminateAfter);
        }

        static TestTotalHitCountCollectorManager create(final ExecutorService executor, final Sort sort) {
            return create(executor, sort, null);
        }

        static TestTotalHitCountCollectorManager create(final ExecutorService executor, final Sort sort, final Integer teminateAfter) {
            if (executor == null) {
                return new TestTotalHitCountCollectorManager(new TotalHitCountCollector(), sort);
            } else {
                return new TestTotalHitCountCollectorManager(sort, teminateAfter);
            }
        }

        private TestTotalHitCountCollectorManager(final TotalHitCountCollector collector, final Sort sort) {
            super(sort);
            this.collector = collector;
            this.teminateAfter = null;
        }

        private TestTotalHitCountCollectorManager(final Sort sort, final Integer teminateAfter) {
            super(sort);
            this.collector = null;
            this.teminateAfter = teminateAfter;
        }

        @Override
        public TotalHitCountCollector newCollector() throws IOException {
            return (collector == null) ? super.newCollector() : collector;
        }

        @Override
        public ReduceableSearchResult reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
            totalHits = collectors.stream().mapToInt(TotalHitCountCollector::getTotalHits).sum();

            if (teminateAfter != null) {
                assertThat(totalHits, greaterThanOrEqualTo(teminateAfter));
                totalHits = Math.min(totalHits, teminateAfter);
            }
            // this collector should not participate in reduce as it is added for test purposes to capture the totalHits count
            // returning a ReduceableSearchResult modifies the QueryResult which is not expected
            return (result) -> {};
        }

        public int getTotalHits() {
            return (collector == null) ? totalHits : collector.getTotalHits();
        }
    }

    private static class AssertingEarlyTerminationFilterCollector extends FilterCollector {
        private final int size;

        AssertingEarlyTerminationFilterCollector(Collector in, int size) {
            super(in);
            this.size = size;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            final LeafCollector in = super.getLeafCollector(context);
            return new FilterLeafCollector(in) {
                int collected;

                @Override
                public void collect(int doc) throws IOException {
                    assert collected <= size : "should not collect more than " + size + " doc per segment, got " + collected;
                    ++collected;
                    super.collect(doc);
                }
            };
        }
    }
}

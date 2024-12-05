/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.SourceFieldMatchQuery;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.lucene.queries.MinDocQuery;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.test.TestSearchContext;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryProfilePhaseTests extends IndexShardTestCase {
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

    public QueryProfilePhaseTests(int concurrency, QueryPhaseSearcher queryPhaseSearcher) {
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

        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertEquals(1, context.queryResult().topDocs().topDocs.totalHits.value);
        // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
        // see: https://github.com/apache/lucene/pull/672
        assertProfileData(context, "ConstantScoreQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_count"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });

        context.setSearcher(newContextSearcher(reader, executor));
        context.parsedPostFilter(new ParsedQuery(new MatchNoDocsQuery()));
        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertEquals(0, context.queryResult().topDocs().topDocs.totalHits.value);
        assertProfileData(context, collector -> {
            assertThat(collector.getReason(), equalTo("search_post_filter"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), hasSize(1));
            assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_count"));
            assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
        }, (query) -> {
            assertThat(query.getQueryName(), equalTo("MatchNoDocsQuery"));
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, (query) -> {
            // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
            // see: https://github.com/apache/lucene/pull/672
            assertThat(query.getQueryName(), equalTo("ConstantScoreQuery"));
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        });

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
            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertEquals(1, context.queryResult().topDocs().topDocs.totalHits.value);
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertProfileData(context, collector -> {
                assertThat(collector.getReason(), equalTo("search_post_filter"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), hasSize(1));
                assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_terminate_after_count"));
                assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren().get(0).getProfiledChildren(), hasSize(1));
                assertThat(collector.getProfiledChildren().get(0).getProfiledChildren().get(0).getReason(), equalTo("search_top_hits"));
                assertThat(collector.getProfiledChildren().get(0).getProfiledChildren().get(0).getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getProfiledChildren().get(0).getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getProfiledChildren().get(0).getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
            }, (query) -> {
                assertThat(query.getQueryName(), equalTo("TermQuery"));
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            }, (query) -> {
                assertThat(query.getQueryName(), equalTo("MatchAllDocsQuery"));
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), equalTo(1L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            });
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
        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertEquals(1, context.queryResult().topDocs().topDocs.totalHits.value);
        // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
        // see: https://github.com/apache/lucene/pull/672
        assertProfileData(context, "ConstantScoreQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_count"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });

        context.minimumScore(100);
        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertEquals(0, context.queryResult().topDocs().topDocs.totalHits.value);
        assertEquals(TotalHits.Relation.EQUAL_TO, context.queryResult().topDocs().topDocs.totalHits.relation);
        assertProfileData(context, "MatchAllDocsQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), greaterThanOrEqualTo(1L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(1L));
            if (executor != null) {
                long maxScore = query.getTimeBreakdown().get("max_score");
                long minScore = query.getTimeBreakdown().get("min_score");
                long avgScore = query.getTimeBreakdown().get("avg_score");
                assertThat(maxScore, greaterThanOrEqualTo(1L));
                assertThat(minScore, greaterThanOrEqualTo(1L));
                assertThat(avgScore, greaterThanOrEqualTo(1L));
                assertThat(maxScore, greaterThanOrEqualTo(avgScore));
                assertThat(avgScore, greaterThanOrEqualTo(minScore));
                assertThat(query.getTimeBreakdown().get("max_score_count"), equalTo(1L));
                assertThat(query.getTimeBreakdown().get("min_score_count"), equalTo(1L));
                assertThat(query.getTimeBreakdown().get("avg_score_count"), equalTo(1L));
            }
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_min_score"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), hasSize(1));
            assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_count"));
            assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
        });

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

        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertNull(context.queryResult().terminatedEarly());
        assertThat(context.terminateAfter(), equalTo(0));
        assertThat(context.queryResult().getTotalHits().value, equalTo((long) numDocs));
        assertProfileData(context, "ConstantScoreQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_top_hits"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });

        context.setSearcher(newEarlyTerminationContextSearcher(reader, size, executor));
        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertThat(context.queryResult().getTotalHits().value, equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0].doc, greaterThanOrEqualTo(size));
        assertProfileData(context, "ConstantScoreQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("score"), equalTo(0L));
            assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("score_count"), equalTo(0L));
            assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_top_hits"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), hasSize(0));
        });

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

        context.terminateAfter(1);
        {
            context.setSize(1);
            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertProfileData(context, "MatchAllDocsQuery", query -> {
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), greaterThan(0L));
                if (executor != null) {
                    assertThat(query.getTimeBreakdown().get("max_score"), greaterThan(0L));
                    assertThat(query.getTimeBreakdown().get("min_score"), greaterThan(0L));
                    assertThat(query.getTimeBreakdown().get("avg_score"), greaterThan(0L));
                    assertThat(query.getTimeBreakdown().get("max_score_count"), greaterThan(0L));
                    assertThat(query.getTimeBreakdown().get("min_score_count"), greaterThan(0L));
                    assertThat(query.getTimeBreakdown().get("avg_score_count"), greaterThan(0L));
                }
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            }, collector -> {
                assertThat(collector.getReason(), equalTo("search_terminate_after_count"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), hasSize(1));
                assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_top_hits"));
                assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
            });

            context.setSize(0);
            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(0));
            // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
            // see: https://github.com/apache/lucene/pull/672
            assertProfileData(context, "ConstantScoreQuery", query -> {
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            }, collector -> {
                assertThat(collector.getReason(), equalTo("search_terminate_after_count"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), hasSize(1));
                assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_count"));
                assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
            });
        }

        {
            context.setSize(1);
            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertProfileData(context, "MatchAllDocsQuery", query -> {
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), greaterThan(0L));
                if (executor != null) {
                    assertThat(query.getTimeBreakdown().get("max_score"), greaterThan(0L));
                    assertThat(query.getTimeBreakdown().get("min_score"), greaterThan(0L));
                    assertThat(query.getTimeBreakdown().get("avg_score"), greaterThan(0L));
                    assertThat(query.getTimeBreakdown().get("max_score_count"), greaterThan(0L));
                    assertThat(query.getTimeBreakdown().get("min_score_count"), greaterThan(0L));
                    assertThat(query.getTimeBreakdown().get("avg_score_count"), greaterThan(0L));
                }
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            }, collector -> {
                assertThat(collector.getReason(), equalTo("search_terminate_after_count"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), hasSize(1));
                assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_top_hits"));
                assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
            });
        }
        {
            context.setSize(1);
            BooleanQuery bq = new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
                .build();
            context.parsedQuery(new ParsedQuery(bq));
            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertProfileData(context, "BooleanQuery", query -> {
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), greaterThan(0L));
                if (executor != null) {
                    long maxScore = query.getTimeBreakdown().get("max_score");
                    long minScore = query.getTimeBreakdown().get("min_score");
                    long avgScore = query.getTimeBreakdown().get("avg_score");
                    long maxScoreCount = query.getTimeBreakdown().get("max_score_count");
                    long minScoreCount = query.getTimeBreakdown().get("min_score_count");
                    long avgScoreCount = query.getTimeBreakdown().get("avg_score_count");
                    assertThat(maxScore, greaterThan(0L));
                    assertThat(minScore, greaterThanOrEqualTo(0L));
                    assertThat(avgScore, greaterThanOrEqualTo(0L));
                    assertThat(maxScore, greaterThanOrEqualTo(avgScore));
                    assertThat(avgScore, greaterThanOrEqualTo(minScore));
                    assertThat(maxScoreCount, greaterThan(0L));
                    assertThat(minScoreCount, greaterThanOrEqualTo(0L));
                    assertThat(avgScoreCount, greaterThanOrEqualTo(0L));
                    assertThat(maxScoreCount, greaterThanOrEqualTo(avgScoreCount));
                    assertThat(avgScoreCount, greaterThanOrEqualTo(minScoreCount));
                }
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));

                assertThat(query.getProfiledChildren(), hasSize(2));
                assertThat(query.getProfiledChildren().get(0).getQueryName(), equalTo("TermQuery"));
                assertThat(query.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight_count"), equalTo(1L));

                assertThat(query.getProfiledChildren().get(1).getQueryName(), equalTo("TermQuery"));
                assertThat(query.getProfiledChildren().get(1).getTime(), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            }, collector -> {
                assertThat(collector.getReason(), equalTo("search_terminate_after_count"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), hasSize(1));
                assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_top_hits"));
                assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
            });
            context.setSize(0);
            context.parsedQuery(new ParsedQuery(bq));
            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(0));

            // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
            // see: https://github.com/apache/lucene/pull/672
            assertProfileData(context, "ConstantScoreQuery", query -> {
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));

                // rewritten as a ConstantScoreQuery wrapping the original BooleanQuery
                // see: https://github.com/apache/lucene/pull/672
                assertThat(query.getProfiledChildren(), hasSize(1));
                assertThat(query.getProfiledChildren().get(0).getQueryName(), equalTo("BooleanQuery"));
                assertThat(query.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("score_count"), equalTo(0L));

                List<ProfileResult> children = query.getProfiledChildren().get(0).getProfiledChildren();
                assertThat(children, hasSize(2));
                assertThat(children.get(0).getQueryName(), equalTo("TermQuery"));
                assertThat(children.get(0).getTime(), greaterThan(0L));
                assertThat(children.get(0).getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(children.get(0).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
                assertThat(children.get(0).getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(children.get(0).getTimeBreakdown().get("score_count"), equalTo(0L));

                assertThat(children.get(1).getQueryName(), equalTo("TermQuery"));
                assertThat(children.get(1).getTime(), greaterThan(0L));
                assertThat(children.get(1).getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(children.get(1).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
                assertThat(children.get(1).getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(children.get(1).getTimeBreakdown().get("score_count"), equalTo(0L));
            }, collector -> {
                assertThat(collector.getReason(), equalTo("search_terminate_after_count"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), hasSize(1));
                assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_count"));
                assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
            });
        }

        context.terminateAfter(7);
        context.setSize(10);
        for (int trackTotalHits : new int[] { -1, 3, 75, 100 }) {
            context.trackTotalHitsUpTo(trackTotalHits);
            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertTrue(context.queryResult().terminatedEarly());
            if (trackTotalHits == -1) {
                assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(0L));
            } else {
                assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(7L));
            }
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(7));
            assertProfileData(context, "BooleanQuery", query -> {
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), greaterThanOrEqualTo(7L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));

                assertThat(query.getProfiledChildren(), hasSize(2));
                assertThat(query.getProfiledChildren().get(0).getQueryName(), equalTo("TermQuery"));
                assertThat(query.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("score"), greaterThanOrEqualTo(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("score_count"), greaterThanOrEqualTo(0L));
                if (executor != null) {
                    long maxScore = query.getProfiledChildren().get(0).getTimeBreakdown().get("max_score");
                    long minScore = query.getProfiledChildren().get(0).getTimeBreakdown().get("min_score");
                    long avgScore = query.getProfiledChildren().get(0).getTimeBreakdown().get("avg_score");
                    long maxScoreCount = query.getProfiledChildren().get(0).getTimeBreakdown().get("max_score_count");
                    long minScoreCount = query.getProfiledChildren().get(0).getTimeBreakdown().get("min_score_count");
                    long avgScoreCount = query.getProfiledChildren().get(0).getTimeBreakdown().get("avg_score_count");
                    assertThat(maxScore, greaterThanOrEqualTo(0L));
                    assertThat(minScore, greaterThanOrEqualTo(0L));
                    assertThat(avgScore, greaterThanOrEqualTo(0L));
                    assertThat(maxScore, greaterThanOrEqualTo(avgScore));
                    assertThat(avgScore, greaterThanOrEqualTo(minScore));
                    assertThat(maxScoreCount, greaterThanOrEqualTo(0L));
                    assertThat(minScoreCount, greaterThanOrEqualTo(0L));
                    assertThat(avgScoreCount, greaterThanOrEqualTo(0L));
                    assertThat(maxScoreCount, greaterThanOrEqualTo(avgScoreCount));
                    assertThat(avgScoreCount, greaterThanOrEqualTo(minScoreCount));
                }

                assertThat(query.getProfiledChildren().get(1).getQueryName(), equalTo("TermQuery"));
                assertThat(query.getProfiledChildren().get(1).getTime(), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
                assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("score"), greaterThanOrEqualTo(0L));
                assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("score_count"), greaterThanOrEqualTo(0L));
                if (executor != null) {
                    long maxScore = query.getProfiledChildren().get(1).getTimeBreakdown().get("max_score");
                    long minScore = query.getProfiledChildren().get(1).getTimeBreakdown().get("min_score");
                    long avgScore = query.getProfiledChildren().get(1).getTimeBreakdown().get("avg_score");
                    long maxScoreCount = query.getProfiledChildren().get(1).getTimeBreakdown().get("max_score_count");
                    long minScoreCount = query.getProfiledChildren().get(1).getTimeBreakdown().get("min_score_count");
                    long avgScoreCount = query.getProfiledChildren().get(1).getTimeBreakdown().get("avg_score_count");
                    assertThat(maxScore, greaterThanOrEqualTo(0L));
                    assertThat(minScore, greaterThanOrEqualTo(0L));
                    assertThat(avgScore, greaterThanOrEqualTo(0L));
                    assertThat(maxScore, greaterThanOrEqualTo(avgScore));
                    assertThat(avgScore, greaterThanOrEqualTo(minScore));
                    assertThat(maxScoreCount, greaterThanOrEqualTo(0L));
                    assertThat(minScoreCount, greaterThanOrEqualTo(0L));
                    assertThat(avgScoreCount, greaterThanOrEqualTo(0L));
                    assertThat(maxScoreCount, greaterThanOrEqualTo(avgScoreCount));
                    assertThat(avgScoreCount, greaterThanOrEqualTo(minScoreCount));
                }
            }, collector -> {
                assertThat(collector.getReason(), equalTo("search_terminate_after_count"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), hasSize(1));
                assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_top_hits"));
                assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
            });
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

        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
        assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0], instanceOf(FieldDoc.class));
        FieldDoc fieldDoc = (FieldDoc) context.queryResult().topDocs().topDocs.scoreDocs[0];
        assertThat(fieldDoc.fields[0], equalTo(1));
        // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
        // see: https://github.com/apache/lucene/pull/672
        assertProfileData(context, "ConstantScoreQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_top_hits"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });

        {
            context.parsedPostFilter(new ParsedQuery(new MinDocQuery(1)));
            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo(numDocs - 1L));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));
            assertProfileData(context, collector -> {
                assertThat(collector.getReason(), equalTo("search_post_filter"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), hasSize(1));
                assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_top_hits"));
                assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
            }, (query) -> {
                assertThat(query.getQueryName(), equalTo("MinDocQuery"));
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            }, (query) -> {
                // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
                // see: https://github.com/apache/lucene/pull/672
                assertThat(query.getQueryName(), equalTo("ConstantScoreQuery"));
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            });
            context.parsedPostFilter(null);
        }

        {
            context.setSearcher(newEarlyTerminationContextSearcher(reader, 1, executor));
            context.trackTotalHitsUpTo(SearchContext.TRACK_TOTAL_HITS_DISABLED);
            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));
            // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
            // see: https://github.com/apache/lucene/pull/672
            assertProfileData(context, "ConstantScoreQuery", query -> {
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            }, collector -> {
                assertThat(collector.getReason(), equalTo("search_top_hits"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), empty());
            });

            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().topDocs.scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));
            // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
            // see: https://github.com/apache/lucene/pull/672
            assertProfileData(context, "ConstantScoreQuery", query -> {
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            }, collector -> {
                assertThat(collector.getReason(), equalTo("search_top_hits"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), empty());
            });
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

            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.terminateAfter(), equalTo(0));
            assertThat(context.queryResult().getTotalHits().value, equalTo((long) numDocs));
            // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
            // see: https://github.com/apache/lucene/pull/672
            assertProfileData(context, "ConstantScoreQuery", query -> {
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            }, collector -> {
                assertThat(collector.getReason(), equalTo("search_top_hits"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), empty());
            });

            int sizeMinus1 = context.queryResult().topDocs().topDocs.scoreDocs.length - 1;
            FieldDoc lastDoc = (FieldDoc) context.queryResult().topDocs().topDocs.scoreDocs[sizeMinus1];

            context.setSearcher(newEarlyTerminationContextSearcher(reader, 10, executor));
            QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
            assertThat(context.terminateAfter(), equalTo(0));
            assertThat(context.queryResult().getTotalHits().value, equalTo((long) numDocs));
            assertProfileData(context, "ConstantScoreQuery", query -> {
                assertThat(query.getTimeBreakdown().keySet(), not(empty()));
                assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
                assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));

                assertThat(query.getProfiledChildren(), hasSize(1));
                assertThat(query.getProfiledChildren().get(0).getQueryName(), equalTo("SearchAfterSortedDocQuery"));
                assertThat(query.getProfiledChildren().get(0).getTime(), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("score"), equalTo(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("score_count"), equalTo(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight"), greaterThan(0L));
                assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            }, collector -> {
                assertThat(collector.getReason(), equalTo("search_top_hits"));
                assertThat(collector.getTime(), greaterThan(0L));
                if (collector.getName().contains("CollectorManager")) {
                    assertThat(collector.getReduceTime(), greaterThan(0L));
                }
                assertThat(collector.getMaxSliceTime(), greaterThan(0L));
                assertThat(collector.getMinSliceTime(), greaterThan(0L));
                assertThat(collector.getAvgSliceTime(), greaterThan(0L));
                assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
                assertThat(collector.getProfiledChildren(), empty());
            });
            FieldDoc firstDoc = (FieldDoc) context.queryResult().topDocs().topDocs.scoreDocs[0];
            for (int i = 0; i < searchSortAndFormat.sort.getSort().length; i++) {
                @SuppressWarnings("unchecked")
                FieldComparator<Object> comparator = (FieldComparator<Object>) searchSortAndFormat.sort.getSort()[i].getComparator(
                    i,
                    randomFrom(Pruning.values())
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
        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertEquals(numDocs / 2, context.queryResult().topDocs().topDocs.totalHits.value);
        assertEquals(context.queryResult().topDocs().topDocs.totalHits.relation, TotalHits.Relation.EQUAL_TO);
        assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(3));
        assertProfileData(context, "SpanNearQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), greaterThan(0L));
            if (executor != null) {
                assertThat(query.getTimeBreakdown().get("max_score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("min_score"), greaterThanOrEqualTo(0L));
                assertThat(query.getTimeBreakdown().get("avg_score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("max_score_count"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("min_score_count"), greaterThanOrEqualTo(0L));
                assertThat(query.getTimeBreakdown().get("avg_score_count"), greaterThan(0L));
            }
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_top_hits"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });

        context.sort(new SortAndFormats(new Sort(new SortField("other", SortField.Type.INT)), new DocValueFormat[] { DocValueFormat.RAW }));
        topDocsContext = TopDocsCollectorContext.createTopDocsCollectorContext(context, false);
        assertEquals(topDocsContext.create(null).scoreMode(), org.apache.lucene.search.ScoreMode.TOP_DOCS);
        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertEquals(numDocs / 2, context.queryResult().topDocs().topDocs.totalHits.value);
        assertThat(context.queryResult().topDocs().topDocs.scoreDocs.length, equalTo(3));
        assertEquals(context.queryResult().topDocs().topDocs.totalHits.relation, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        // IndexSearcher#rewrite optimizes by rewriting non-scoring queries to ConstantScoreQuery
        // see: https://github.com/apache/lucene/pull/672
        assertProfileData(context, "ConstantScoreQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_top_hits"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });

        reader.close();
        dir.close();
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

        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertEquals(10, context.queryResult().topDocs().topDocs.totalHits.value);
        assertProfileData(context, "BooleanQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(10L));
            if (executor != null) {
                assertThat(query.getTimeBreakdown().get("max_score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("min_score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("avg_score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("max_score_count"), equalTo(10L));
                assertThat(query.getTimeBreakdown().get("min_score_count"), equalTo(10L));
                assertThat(query.getTimeBreakdown().get("avg_score_count"), equalTo(10L));
            }
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));

            assertThat(query.getProfiledChildren(), hasSize(2));
            assertThat(query.getProfiledChildren().get(0).getQueryName(), equalTo("TermQuery"));
            assertThat(query.getProfiledChildren().get(0).getTime(), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight_count"), equalTo(1L));

            assertThat(query.getProfiledChildren().get(1).getQueryName(), equalTo("TermQuery"));
            assertThat(query.getProfiledChildren().get(1).getTime(), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_min_score"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), hasSize(1));
            assertThat(collector.getProfiledChildren().get(0).getReason(), equalTo("search_top_hits"));
            assertThat(collector.getProfiledChildren().get(0).getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getProfiledChildren().get(0).getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getProfiledChildren().get(0).getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getProfiledChildren().get(0).getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getProfiledChildren().get(0).getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getProfiledChildren().get(0).getSliceCount(), greaterThanOrEqualTo(1));
        });

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

        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertFalse(Float.isNaN(context.queryResult().getMaxScore()));
        assertEquals(1, context.queryResult().topDocs().topDocs.scoreDocs.length);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, greaterThanOrEqualTo(6L));
        assertProfileData(context, "BooleanQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), greaterThanOrEqualTo(6L));
            if (executor != null) {
                assertThat(query.getTimeBreakdown().get("max_score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("min_score"), greaterThanOrEqualTo(0L));
                assertThat(query.getTimeBreakdown().get("avg_score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("max_score_count"), greaterThanOrEqualTo(4L));
                assertThat(query.getTimeBreakdown().get("min_score_count"), greaterThanOrEqualTo(0L));
                assertThat(query.getTimeBreakdown().get("avg_score_count"), greaterThanOrEqualTo(1L));
            }
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));

            assertThat(query.getProfiledChildren(), hasSize(2));
            assertThat(query.getProfiledChildren().get(0).getQueryName(), equalTo("TermQuery"));
            assertThat(query.getProfiledChildren().get(0).getTime(), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight_count"), equalTo(1L));

            assertThat(query.getProfiledChildren().get(1).getQueryName(), equalTo("TermQuery"));
            assertThat(query.getProfiledChildren().get(1).getTime(), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_top_hits"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });

        context.sort(new SortAndFormats(sort, new DocValueFormat[] { DocValueFormat.RAW }));
        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertFalse(Float.isNaN(context.queryResult().getMaxScore()));
        assertEquals(1, context.queryResult().topDocs().topDocs.scoreDocs.length);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, greaterThanOrEqualTo(6L));
        assertProfileData(context, "BooleanQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), greaterThanOrEqualTo(6L));
            if (executor != null) {
                assertThat(query.getTimeBreakdown().get("max_score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("min_score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("avg_score"), greaterThan(0L));
                assertThat(query.getTimeBreakdown().get("max_score_count"), greaterThanOrEqualTo(6L));
                assertThat(query.getTimeBreakdown().get("min_score_count"), greaterThanOrEqualTo(0L));
                assertThat(query.getTimeBreakdown().get("avg_score_count"), greaterThanOrEqualTo(1L));
            }
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));

            assertThat(query.getProfiledChildren(), hasSize(2));
            assertThat(query.getProfiledChildren().get(0).getQueryName(), equalTo("TermQuery"));
            assertThat(query.getProfiledChildren().get(0).getTime(), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(0).getTimeBreakdown().get("create_weight_count"), equalTo(1L));

            assertThat(query.getProfiledChildren().get(1).getQueryName(), equalTo("TermQuery"));
            assertThat(query.getProfiledChildren().get(1).getTime(), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getProfiledChildren().get(1).getTimeBreakdown().get("create_weight_count"), equalTo(1L));
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_top_hits"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });

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

        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertFalse(Float.isNaN(context.queryResult().getMaxScore()));
        assertEquals(2, context.queryResult().topDocs().topDocs.scoreDocs.length);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().topDocs, instanceOf(CollapseTopFieldDocs.class));

        assertProfileData(context, "TermQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), greaterThanOrEqualTo(6L));
            if (executor != null) {
                long maxScore = query.getTimeBreakdown().get("max_score");
                long minScore = query.getTimeBreakdown().get("min_score");
                long avgScore = query.getTimeBreakdown().get("avg_score");
                long maxScoreCount = query.getTimeBreakdown().get("max_score_count");
                long minScoreCount = query.getTimeBreakdown().get("min_score_count");
                long avgScoreCount = query.getTimeBreakdown().get("avg_score_count");
                assertThat(maxScore, greaterThan(0L));
                assertThat(minScore, greaterThan(0L));
                assertThat(avgScore, greaterThan(0L));
                assertThat(maxScore, greaterThanOrEqualTo(avgScore));
                assertThat(avgScore, greaterThanOrEqualTo(minScore));
                assertThat(maxScoreCount, greaterThan(0L));
                assertThat(minScoreCount, greaterThan(0L));
                assertThat(avgScoreCount, greaterThan(0L));
                assertThat(maxScoreCount, greaterThanOrEqualTo(avgScoreCount));
                assertThat(avgScoreCount, greaterThanOrEqualTo(minScoreCount));
            }
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            assertThat(query.getProfiledChildren(), empty());
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_top_hits"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });

        context.sort(new SortAndFormats(sort, new DocValueFormat[] { DocValueFormat.RAW }));
        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertFalse(Float.isNaN(context.queryResult().getMaxScore()));
        assertEquals(2, context.queryResult().topDocs().topDocs.scoreDocs.length);
        assertThat(context.queryResult().topDocs().topDocs.totalHits.value, equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().topDocs, instanceOf(CollapseTopFieldDocs.class));

        assertProfileData(context, "TermQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), greaterThanOrEqualTo(6L));
            if (executor != null) {
                long maxScore = query.getTimeBreakdown().get("max_score");
                long minScore = query.getTimeBreakdown().get("min_score");
                long avgScore = query.getTimeBreakdown().get("avg_score");
                long maxScoreCount = query.getTimeBreakdown().get("max_score_count");
                long minScoreCount = query.getTimeBreakdown().get("min_score_count");
                long avgScoreCount = query.getTimeBreakdown().get("avg_score_count");
                assertThat(maxScore, greaterThan(0L));
                assertThat(minScore, greaterThan(0L));
                assertThat(avgScore, greaterThan(0L));
                assertThat(maxScore, greaterThanOrEqualTo(avgScore));
                assertThat(avgScore, greaterThanOrEqualTo(minScore));
                assertThat(maxScoreCount, greaterThan(0L));
                assertThat(minScoreCount, greaterThan(0L));
                assertThat(avgScoreCount, greaterThan(0L));
                assertThat(maxScoreCount, greaterThanOrEqualTo(avgScoreCount));
                assertThat(avgScoreCount, greaterThanOrEqualTo(minScoreCount));
            }
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            assertThat(query.getProfiledChildren(), empty());
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_top_hits"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });

        reader.close();
        dir.close();
    }

    public void testSourceFieldMatchQueryWithProfile() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        w.close();
        IndexReader reader = DirectoryReader.open(dir);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        DocumentMapper mockDocumentMapper = mock(DocumentMapper.class);
        SourceFieldMapper mockSourceMapper = mock(SourceFieldMapper.class);
        SearchLookup searchLookup = mock(SearchLookup.class);
        LeafSearchLookup leafSearchLookup = mock(LeafSearchLookup.class);

        when(queryShardContext.sourcePath("foo")).thenReturn(Set.of("bar"));
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        when(searchLookup.getLeafSearchLookup(any())).thenReturn(leafSearchLookup);
        when(leafSearchLookup.source()).thenReturn(new SourceLookup());
        when(mockSourceMapper.enabled()).thenReturn(true);
        when(mockDocumentMapper.sourceMapper()).thenReturn(mockSourceMapper);
        when(queryShardContext.documentMapper(any())).thenReturn(mockDocumentMapper);
        when(queryShardContext.lookup()).thenReturn(searchLookup);

        TestSearchContext context = new TestSearchContext(queryShardContext, indexShard, newContextSearcher(reader, executor));
        context.parsedQuery(
            new ParsedQuery(
                new SourceFieldMatchQuery(
                    new TermQuery(new Term("foo", "bar")),
                    new PhraseQuery("foo", "bar", "baz"),
                    new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
                        "user",
                        true,
                        true,
                        TextSearchInfo.WHITESPACE_MATCH_ONLY,
                        Collections.emptyMap()
                    ),
                    queryShardContext
                )
            )
        );

        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.setSize(1);
        context.trackTotalHitsUpTo(5);
        QueryPhase.executeInternal(context.withCleanQueryResult().withProfilers(), queryPhaseSearcher);
        assertProfileData(context, "SourceFieldMatchQuery", query -> {
            assertThat(query.getTimeBreakdown().keySet(), not(empty()));
            assertThat(query.getTimeBreakdown().get("score"), equalTo(0L));
            assertThat(query.getTimeBreakdown().get("score_count"), equalTo(0L));
            if (executor != null) {
                long maxScore = query.getTimeBreakdown().get("max_score");
                long minScore = query.getTimeBreakdown().get("min_score");
                long avgScore = query.getTimeBreakdown().get("avg_score");
                long maxScoreCount = query.getTimeBreakdown().get("max_score_count");
                long minScoreCount = query.getTimeBreakdown().get("min_score_count");
                long avgScoreCount = query.getTimeBreakdown().get("avg_score_count");
                assertThat(maxScore, equalTo(0L));
                assertThat(minScore, equalTo(0L));
                assertThat(avgScore, equalTo(0L));
                assertThat(maxScore, equalTo(avgScore));
                assertThat(avgScore, equalTo(minScore));
                assertThat(maxScoreCount, equalTo(0L));
                assertThat(minScoreCount, equalTo(0L));
                assertThat(avgScoreCount, equalTo(0L));
                assertThat(maxScoreCount, equalTo(avgScoreCount));
                assertThat(avgScoreCount, equalTo(minScoreCount));
            }
            assertThat(query.getTimeBreakdown().get("create_weight"), greaterThan(0L));
            assertThat(query.getTimeBreakdown().get("create_weight_count"), equalTo(1L));
            assertThat(query.getProfiledChildren(), empty());
        }, collector -> {
            assertThat(collector.getReason(), equalTo("search_top_hits"));
            assertThat(collector.getTime(), greaterThan(0L));
            if (collector.getName().contains("CollectorManager")) {
                assertThat(collector.getReduceTime(), greaterThan(0L));
            }
            assertThat(collector.getMaxSliceTime(), greaterThan(0L));
            assertThat(collector.getMinSliceTime(), greaterThan(0L));
            assertThat(collector.getAvgSliceTime(), greaterThan(0L));
            assertThat(collector.getSliceCount(), greaterThanOrEqualTo(1));
            assertThat(collector.getProfiledChildren(), empty());
        });
        reader.close();
        dir.close();
    }

    private void assertProfileData(SearchContext context, String type, Consumer<ProfileResult> query, Consumer<CollectorResult> collector)
        throws IOException {
        assertProfileData(context, collector, (profileResult) -> {
            assertThat(profileResult.getQueryName(), equalTo(type));
            assertThat(profileResult.getTime(), greaterThan(0L));
            query.accept(profileResult);
        });
    }

    private void assertProfileData(SearchContext context, Consumer<CollectorResult> collector, Consumer<ProfileResult> query1)
        throws IOException {
        assertProfileData(context, Arrays.asList(query1), collector, false);
    }

    private void assertProfileData(
        SearchContext context,
        Consumer<CollectorResult> collector,
        Consumer<ProfileResult> query1,
        Consumer<ProfileResult> query2
    ) throws IOException {
        assertProfileData(context, Arrays.asList(query1, query2), collector, false);
    }

    private final void assertProfileData(
        SearchContext context,
        List<Consumer<ProfileResult>> queries,
        Consumer<CollectorResult> collector,
        boolean debug
    ) throws IOException {
        assertThat(context.getProfilers(), not(nullValue()));

        final ProfileShardResult result = SearchProfileShardResults.buildShardResults(context.getProfilers(), null);
        if (debug) {
            final SearchProfileShardResults results = new SearchProfileShardResults(
                Collections.singletonMap(indexShard.shardId().toString(), result)
            );

            try (final XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint()) {
                builder.startObject();
                results.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
                builder.flush();

                final OutputStream out = builder.getOutputStream();
                assertThat(out, instanceOf(ByteArrayOutputStream.class));

                logger.info(new String(((ByteArrayOutputStream) out).toByteArray(), StandardCharsets.UTF_8));
            }
        }

        assertThat(result.getQueryProfileResults(), hasSize(1));

        final QueryProfileShardResult queryProfileShardResult = result.getQueryProfileResults().get(0);
        assertThat(queryProfileShardResult.getQueryResults(), hasSize(queries.size()));

        for (int i = 0; i < queries.size(); ++i) {
            queries.get(i).accept(queryProfileShardResult.getQueryResults().get(i));
        }

        collector.accept(queryProfileShardResult.getCollectorResult());
    }

    private static ContextIndexSearcher newContextSearcher(IndexReader reader, ExecutorService executor) throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(indexShard);
        SearchOperationListener searchOperationListener = new SearchOperationListener() {
        };
        when(indexShard.getSearchOperationListener()).thenReturn(searchOperationListener);
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
        SearchOperationListener searchOperationListener = new SearchOperationListener() {
        };
        when(indexShard.getSearchOperationListener()).thenReturn(searchOperationListener);
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

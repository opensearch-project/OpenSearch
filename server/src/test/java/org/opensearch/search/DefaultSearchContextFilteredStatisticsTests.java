/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.IndexCache;
import org.opensearch.index.cache.filteredstats.FilteredStatsCache;
import org.opensearch.index.cache.filteredstats.IndicesFilteredStatsCache;
import org.opensearch.index.cache.query.QueryCache;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * How {@link DefaultSearchContext} combines an alias filter with a caller-declared restriction to decide which
 * documents relevance statistics cover, and what it scores with when they cannot be scoped.
 */
public class DefaultSearchContextFilteredStatisticsTests extends OpenSearchTestCase {

    private static final Query ALIAS = new TermQuery(new Term("region", "x"));
    private static final Query RESTRICTION = new TermQuery(new Term("dept", "visible"));

    private ThreadPool threadPool;
    private Directory dir;
    private RandomIndexWriter writer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        dir = newDirectory();
        writer = new RandomIndexWriter(random(), dir);
        for (int i = 0; i < 6; i++) {
            Document doc = new Document();
            doc.add(new StringField("dept", i % 2 == 0 ? "visible" : "hidden", Field.Store.NO));
            doc.add(new StringField("region", i < 4 ? "x" : "y", Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();
    }

    @Override
    public void tearDown() throws Exception {
        writer.close();
        dir.close();
        terminate(threadPool);
        super.tearDown();
    }

    public void testNoAliasAndNoRestrictionScopesNothing() throws IOException {
        try (DefaultSearchContext context = context(null, -1)) {
            assertNull(context.filteredStatisticsFilter());
            assertFalse(context.useFilteredStatistics());
        }
    }

    public void testRestrictionAloneScopesStatistics() throws IOException {
        try (DefaultSearchContext context = context(null, -1)) {
            context.visibleSubsetFilter(RESTRICTION);
            assertEquals(RESTRICTION, context.filteredStatisticsFilter());
            assertTrue(context.useFilteredStatistics());
        }
    }

    public void testFilteredStatsAliasAloneScopesToTheAlias() throws IOException {
        try (DefaultSearchContext context = context(AliasFilter.Enforcement.FILTERED_STATS, -1)) {
            // The alias filter is built here, before any hook can declare a restriction, as in a real request.
            context.preProcess(false);
            assertEquals(ALIAS, context.filteredStatisticsFilter());
            assertTrue(context.useFilteredStatistics());
        }
    }

    public void testFilteredStatsAliasAndRestrictionScopeToBoth() throws IOException {
        try (DefaultSearchContext context = context(AliasFilter.Enforcement.FILTERED_STATS, -1)) {
            // The alias filter is built here, before any hook can declare a restriction, as in a real request.
            context.preProcess(false);
            context.visibleSubsetFilter(RESTRICTION);
            assertEquals(Set.of(ALIAS, RESTRICTION), filterClauses(context.filteredStatisticsFilter()));
            assertTrue(context.useFilteredStatistics());
        }
    }

    public void testPostFilterAliasLeavesTheRestrictionInCharge() throws IOException {
        try (DefaultSearchContext context = context(AliasFilter.Enforcement.POST_FILTER, -1)) {
            // The alias filter is built here, before any hook can declare a restriction, as in a real request.
            context.preProcess(false);
            assertNull("a post_filter alias alone does not scope statistics", context.filteredStatisticsFilter());
            assertFalse(context.useFilteredStatistics());

            context.visibleSubsetFilter(RESTRICTION);
            assertEquals(RESTRICTION, context.filteredStatisticsFilter());
            assertTrue(context.useFilteredStatistics());
        }
    }

    public void testPreFilterAliasSuppressesScoringSoNothingIsScoped() throws IOException {
        try (DefaultSearchContext context = context(AliasFilter.Enforcement.PRE_FILTER, -1)) {
            // The alias filter is built here, before any hook can declare a restriction, as in a real request.
            context.preProcess(false);
            context.visibleSubsetFilter(RESTRICTION);
            assertFalse(context.useFilteredStatistics());
        }
    }

    public void testRepeatedDeclarationsNarrowEachOther() throws IOException {
        Query second = new TermQuery(new Term("region", "y"));
        try (DefaultSearchContext context = context(null, -1)) {
            context.visibleSubsetFilter(RESTRICTION);
            context.visibleSubsetFilter(second);
            assertEquals(Set.of(RESTRICTION, second), filterClauses(context.filteredStatisticsFilter()));
        }
    }

    public void testWithinBudgetKeepsRelevanceScoring() throws IOException {
        try (DefaultSearchContext context = context(null, 100)) {
            context.visibleSubsetFilter(RESTRICTION);
            context.preProcess(false);
            assertTrue(context.useFilteredStatistics());
            assertThat(context.query(), not(instanceOf(ConstantScoreQuery.class)));
        }
    }

    public void testOverBudgetScoresWithConstantScore() throws IOException {
        try (DefaultSearchContext context = context(null, 0)) {
            context.visibleSubsetFilter(RESTRICTION);
            context.preProcess(false);
            assertFalse(context.useFilteredStatistics());
            assertThat(context.query(), instanceOf(ConstantScoreQuery.class));
        }
    }

    public void testOverBudgetDeclaredAfterTheQueryIsBuiltStillScoresWithConstantScore() throws IOException {
        try (DefaultSearchContext context = context(null, 0)) {
            context.preProcess(false);
            assertThat(context.query(), not(instanceOf(ConstantScoreQuery.class)));
            context.visibleSubsetFilter(RESTRICTION);
            assertThat(context.query(), instanceOf(ConstantScoreQuery.class));
        }
    }

    public void testFilteredStatsAliasOverBudgetScoresWithConstantScore() throws IOException {
        try (DefaultSearchContext context = context(AliasFilter.Enforcement.FILTERED_STATS, 0)) {
            context.preProcess(false);
            assertFalse(context.useFilteredStatistics());
            assertThat(context.query(), instanceOf(ConstantScoreQuery.class));
        }
    }

    public void testCacheComesFromTheIndex() throws IOException {
        try (DefaultSearchContext context = context(null, -1)) {
            assertSame(cache, context.filteredStatsCache());
        }
    }

    private FilteredStatsCache cache;

    private static Set<Query> filterClauses(Query query) {
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bool = (BooleanQuery) query;
        for (BooleanClause clause : bool.clauses()) {
            assertEquals("every clause must be non-scoring", BooleanClause.Occur.FILTER, clause.occur());
        }
        return bool.clauses().stream().map(BooleanClause::query).collect(Collectors.toSet());
    }

    /**
     * A context over the test index, with an alias of the given enforcement (none when null) and the given
     * visible-document ceiling (unlimited when negative).
     */
    private DefaultSearchContext context(AliasFilter.Enforcement enforcement, long maxVisibleDocs) throws IOException {
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 0);
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.searchType()).thenReturn(SearchType.DEFAULT);
        when(request.shardId()).thenReturn(shardId);
        if (enforcement == null) {
            when(request.getAliasFilter()).thenReturn(AliasFilter.EMPTY);
        } else {
            QueryBuilder aliasBuilder = mock(QueryBuilder.class);
            when(aliasBuilder.toQuery(any())).thenReturn(ALIAS);
            when(request.getAliasFilter()).thenReturn(new AliasFilter(aliasBuilder, enforcement, "alias"));
        }

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(FilteredStatsCache.INDEX_FILTERED_STATS_MAX_VISIBLE_DOCS_SETTING.getKey(), maxVisibleDocs)
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), Settings.EMPTY);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(mock(QueryCachingPolicy.class));
        when(indexShard.getThreadPool()).thenReturn(threadPool);
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        IndexService indexService = mock(IndexService.class);
        IndexCache indexCache = mock(IndexCache.class);
        when(indexCache.query()).thenReturn(mock(QueryCache.class));
        cache = new FilteredStatsCache(
            indexSettings,
            new IndicesFilteredStatsCache(Settings.EMPTY, new NoopCircuitBreaker(CircuitBreaker.FIELDDATA))
        );
        when(indexCache.filteredStatsCache()).thenReturn(cache);
        when(indexService.cache()).thenReturn(indexCache);
        when(indexService.newQueryShardContext(eq(shardId.id()), any(), any(), nullable(String.class), anyBoolean(), anyBoolean()))
            .thenReturn(mock(QueryShardContext.class));
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.hasNested()).thenReturn(false);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        when(indexService.mapperService()).thenReturn(mapperService);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);

        IndexReader reader = writer.getReader();
        Engine.SearcherSupplier searcherSupplier = new Engine.SearcherSupplier(Function.identity()) {
            @Override
            protected void doClose() {}

            @Override
            protected Engine.Searcher acquireSearcherInternal(String source) {
                return new Engine.Searcher(
                    "test",
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    reader
                );
            }
        };
        ReaderContext readerContext = new ReaderContext(
            new ShardSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong()),
            indexService,
            indexShard,
            searcherSupplier,
            randomNonNegativeLong(),
            false
        );
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        DefaultSearchContext context = new DefaultSearchContext(
            readerContext,
            request,
            new SearchShardTarget("node", shardId, null, OriginalIndices.NONE),
            null,
            bigArrays,
            null,
            TimeValue.timeValueMinutes(1),
            null,
            false,
            Version.CURRENT,
            false,
            null,
            null,
            Collections.emptyList()
        );
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        return context;
    }
}

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

package org.opensearch.search;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.PitSearchContextIdForNode;
import org.opensearch.action.search.SearchContextIdForNode;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.search.UpdatePitContextRequest;
import org.opensearch.action.search.UpdatePitContextResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Numbers;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.DerivedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.settings.InternalOrPrivateSettingsPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.ShardFetchRequest;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.sort.FieldStats;
import org.opensearch.search.sort.MinAndMax;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchServiceTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(
            FailOnRewriteQueryPlugin.class,
            CustomScriptPlugin.class,
            ReaderWrapperCountPlugin.class,
            InternalOrPrivateSettingsPlugin.class,
            MockSearchService.TestPlugin.class
        );
    }

    public static class ReaderWrapperCountPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.setReaderWrapper(service -> SearchServiceTests::apply);
        }
    }

    @Before
    private void resetCount() {
        numWrapInvocations = new AtomicInteger(0);
    }

    private static AtomicInteger numWrapInvocations = new AtomicInteger(0);

    private static DirectoryReader apply(DirectoryReader directoryReader) throws IOException {
        numWrapInvocations.incrementAndGet();
        return new FilterDirectoryReader(directoryReader, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return reader;
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
                return in;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return directoryReader.getReaderCacheHelper();
            }
        };
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        static final String DUMMY_SCRIPT = "dummyScript";

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(DUMMY_SCRIPT, vars -> "dummy");
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onFetchPhase(SearchContext context, long tookInNanos) {
                    if ("throttled_threadpool_index".equals(context.indexShard().shardId().getIndex().getName())) {
                        assertThat(Thread.currentThread().getName(), startsWith("opensearch[node_s_0][search_throttled]"));
                    } else {
                        assertThat(Thread.currentThread().getName(), startsWith("opensearch[node_s_0][search]"));
                    }
                }

                @Override
                public void onQueryPhase(SearchContext context, long tookInNanos) {
                    if ("throttled_threadpool_index".equals(context.indexShard().shardId().getIndex().getName())) {
                        assertThat(Thread.currentThread().getName(), startsWith("opensearch[node_s_0][search_throttled]"));
                    } else {
                        assertThat(Thread.currentThread().getName(), startsWith("opensearch[node_s_0][search]"));
                    }
                }
            });
        }
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put("search.default_search_timeout", "5s").build();
    }

    public void testClearOnClose() {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearOnStop() {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doStop();
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearIndexDelete() {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        assertAcked(client().admin().indices().prepareDelete("index"));
        assertEquals(0, service.getActiveContexts());
    }

    public void testCloseSearchContextOnRewriteException() {
        // if refresh happens while checking the exception, the subsequent reference count might not match, so we switch it off
        createIndex("index", Settings.builder().put("index.refresh_interval", -1).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);

        final int activeContexts = service.getActiveContexts();
        final int activeRefs = indexShard.store().refCount();
        expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch("index").setQuery(new FailOnRewriteQueryBuilder()).get()
        );
        assertEquals(activeContexts, service.getActiveContexts());
        assertEquals(activeRefs, indexShard.store().refCount());
    }

    public void testSearchWhileIndexDeleted() throws InterruptedException {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startGun = new CountDownLatch(1);
        Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);

        final Thread thread = new Thread() {
            @Override
            public void run() {
                startGun.countDown();
                while (running.get()) {
                    service.afterIndexRemoved(indexService.index(), indexService.getIndexSettings(), DELETED);
                    if (randomBoolean()) {
                        // here we trigger some refreshes to ensure the IR go out of scope such that we hit ACE if we access a search
                        // context in a non-sane way.
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        client().prepareIndex("index")
                            .setSource("field", "value")
                            .setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()))
                            .execute(new ActionListener<IndexResponse>() {
                                @Override
                                public void onResponse(IndexResponse indexResponse) {
                                    semaphore.release();
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    semaphore.release();
                                }
                            });
                    }
                }
            }
        };
        thread.start();
        startGun.await();
        try {
            final int rounds = scaledRandomIntBetween(100, 1000);
            SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
            SearchRequest scrollSearchRequest = new SearchRequest().allowPartialSearchResults(true)
                .scroll(new Scroll(TimeValue.timeValueMinutes(1)));
            for (int i = 0; i < rounds; i++) {
                try {
                    try {
                        PlainActionFuture<SearchPhaseResult> result = new PlainActionFuture<>();
                        final boolean useScroll = randomBoolean();
                        service.executeQueryPhase(
                            new ShardSearchRequest(
                                OriginalIndices.NONE,
                                useScroll ? scrollSearchRequest : searchRequest,
                                indexShard.shardId(),
                                1,
                                new AliasFilter(null, Strings.EMPTY_ARRAY),
                                1.0f,
                                -1,
                                null,
                                null
                            ),
                            true,
                            new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()),
                            result
                        );
                        SearchPhaseResult searchPhaseResult = result.get();
                        List<Integer> intCursors = new ArrayList(1);
                        intCursors.add(0);
                        ShardFetchRequest req = new ShardFetchRequest(searchPhaseResult.getContextId(), intCursors, null/* not a scroll */);
                        PlainActionFuture<FetchSearchResult> listener = new PlainActionFuture<>();
                        service.executeFetchPhase(req, new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()), listener);
                        listener.get();
                        if (useScroll) {
                            // have to free context since this test does not remove the index from IndicesService.
                            service.freeReaderContext(searchPhaseResult.getContextId());
                        }
                    } catch (ExecutionException ex) {
                        assertThat(ex.getCause(), instanceOf(RuntimeException.class));
                        throw ((RuntimeException) ex.getCause());
                    }
                } catch (AlreadyClosedException ex) {
                    throw ex;
                } catch (IllegalStateException ex) {
                    assertEquals("reader_context is already closed can't increment refCount current count [0]", ex.getMessage());
                } catch (SearchContextMissingException ex) {
                    // that's fine
                }
            }
        } finally {
            running.set(false);
            thread.join();
            semaphore.acquire(Integer.MAX_VALUE);
        }

        assertEquals(0, service.getActiveContexts());

        SearchStats.Stats totalStats = indexShard.searchStats().getTotal();
        assertEquals(0, totalStats.getQueryCurrent());
        assertEquals(0, totalStats.getScrollCurrent());
        assertEquals(0, totalStats.getFetchCurrent());
    }

    public void testSearchWhileIndexDeletedDoesNotLeakSearchContext() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);

        MockSearchService service = (MockSearchService) getInstanceFromNode(SearchService.class);
        service.setOnPutContext(context -> {
            if (context.indexShard() == indexShard) {
                assertAcked(client().admin().indices().prepareDelete("index"));
            }
        });

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchRequest scrollSearchRequest = new SearchRequest().allowPartialSearchResults(true)
            .scroll(new Scroll(TimeValue.timeValueMinutes(1)));

        // the scrolls are not explicitly freed, but should all be gone when the test finished.
        // for completeness, we also randomly test the regular search path.
        final boolean useScroll = randomBoolean();
        PlainActionFuture<SearchPhaseResult> result = new PlainActionFuture<>();
        service.executeQueryPhase(
            new ShardSearchRequest(
                OriginalIndices.NONE,
                useScroll ? scrollSearchRequest : searchRequest,
                new ShardId(resolveIndex("index"), 0),
                1,
                new AliasFilter(null, Strings.EMPTY_ARRAY),
                1.0f,
                -1,
                null,
                null
            ),
            randomBoolean(),
            new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()),
            result
        );

        try {
            result.get();
        } catch (Exception e) {
            // ok
        }

        expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareGetIndex().setIndices("index").get());

        assertEquals(0, service.getActiveContexts());

        SearchStats.Stats totalStats = indexShard.searchStats().getTotal();
        assertEquals(0, totalStats.getQueryCurrent());
        assertEquals(0, totalStats.getScrollCurrent());
        assertEquals(0, totalStats.getFetchCurrent());
    }

    public void testTimeout() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final ShardSearchRequest requestWithDefaultTimeout = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );

        try (
            ReaderContext reader = createReaderContext(indexService, indexShard);
            SearchContext contextWithDefaultTimeout = service.createContext(reader, requestWithDefaultTimeout, null, randomBoolean())
        ) {
            // the search context should inherit the default timeout
            assertThat(contextWithDefaultTimeout.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        }

        final long seconds = randomIntBetween(6, 10);
        searchRequest.source(new SearchSourceBuilder().timeout(TimeValue.timeValueSeconds(seconds)));
        final ShardSearchRequest requestWithCustomTimeout = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );
        try (
            ReaderContext reader = createReaderContext(indexService, indexShard);
            SearchContext context = service.createContext(reader, requestWithCustomTimeout, null, randomBoolean())
        ) {
            // the search context should inherit the query timeout
            assertThat(context.timeout(), equalTo(TimeValue.timeValueSeconds(seconds)));
        }
    }

    /**
     * test that getting more than the allowed number of docvalue_fields throws an exception
     */
    public void testMaxDocvalueFieldsSearch() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        // adding the maximum allowed number of docvalue_fields to retrieve
        for (int i = 0; i < indexService.getIndexSettings().getMaxDocvalueFields(); i++) {
            searchSourceBuilder.docValueField("field" + i);
        }
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );
        try (
            ReaderContext reader = createReaderContext(indexService, indexShard);
            SearchContext context = service.createContext(reader, request, null, randomBoolean())
        ) {
            assertNotNull(context);
        }
        searchSourceBuilder.docValueField("one_field_too_much");
        try (ReaderContext reader = createReaderContext(indexService, indexShard)) {
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> service.createContext(reader, request, null, randomBoolean())
            );
            assertEquals(
                "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [100] but was [101]. "
                    + "This limit can be set by changing the [index.max_docvalue_fields_search] index level setting.",
                ex.getMessage()
            );
        }
    }

    public void testDerivedFieldsSearch() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);

        for (int i = 0; i < 5; i++) {
            searchSourceBuilder.derivedField(
                "field" + i,
                "date",
                new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap())
            );
        }
        indexService.getIndexSettings().isDerivedFieldAllowed();
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );

        try (ReaderContext reader = createReaderContext(indexService, indexShard)) {
            try (SearchContext context = service.createContext(reader, request, null, randomBoolean())) {
                assertNotNull(context);
                for (int i = 0; i < 5; i++) {
                    DerivedFieldType derivedFieldType = (DerivedFieldType) context.getQueryShardContext()
                        .resolveDerivedFieldType("field" + i);
                    assertEquals("field" + i, derivedFieldType.name());
                    assertEquals("date", derivedFieldType.getType());
                }
                assertNull(context.getQueryShardContext().resolveDerivedFieldType("field" + 5));
            }
        }
    }

    public void testDerivedFieldDisabled() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);

        searchSourceBuilder.derivedField(
            "field",
            "date",
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap())
        );
        indexService.getIndexSettings().isDerivedFieldAllowed();
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );

        try (ReaderContext reader = createReaderContext(indexService, indexShard)) {
            SearchContext context = service.createContext(reader, request, null, randomBoolean());

            // nothing disabled, derived field resolved fine
            assertNotNull(context.getQueryShardContext().resolveDerivedFieldType("field"));

            // disabled using cluster setting, assert create context fails
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(SearchService.CLUSTER_ALLOW_DERIVED_FIELD_SETTING.getKey(), false))
                .get();
            assertThrows(OpenSearchException.class, () -> service.createContext(reader, request, null, randomBoolean()));

            // dynamically enabled using cluster setting, assert derived field resolved fine
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(SearchService.CLUSTER_ALLOW_DERIVED_FIELD_SETTING.getKey(), true))
                .get();
            context = service.createContext(reader, request, null, randomBoolean());
            assertNotNull(context.getQueryShardContext().resolveDerivedFieldType("field"));

            // disabled using index setting, assert create context fails
            client().admin()
                .indices()
                .prepareUpdateSettings("index")
                .setSettings(Settings.builder().put(IndexSettings.ALLOW_DERIVED_FIELDS.getKey(), false))
                .get();

            assertThrows(OpenSearchException.class, () -> service.createContext(reader, request, null, randomBoolean()));

            // dynamically enabled using index setting, assert derived field resolved fine
            client().admin()
                .indices()
                .prepareUpdateSettings("index")
                .setSettings(Settings.builder().put(IndexSettings.ALLOW_DERIVED_FIELDS.getKey(), true))
                .get();

            context = service.createContext(reader, request, null, randomBoolean());
            assertNotNull(context.getQueryShardContext().resolveDerivedFieldType("field"));

            // Cleanup
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(SearchService.CLUSTER_ALLOW_DERIVED_FIELD_SETTING.getKey()))
                .get();
        }
    }

    /**
     * test that getting more than the allowed number of script_fields throws an exception
     */
    public void testMaxScriptFieldsSearch() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        // adding the maximum allowed number of script_fields to retrieve
        int maxScriptFields = indexService.getIndexSettings().getMaxScriptFields();
        for (int i = 0; i < maxScriptFields; i++) {
            searchSourceBuilder.scriptField(
                "field" + i,
                new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap())
            );
        }
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );

        try (ReaderContext reader = createReaderContext(indexService, indexShard)) {
            try (SearchContext context = service.createContext(reader, request, null, randomBoolean())) {
                assertNotNull(context);
            }
            searchSourceBuilder.scriptField(
                "anotherScriptField",
                new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap())
            );
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> service.createContext(reader, request, null, randomBoolean())
            );
            assertEquals(
                "Trying to retrieve too many script_fields. Must be less than or equal to: ["
                    + maxScriptFields
                    + "] but was ["
                    + (maxScriptFields + 1)
                    + "]. This limit can be set by changing the [index.max_script_fields] index level setting.",
                ex.getMessage()
            );
        }
    }

    public void testIgnoreScriptfieldIfSizeZero() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.scriptField(
            "field" + 0,
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap())
        );
        searchSourceBuilder.size(0);
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );
        try (
            ReaderContext reader = createReaderContext(indexService, indexShard);
            SearchContext context = service.createContext(reader, request, null, randomBoolean())
        ) {
            assertEquals(0, context.scriptFields().fields().size());
        }
    }

    /**
     * test that creating more than the allowed number of scroll contexts throws an exception
     */
    public void testMaxOpenScrollContexts() throws Exception {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        // Open all possible scrolls, clear some of them, then open more until the limit is reached
        LinkedList<String> clearScrollIds = new LinkedList<>();

        for (int i = 0; i < SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY); i++) {
            SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();

            if (randomInt(4) == 0) clearScrollIds.addLast(searchResponse.getScrollId());
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.setScrollIds(clearScrollIds);
        client().clearScroll(clearScrollRequest);

        for (int i = 0; i < clearScrollIds.size(); i++) {
            client().prepareSearch("index").setSize(1).setScroll("1m").get();
        }

        final ShardScrollRequestTest request = new ShardScrollRequestTest(indexShard.shardId());
        OpenSearchRejectedExecutionException ex = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> service.createAndPutReaderContext(
                request,
                indexService,
                indexShard,
                indexShard.acquireSearcherSupplier(),
                randomBoolean()
            )
        );
        assertEquals(
            "Trying to create too many scroll contexts. Must be less than or equal to: ["
                + SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY)
                + "]. "
                + "This limit can be set by changing the [search.max_open_scroll_context] setting.",
            ex.getMessage()
        );

        service.freeAllScrollContexts();
    }

    public void testOpenScrollContextsConcurrently() throws Exception {
        createIndex("index");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        final int maxScrollContexts = SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY);
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        Thread[] threads = new Thread[randomIntBetween(2, 8)];
        CountDownLatch latch = new CountDownLatch(threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                    for (;;) {
                        final Engine.SearcherSupplier reader = indexShard.acquireSearcherSupplier();
                        try {
                            searchService.createAndPutReaderContext(
                                new ShardScrollRequestTest(indexShard.shardId()),
                                indexService,
                                indexShard,
                                reader,
                                true
                            );
                        } catch (OpenSearchRejectedExecutionException e) {
                            assertThat(
                                e.getMessage(),
                                equalTo(
                                    "Trying to create too many scroll contexts. Must be less than or equal to: "
                                        + "["
                                        + maxScrollContexts
                                        + "]. "
                                        + "This limit can be set by changing the [search.max_open_scroll_context] setting."
                                )
                            );
                            return;
                        }
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            threads[i].setName("opensearch[node_s_0][search]");
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(searchService.getActiveContexts(), equalTo(maxScrollContexts));
        searchService.freeAllScrollContexts();
    }

    public static class FailOnRewriteQueryPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QuerySpec<?>> getQueries() {
            return singletonList(new QuerySpec<>("fail_on_rewrite_query", FailOnRewriteQueryBuilder::new, parseContext -> {
                throw new UnsupportedOperationException("No query parser for this plugin");
            }));
        }
    }

    public static class FailOnRewriteQueryBuilder extends AbstractQueryBuilder<FailOnRewriteQueryBuilder> {

        public FailOnRewriteQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        public FailOnRewriteQueryBuilder() {}

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
            if (queryRewriteContext.convertToShardContext() != null) {
                throw new IllegalStateException("Fail on rewrite phase");
            }
            return this;
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected void doXContent(XContentBuilder builder, Params params) {}

        @Override
        protected Query doToQuery(QueryShardContext context) {
            return null;
        }

        @Override
        protected boolean doEquals(FailOnRewriteQueryBuilder other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return null;
        }
    }

    private static class ShardScrollRequestTest extends ShardSearchRequest {
        private Scroll scroll;

        ShardScrollRequestTest(ShardId shardId) {
            super(
                OriginalIndices.NONE,
                new SearchRequest().allowPartialSearchResults(true),
                shardId,
                1,
                new AliasFilter(null, Strings.EMPTY_ARRAY),
                1f,
                -1,
                null,
                null
            );
            this.scroll = new Scroll(TimeValue.timeValueMinutes(1));
        }

        @Override
        public Scroll scroll() {
            return this.scroll;
        }
    }

    public void testCanMatch() throws Exception {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        assertTrue(
            service.canMatch(
                new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    1,
                    new AliasFilter(null, Strings.EMPTY_ARRAY),
                    1f,
                    -1,
                    null,
                    null
                )
            ).canMatch()
        );

        searchRequest.source(new SearchSourceBuilder());
        assertTrue(
            service.canMatch(
                new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    1,
                    new AliasFilter(null, Strings.EMPTY_ARRAY),
                    1f,
                    -1,
                    null,
                    null
                )
            ).canMatch()
        );

        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        assertTrue(
            service.canMatch(
                new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    1,
                    new AliasFilter(null, Strings.EMPTY_ARRAY),
                    1f,
                    -1,
                    null,
                    null
                )
            ).canMatch()
        );

        searchRequest.source(
            new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
                .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(0))
        );
        assertTrue(
            service.canMatch(
                new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    1,
                    new AliasFilter(null, Strings.EMPTY_ARRAY),
                    1f,
                    -1,
                    null,
                    null
                )
            ).canMatch()
        );
        searchRequest.source(
            new SearchSourceBuilder().query(new MatchNoneQueryBuilder()).aggregation(new GlobalAggregationBuilder("test"))
        );
        assertTrue(
            service.canMatch(
                new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    1,
                    new AliasFilter(null, Strings.EMPTY_ARRAY),
                    1f,
                    -1,
                    null,
                    null
                )
            ).canMatch()
        );

        searchRequest.source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder()));
        assertFalse(
            service.canMatch(
                new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    1,
                    new AliasFilter(null, Strings.EMPTY_ARRAY),
                    1f,
                    -1,
                    null,
                    null
                )
            ).canMatch()
        );
        assertEquals(0, numWrapInvocations.get());

        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );

        /*
         * Checks that canMatch takes into account the alias filter
         */
        // the source cannot be rewritten to a match_none
        searchRequest.indices("alias").source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        assertFalse(
            service.canMatch(
                new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    1,
                    new AliasFilter(new TermQueryBuilder("foo", "bar"), "alias"),
                    1f,
                    -1,
                    null,
                    null
                )
            ).canMatch()
        );
        // the source can match and can be rewritten to a match_none, but not the alias filter
        final IndexResponse response = client().prepareIndex("index").setId("1").setSource("id", "1").get();
        assertEquals(RestStatus.CREATED, response.status());
        searchRequest.indices("alias").source(new SearchSourceBuilder().query(new TermQueryBuilder("id", "1")));
        assertFalse(
            service.canMatch(
                new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    1,
                    new AliasFilter(new TermQueryBuilder("foo", "bar"), "alias"),
                    1f,
                    -1,
                    null,
                    null
                )
            ).canMatch()
        );

        CountDownLatch latch = new CountDownLatch(1);
        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());
        service.executeQueryPhase(request, randomBoolean(), task, new ActionListener<SearchPhaseResult>() {
            @Override
            public void onResponse(SearchPhaseResult searchPhaseResult) {
                try {
                    // make sure that the wrapper is called when the query is actually executed
                    assertEquals(1, numWrapInvocations.get());
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    throw new AssertionError(e);
                } finally {
                    latch.countDown();
                }
            }
        });
        latch.await();
    }

    public void testCanRewriteToMatchNone() {
        assertFalse(
            SearchService.canRewriteToMatchNone(
                new SearchSourceBuilder().query(new MatchNoneQueryBuilder()).aggregation(new GlobalAggregationBuilder("test"))
            )
        );
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder()));
        assertFalse(SearchService.canRewriteToMatchNone(null));
        assertFalse(
            SearchService.canRewriteToMatchNone(
                new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
                    .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(0))
            )
        );
        assertTrue(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new TermQueryBuilder("foo", "bar"))));
        assertTrue(
            SearchService.canRewriteToMatchNone(
                new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
                    .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(1))
            )
        );
        assertFalse(
            SearchService.canRewriteToMatchNone(
                new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
                    .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(1))
                    .suggest(new SuggestBuilder())
            )
        );
        assertFalse(
            SearchService.canRewriteToMatchNone(
                new SearchSourceBuilder().query(new TermQueryBuilder("foo", "bar")).suggest(new SuggestBuilder())
            )
        );
    }

    private Number randomNumber(NumberFieldMapper.NumberType type) {
        return switch (type) {
            case BYTE -> randomByte();
            case SHORT -> randomShort();
            case INTEGER -> randomInt(10000);
            case LONG -> randomLongBetween(0L, 10000L);
            case DOUBLE -> randomDoubleBetween(0, 10000, false);
            case FLOAT, HALF_FLOAT -> (float) randomDoubleBetween(0, 10000, false);
            case UNSIGNED_LONG -> {
                BigInteger ul = randomUnsignedLong();
                while (ul.compareTo(Numbers.MIN_UNSIGNED_LONG_VALUE) == 0 || ul.compareTo(Numbers.MAX_UNSIGNED_LONG_VALUE) == 0) {
                    ul = randomUnsignedLong();
                }
                yield ul;
            }
        };
    }

    private Number incOrDecNumber(Number number, NumberFieldMapper.NumberType type, boolean inc) {
        return switch (type) {
            case BYTE, SHORT, INTEGER -> number.intValue() + (inc ? 1 : -1);
            case LONG -> number.longValue() + (inc ? 1 : -1);
            case DOUBLE -> number.doubleValue() + (inc ? 1 : -1);
            case FLOAT, HALF_FLOAT -> number.floatValue() + (inc ? 1 : -1);
            case UNSIGNED_LONG -> ((BigInteger) number).add(inc ? BigInteger.valueOf(1) : BigInteger.valueOf(-1));
        };
    }

    private void canMatchSearchAfterForOneNumericType(NumberFieldMapper.NumberType type) throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("num1")
            .field("type", type.typeName())
            .endObject()
            .startObject("num2")
            .field("type", type.typeName())
            .endObject()
            .endObject()
            .endObject();
        IndexService indexService = createIndex(
            "test",
            Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build(),
            MapperService.SINGLE_MAPPING_NAME,
            mapping
        );
        ensureGreen();

        final boolean allDocsHaveValue = randomBoolean();
        final BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        final int numDocs = randomIntBetween(10, 20);
        final Number[] nums1 = new Number[allDocsHaveValue ? numDocs : numDocs - 1];
        final Number[] nums2 = new Number[numDocs];
        for (int i = 0; i < numDocs; i++) {
            String source;
            if (i < numDocs - 1 || allDocsHaveValue) {
                nums1[i] = randomNumber(type);
                nums2[i] = randomNumber(type);
                source = String.format(Locale.ROOT, "{\"num1\": %s, \"num2\": %s}", nums1[i].toString(), nums2[i].toString());
            } else {
                nums2[i] = randomNumber(type);
                source = String.format(Locale.ROOT, "{\"num2\": %s}", nums2[i].toString());
            }
            bulkRequestBuilder.add(client().prepareIndex("test").setId(String.valueOf(i)).setSource(source, MediaTypeRegistry.JSON));
        }
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        final IndexShard shard = indexService.getShard(0);
        final ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(true),
            shard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1f,
            -1,
            null,
            null
        );
        shardRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).trackTotalHits(false));
        assertFalse(shard.hasRefreshPending());

        for (boolean singleSort : new boolean[] { true, false }) {
            for (boolean searchAfterOutOfRange : new boolean[] { true, false }) {
                for (boolean searchAfterEqualsToBoundary : new boolean[] { true, false }) {
                    for (boolean missingValueOutOfRange : new boolean[] { true, false }) {
                        for (boolean missingValueEqualsToSearchAfter : new boolean[] { true, false }) {
                            runNumericCanMatchTest(
                                type,
                                shardRequest,
                                nums1,
                                nums2,
                                allDocsHaveValue,
                                singleSort,
                                searchAfterOutOfRange,
                                searchAfterEqualsToBoundary,
                                missingValueOutOfRange,
                                missingValueEqualsToSearchAfter
                            );
                        }
                    }
                }
            }
        }
        client().admin().indices().prepareDelete("test").get();
        ensureGreen();
    }

    private void runNumericCanMatchTest(
        NumberFieldMapper.NumberType type,
        ShardSearchRequest shardRequest,
        Number[] nums1,
        Number[] nums2,
        boolean allDocsHaveValue,
        boolean singleSort,
        boolean searchAfterOutOfRange,
        boolean searchAfterEqualsToBoundary,
        boolean missingValueOutOfRange,
        boolean missingValueEqualsToSearchAfter
    ) throws IOException {
        Number searchAfter;
        Object missingValue;
        Arrays.sort(nums1);
        Arrays.sort(nums2);
        final boolean reverse = randomBoolean();
        if (searchAfterOutOfRange) {
            searchAfter = reverse ? incOrDecNumber(nums1[0], type, false) : incOrDecNumber(nums1[nums1.length - 1], type, true);
        } else {
            if (searchAfterEqualsToBoundary) {
                searchAfter = reverse ? nums1[0] : nums1[nums1.length - 1];
            } else {
                int from = 1, to = nums1.length - 1;
                while (nums1[from].equals(nums1[from - 1]))
                    from++;
                while (nums1[to - 1].equals(nums1[to]))
                    to--;
                assert from < to;
                searchAfter = randomFrom(Arrays.asList(nums1).subList(from, to));
            }
        }
        if (missingValueOutOfRange) {
            missingValue = reverse
                ? (randomBoolean() ? "_first" : incOrDecNumber(searchAfter, type, true))
                : (randomBoolean() ? "_first" : incOrDecNumber(searchAfter, type, false));
        } else {
            missingValue = missingValueEqualsToSearchAfter ? searchAfter
                : reverse ? (randomBoolean() ? "_last" : incOrDecNumber(searchAfter, type, false))
                : (randomBoolean() ? "_last" : incOrDecNumber(searchAfter, type, true));
        }
        shardRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).trackTotalHits(false));
        shardRequest.source().sort(SortBuilders.fieldSort("num1").missing(missingValue).order(reverse ? SortOrder.DESC : SortOrder.ASC));
        List<Object> searchAfterFields = new ArrayList<>();
        searchAfterFields.add(searchAfter);
        if (singleSort == false) {
            shardRequest.source().sort("num2");
            searchAfterFields.add(randomFrom(nums2));
        }
        shardRequest.source().searchAfter(searchAfterFields.toArray());
        final SearchService service = getInstanceFromNode(SearchService.class);
        SearchService.CanMatchResponse response = service.canMatch(shardRequest);

        if (type == NumberFieldMapper.NumberType.HALF_FLOAT || type == NumberFieldMapper.NumberType.UNSIGNED_LONG) {
            assertTrue(response.canMatch());
            return;
        }

        final boolean nonMatch;
        if (allDocsHaveValue) {
            nonMatch = searchAfterOutOfRange || (singleSort && searchAfterEqualsToBoundary);
        } else {
            if (searchAfterOutOfRange) {
                nonMatch = missingValueOutOfRange || (singleSort && missingValueEqualsToSearchAfter);
            } else {
                nonMatch = singleSort && searchAfterEqualsToBoundary && (missingValueOutOfRange || missingValueEqualsToSearchAfter);
            }
        }

        boolean canMatch = response.canMatch();
        assertEquals(nonMatch == false, canMatch);
    }

    public void testNumericCanMatch() throws Exception {
        for (var type : NumberFieldMapper.NumberType.values()) {
            canMatchSearchAfterForOneNumericType(type);
        }
    }

    private void runStringCanMatchTest(
        ShardSearchRequest shardRequest,
        BytesRef[] values,
        boolean allDocsHaveValue,
        boolean singleSort,
        boolean searchAfterOutOfRange,
        boolean searchAfterEqualsToBoundary,
        boolean customMissingValue,
        boolean sortMissingLast
    ) throws IOException {
        Object searchAfter;
        Object missingValue;
        Arrays.sort(values, BytesRef::compareTo);
        final String minValue = values[0].utf8ToString();
        final String maxValue = values[values.length - 1].utf8ToString();
        final boolean reverse = randomBoolean();
        if (searchAfterOutOfRange) {
            searchAfter = reverse ? minValue.substring(0, minValue.length() - 1) : maxValue + randomAlphaOfLength(1);
        } else {
            if (searchAfterEqualsToBoundary) {
                searchAfter = reverse ? minValue : maxValue;
            } else {
                int from = 1, to = values.length - 1;
                while (values[from].equals(values[0]))
                    from++;
                while (values[to - 1].equals(values[values.length - 1]))
                    to--;
                assert from < to;
                searchAfter = randomFrom(Arrays.asList(values).subList(from, to)).utf8ToString();
            }
        }
        if (customMissingValue) {
            missingValue = randomBoolean() ? randomFrom(values).utf8ToString() : searchAfter;
        } else {
            missingValue = sortMissingLast ? "_last" : "_first";
        }
        shardRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).trackTotalHits(false));
        shardRequest.source().sort(SortBuilders.fieldSort("field1").missing(missingValue).order(reverse ? SortOrder.DESC : SortOrder.ASC));
        List<Object> searchAfterFields = new ArrayList<>();
        searchAfterFields.add(searchAfter);
        if (singleSort == false) {
            shardRequest.source().sort("field2");
            searchAfterFields.add(randomAlphaOfLength(10));
        }
        shardRequest.source().searchAfter(searchAfterFields.toArray());
        final SearchService service = getInstanceFromNode(SearchService.class);
        SearchService.CanMatchResponse response = service.canMatch(shardRequest);

        if (customMissingValue) {
            assertTrue(response.canMatch());
            return;
        }

        final boolean nonMatch;
        if (allDocsHaveValue) {
            nonMatch = searchAfterOutOfRange || (singleSort && searchAfterEqualsToBoundary);
        } else {
            if (searchAfterOutOfRange) {
                nonMatch = sortMissingLast == false;
            } else {
                nonMatch = singleSort && searchAfterEqualsToBoundary && sortMissingLast == false;
            }
        }

        boolean canMatch = response.canMatch();
        assertEquals(nonMatch == false, canMatch);
    }

    public void testStringCanMatch() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        IndexService indexService = createIndex(
            "test",
            Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build(),
            MapperService.SINGLE_MAPPING_NAME,
            mapping
        );
        ensureGreen();

        final boolean allDocsHaveValue = randomBoolean();
        final BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        final int numDocs = randomIntBetween(10, 20);
        final BytesRef[] field1 = new BytesRef[allDocsHaveValue ? numDocs : numDocs - 1];
        for (int i = 0; i < numDocs; i++) {
            String source;
            if (i < numDocs - 1 || allDocsHaveValue) {
                String str = randomAlphaOfLengthBetween(10, 20);
                field1[i] = new BytesRef(str);
                source = String.format(Locale.ROOT, "{\"field1\": \"%s\", \"field2\": \"%s\"}", str, randomAlphaOfLength(10));
            } else {
                source = String.format(Locale.ROOT, "{\"field2\": \"%s\"}", randomAlphaOfLength(10));
            }
            bulkRequestBuilder.add(client().prepareIndex("test").setId(String.valueOf(i)).setSource(source, MediaTypeRegistry.JSON));
        }
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        final IndexShard shard = indexService.getShard(0);
        final ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(true),
            shard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1f,
            -1,
            null,
            null
        );
        assertFalse(shard.hasRefreshPending());

        for (boolean singleSort : new boolean[] { false, true }) {
            for (boolean searchAfterOutOfRange : new boolean[] { false, true }) {
                for (boolean searchAfterEqualsToBoundary : new boolean[] { false, true }) {
                    for (boolean customMissingValue : new boolean[] { false, true }) {
                        for (boolean sortMissingLast : new boolean[] { false, true }) {
                            runStringCanMatchTest(
                                shardRequest,
                                field1,
                                allDocsHaveValue,
                                singleSort,
                                searchAfterOutOfRange,
                                searchAfterEqualsToBoundary,
                                customMissingValue,
                                sortMissingLast
                            );
                        }
                    }
                }
            }
        }
    }

    public void testSetSearchThrottled() {
        createIndex("throttled_threadpool_index");
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request(
                "throttled_threadpool_index",
                IndexSettings.INDEX_SEARCH_THROTTLED.getKey(),
                "true"
            )
        ).actionGet();
        final SearchService service = getInstanceFromNode(SearchService.class);
        Index index = resolveIndex("throttled_threadpool_index");
        assertTrue(service.getIndicesService().indexServiceSafe(index).getIndexSettings().isSearchThrottled());
        client().prepareIndex("throttled_threadpool_index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("throttled_threadpool_index")
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
            .setSize(1)
            .get();
        assertSearchHits(searchResponse, "1");
        // we add a search action listener in a plugin above to assert that this is actually used
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request(
                "throttled_threadpool_index",
                IndexSettings.INDEX_SEARCH_THROTTLED.getKey(),
                "false"
            )
        ).actionGet();

        SettingsException se = expectThrows(
            SettingsException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("throttled_threadpool_index")
                .setSettings(Settings.builder().put(IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), false))
                .get()
        );
        assertEquals("can not update private setting [index.search.throttled]; this setting is managed by OpenSearch", se.getMessage());
        assertFalse(service.getIndicesService().indexServiceSafe(index).getIndexSettings().isSearchThrottled());
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        ShardSearchRequest req = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            new ShardId(index, 0),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1f,
            -1,
            null,
            null
        );
        Thread currentThread = Thread.currentThread();
        // we still make sure can match is executed on the network thread
        service.canMatch(req, ActionListener.wrap(r -> assertSame(Thread.currentThread(), currentThread), e -> fail("unexpected")));
    }

    public void testExpandSearchThrottled() {
        createIndex("throttled_threadpool_index");
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request(
                "throttled_threadpool_index",
                IndexSettings.INDEX_SEARCH_THROTTLED.getKey(),
                "true"
            )
        ).actionGet();

        client().prepareIndex("throttled_threadpool_index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(client().prepareSearch().get(), 1L);
        assertHitCount(client().prepareSearch().setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED).get(), 1L);
    }

    public void testExpandSearchFrozen() {
        createIndex("frozen_index");
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("frozen_index", "index.frozen", "true")
        ).actionGet();

        client().prepareIndex("frozen_index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(client().prepareSearch().get(), 0L);
        assertHitCount(client().prepareSearch().setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED).get(), 1L);
    }

    public void testCreateReduceContext() {
        SearchService service = getInstanceFromNode(SearchService.class);
        InternalAggregation.ReduceContextBuilder reduceContextBuilder = service.aggReduceContextBuilder(new SearchSourceBuilder());
        {
            InternalAggregation.ReduceContext reduceContext = reduceContextBuilder.forFinalReduction();
            expectThrows(
                MultiBucketConsumerService.TooManyBucketsException.class,
                () -> reduceContext.consumeBucketsAndMaybeBreak(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 1)
            );
        }
        {
            InternalAggregation.ReduceContext reduceContext = reduceContextBuilder.forPartialReduction();
            reduceContext.consumeBucketsAndMaybeBreak(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 1);
        }
    }

    public void testCreateSearchContext() throws IOException {
        String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        ShardId shardId = new ShardId(indexService.index(), 0);
        long nowInMillis = System.currentTimeMillis();
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(randomBoolean());
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            shardId,
            indexService.numberOfShards(),
            AliasFilter.EMPTY,
            1f,
            nowInMillis,
            clusterAlias,
            Strings.EMPTY_ARRAY
        );
        try (DefaultSearchContext searchContext = service.createSearchContext(request, new TimeValue(System.currentTimeMillis()))) {
            SearchShardTarget searchShardTarget = searchContext.shardTarget();
            QueryShardContext queryShardContext = searchContext.getQueryShardContext();
            String expectedIndexName = clusterAlias == null ? index : clusterAlias + ":" + index;
            assertEquals(expectedIndexName, queryShardContext.getFullyQualifiedIndex().getName());
            assertEquals(expectedIndexName, searchShardTarget.getFullyQualifiedIndexName());
            assertEquals(clusterAlias, searchShardTarget.getClusterAlias());
            assertEquals(shardId, searchShardTarget.getShardId());
            assertSame(searchShardTarget, searchContext.dfsResult().getSearchShardTarget());
            assertSame(searchShardTarget, searchContext.queryResult().getSearchShardTarget());
            assertSame(searchShardTarget, searchContext.fetchResult().getSearchShardTarget());
        }
    }

    /**
     * Test that the Search Context for concurrent segment search enabled is set correctly based on both
     * index and cluster settings.
     */
    public void testConcurrentSegmentSearchSearchContext() throws IOException {
        Object[][] scenarios = {
            // cluster setting, index setting, cluster mode setting, concurrent search enabled?, concurrent search executor null?
            { null, null, null, false },
            { null, false, null, false },
            { null, true, null, true },
            { true, null, null, true },
            { true, false, null, false },
            { true, true, null, true },
            { false, null, null, false },
            { false, false, null, false },
            { false, true, null, true },

            // Adding cases with mode set to "none"
            { null, null, "none", false },
            { true, true, "none", false },
            { false, false, "none", false },
            { true, false, "none", false },
            { false, true, "none", false },

            // Adding cases with mode set to "all"
            { null, null, "all", true },
            { true, true, "all", true },
            { false, false, "all", true },
            { true, false, "all", true },
            { false, true, "all", true },

            // Adding cases with mode set to "auto"
            // auto mode concurrent search is false since request has no aggregation
            // however concurrentSearchExecutor will not be null
            { null, null, "auto", false },
            { true, true, "auto", false },
            { false, false, "auto", false },
            { true, false, "auto", false },
            { false, true, "auto", false } };

        String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        ShardId shardId = new ShardId(indexService.index(), 0);
        long nowInMillis = System.currentTimeMillis();
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(randomBoolean());
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            shardId,
            indexService.numberOfShards(),
            AliasFilter.EMPTY,
            1f,
            nowInMillis,
            clusterAlias,
            Strings.EMPTY_ARRAY
        );

        for (Object[] scenario : scenarios) {
            Boolean clusterSetting = (Boolean) scenario[0];
            Boolean indexSetting = (Boolean) scenario[1];
            String mode = (String) scenario[2];
            Boolean concurrentSearchEnabled = (Boolean) scenario[3];

            if (clusterSetting == null) {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey()))
                    .get();
            } else {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(
                        Settings.builder().put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), clusterSetting)
                    )
                    .get();
            }

            if (indexSetting == null) {
                client().admin()
                    .indices()
                    .prepareUpdateSettings(index)
                    .setSettings(Settings.builder().putNull(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey()))
                    .get();
            } else {
                client().admin()
                    .indices()
                    .prepareUpdateSettings(index)
                    .setSettings(Settings.builder().put(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), indexSetting))
                    .get();
            }

            // update mode
            if (mode == null) {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey()))
                    .get();
            } else {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), mode))
                    .get();
            }

            try (DefaultSearchContext searchContext = service.createSearchContext(request, new TimeValue(System.currentTimeMillis()))) {
                assertEquals(
                    clusterSetting,
                    client().admin()
                        .cluster()
                        .prepareState()
                        .get()
                        .getState()
                        .getMetadata()
                        .transientSettings()
                        .getAsBoolean(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), null)
                );
                assertEquals(
                    indexSetting == null ? null : indexSetting.toString(),
                    client().admin()
                        .indices()
                        .prepareGetSettings(index)
                        .get()
                        .getSetting(index, IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey())
                );

                assertEquals(
                    mode,
                    client().admin()
                        .cluster()
                        .prepareState()
                        .get()
                        .getState()
                        .getMetadata()
                        .transientSettings()
                        .get(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), null)
                );
                searchContext.evaluateRequestShouldUseConcurrentSearch();
                assertEquals(concurrentSearchEnabled, searchContext.shouldUseConcurrentSearch());
                assertThat(searchContext.searcher().getTaskExecutor(), is(notNullValue()));
            }
        }
        // Cleanup
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .putNull(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey())
                    .putNull(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey())
            )
            .get();
        assertSettingDeprecationsAndWarnings(new Setting[] { IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING });
    }

    public void testConcurrentSegmentSearchWithRandomizedModeSettings() throws IOException {

        String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        ShardId shardId = new ShardId(indexService.index(), 0);
        long nowInMillis = System.currentTimeMillis();
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(randomBoolean());
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            shardId,
            indexService.numberOfShards(),
            AliasFilter.EMPTY,
            1f,
            nowInMillis,
            clusterAlias,
            Strings.EMPTY_ARRAY
        );

        String[] modeSettings = { "all", "auto", "none", null };

        // Randomize both index and cluster settings
        String clusterMode = randomFrom(modeSettings);
        String indexMode = randomFrom(modeSettings);

        // Set the cluster setting for mode
        if (clusterMode == null) {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey()))
                .get();
        } else {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), clusterMode))
                .get();
        }

        // Set the index setting for mode
        if (indexMode == null) {
            client().admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(Settings.builder().putNull(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MODE.getKey()))
                .get();
        } else {
            client().admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(Settings.builder().put(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), indexMode))
                .get();
        }

        assertEquals(
            clusterMode,
            client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .getMetadata()
                .transientSettings()
                .get(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey())
        );
        assertEquals(
            indexMode,
            client().admin()
                .indices()
                .prepareGetSettings(index)
                .get()
                .getSetting(index, IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MODE.getKey())
        );

        boolean concurrentSearchEnabled;
        // with aggregations.
        {
            boolean aggregationSupportsConcurrent = randomBoolean();
            // Default concurrent search mode is auto, which enables concurrent segment search when aggregations are present.
            // aggregationSupportsConcurrent determines if the present aggregation type supports concurrent segment search.
            concurrentSearchEnabled = aggregationSupportsConcurrent;
            if (indexMode != null) {
                concurrentSearchEnabled = !indexMode.equals("none") && aggregationSupportsConcurrent;
            } else if (clusterMode != null) {
                concurrentSearchEnabled = !clusterMode.equals("none") && aggregationSupportsConcurrent;
            }

            try (DefaultSearchContext searchContext = service.createSearchContext(request, new TimeValue(System.currentTimeMillis()))) {
                SearchContextAggregations mockAggregations = mock(SearchContextAggregations.class);
                when(mockAggregations.factories()).thenReturn(mock(AggregatorFactories.class));
                when(mockAggregations.factories().allFactoriesSupportConcurrentSearch()).thenReturn(aggregationSupportsConcurrent);

                // set the aggregations for context
                searchContext.aggregations(mockAggregations);

                searchContext.evaluateRequestShouldUseConcurrentSearch();
                // check concurrentSearchenabled based on mode and supportedAggregation is computed correctly
                assertEquals(concurrentSearchEnabled, searchContext.shouldUseConcurrentSearch());
                assertThat(searchContext.searcher().getTaskExecutor(), is(notNullValue()));
            }
        }

        // without aggregations.
        {
            // Default concurrent search mode is auto, without aggregations, concurrent search will be disabled.
            concurrentSearchEnabled = false;
            if (indexMode != null) {
                concurrentSearchEnabled = indexMode.equals("all");
            } else if (clusterMode != null) {
                concurrentSearchEnabled = clusterMode.equals("all");
            }
            try (DefaultSearchContext searchContext = service.createSearchContext(request, new TimeValue(System.currentTimeMillis()))) {
                searchContext.evaluateRequestShouldUseConcurrentSearch();
                assertEquals(concurrentSearchEnabled, searchContext.shouldUseConcurrentSearch());
                assertThat(searchContext.searcher().getTaskExecutor(), is(notNullValue()));
            }
        }

        // Cleanup
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey()))
            .get();
    }

    /**
     * Tests that the slice count is calculated correctly when concurrent search is enabled
     *  If concurrent search enabled -
     *       pick index level slice count setting if index level setting is set
     *       else pick default cluster level slice count setting
     * @throws IOException
     */
    public void testConcurrentSegmentSearchSliceCount() throws IOException {

        String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        ShardId shardId = new ShardId(indexService.index(), 0);
        long nowInMillis = System.currentTimeMillis();
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(randomBoolean());
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            shardId,
            indexService.numberOfShards(),
            AliasFilter.EMPTY,
            1f,
            nowInMillis,
            clusterAlias,
            Strings.EMPTY_ARRAY
        );
        // enable concurrent search
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true))
            .get();

        Integer[][] scenarios = {
            // cluster setting, index setting, expected slice count
            // expected value null will pick up default value from settings
            { null, null, clusterService.getClusterSettings().get(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING) },
            { 4, null, 4 },
            { null, 3, 3 },
            { 4, 3, 3 }, };

        for (Integer[] sliceCounts : scenarios) {
            Integer clusterSliceCount = sliceCounts[0];
            Integer indexSliceCount = sliceCounts[1];
            Integer targetSliceCount = sliceCounts[2];

            if (clusterSliceCount != null) {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(
                        Settings.builder()
                            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), clusterSliceCount)
                    )
                    .get();
            } else {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(
                        Settings.builder().putNull(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey())
                    )
                    .get();
            }
            if (indexSliceCount != null) {
                client().admin()
                    .indices()
                    .prepareUpdateSettings(index)
                    .setSettings(
                        Settings.builder().put(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT.getKey(), indexSliceCount)
                    )
                    .get();
            } else {
                client().admin()
                    .indices()
                    .prepareUpdateSettings(index)
                    .setSettings(Settings.builder().putNull(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT.getKey()))
                    .get();
            }

            try (DefaultSearchContext searchContext = service.createSearchContext(request, new TimeValue(System.currentTimeMillis()))) {
                searchContext.evaluateRequestShouldUseConcurrentSearch();
                assertEquals(targetSliceCount.intValue(), searchContext.getTargetMaxSliceCount());
            }
        }
        // cleanup
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .putNull(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey())
                    .putNull(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey())
            )
            .get();
    }

    /**
     * Test that the Search Context for concurrent segment search enabled is set correctly at the time of construction.
     * The same is used throughout the context object lifetime even if cluster setting changes before the request completion.
     */
    public void testConcurrentSegmentSearchIsSetOnceDuringContextCreation() throws IOException {
        String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        ShardId shardId = new ShardId(indexService.index(), 0);
        long nowInMillis = System.currentTimeMillis();
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(randomBoolean());
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            shardId,
            indexService.numberOfShards(),
            AliasFilter.EMPTY,
            1f,
            nowInMillis,
            clusterAlias,
            Strings.EMPTY_ARRAY
        );

        Boolean[] concurrentSearchStates = new Boolean[] { true, false };
        for (Boolean concurrentSearchSetting : concurrentSearchStates) {
            // update concurrent search cluster setting and create search context
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder().put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), concurrentSearchSetting)
                )
                .get();
            try (DefaultSearchContext searchContext = service.createSearchContext(request, new TimeValue(System.currentTimeMillis()))) {
                // verify concurrent search state in context
                searchContext.evaluateRequestShouldUseConcurrentSearch();
                assertEquals(concurrentSearchSetting, searchContext.shouldUseConcurrentSearch());
                // verify executor state in searcher
                assertThat(searchContext.searcher().getTaskExecutor(), is(notNullValue()));

                // update cluster setting to flip the concurrent segment search state
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(
                        Settings.builder().put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), !concurrentSearchSetting)
                    )
                    .get();

                // verify that concurrent segment search is still set to same expected value for the context
                assertEquals(concurrentSearchSetting, searchContext.shouldUseConcurrentSearch());
            } finally {
                // Cleanup
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey()))
                    .get();
            }
        }
    }

    /**
     * While we have no NPE in DefaultContext constructor anymore, we still want to guard against it (or other failures) in the future to
     * avoid leaking searchers.
     */
    public void testCreateSearchContextFailure() throws Exception {
        final String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        final IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        final ShardId shardId = new ShardId(indexService.index(), 0);
        final ShardSearchRequest request = new ShardSearchRequest(shardId, 0, null) {
            @Override
            public SearchType searchType() {
                // induce an artificial NPE
                throw new NullPointerException("expected");
            }
        };
        try (ReaderContext reader = createReaderContext(indexService, indexService.getShard(shardId.id()))) {
            NullPointerException e = expectThrows(
                NullPointerException.class,
                () -> service.createContext(reader, request, null, randomBoolean())
            );
            assertEquals("expected", e.getMessage());
        }
        // Needs to busily assert because Engine#refreshNeeded can increase the refCount.
        assertBusy(
            () -> assertEquals("should have 2 store refs (IndexService + InternalEngine)", 2, indexService.getShard(0).store().refCount())
        );
    }

    public void testMatchNoDocsEmptyResponse() throws InterruptedException {
        createIndex("index");
        Thread currentThread = Thread.currentThread();
        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().aggregation(AggregationBuilders.count("count").field("value")));
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            5,
            AliasFilter.EMPTY,
            1.0f,
            0,
            null,
            null
        );
        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());

        {
            CountDownLatch latch = new CountDownLatch(1);
            shardRequest.source().query(new MatchAllQueryBuilder());
            service.executeQueryPhase(shardRequest, randomBoolean(), task, new ActionListener<SearchPhaseResult>() {
                @Override
                public void onResponse(SearchPhaseResult result) {
                    try {
                        assertNotSame(Thread.currentThread(), currentThread);
                        assertThat(Thread.currentThread().getName(), startsWith("opensearch[node_s_0][search]"));
                        assertThat(result, instanceOf(QuerySearchResult.class));
                        assertFalse(result.queryResult().isNull());
                        assertNotNull(result.queryResult().topDocs());
                        assertNotNull(result.queryResult().aggregations());
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    try {
                        throw new AssertionError(exc);
                    } finally {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        }

        {
            CountDownLatch latch = new CountDownLatch(1);
            shardRequest.source().query(new MatchNoneQueryBuilder());
            service.executeQueryPhase(shardRequest, randomBoolean(), task, new ActionListener<SearchPhaseResult>() {
                @Override
                public void onResponse(SearchPhaseResult result) {
                    try {
                        assertNotSame(Thread.currentThread(), currentThread);
                        assertThat(Thread.currentThread().getName(), startsWith("opensearch[node_s_0][search]"));
                        assertThat(result, instanceOf(QuerySearchResult.class));
                        assertFalse(result.queryResult().isNull());
                        assertNotNull(result.queryResult().topDocs());
                        assertNotNull(result.queryResult().aggregations());
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    try {
                        throw new AssertionError(exc);
                    } finally {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        }

        {
            CountDownLatch latch = new CountDownLatch(1);
            shardRequest.canReturnNullResponseIfMatchNoDocs(true);
            service.executeQueryPhase(shardRequest, randomBoolean(), task, new ActionListener<SearchPhaseResult>() {
                @Override
                public void onResponse(SearchPhaseResult result) {
                    try {
                        // make sure we don't use the search threadpool
                        assertSame(Thread.currentThread(), currentThread);
                        assertThat(result, instanceOf(QuerySearchResult.class));
                        assertTrue(result.queryResult().isNull());
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        throw new AssertionError(e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        }
    }

    public void testDeleteIndexWhileSearch() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(1, 20);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setSource("f", "v").get();
        }
        client().admin().indices().prepareRefresh("test").get();
        AtomicBoolean stopped = new AtomicBoolean(false);
        Thread[] searchers = new Thread[randomIntBetween(1, 4)];
        CountDownLatch latch = new CountDownLatch(searchers.length);
        for (int i = 0; i < searchers.length; i++) {
            searchers[i] = new Thread(() -> {
                latch.countDown();
                while (stopped.get() == false) {
                    try {
                        client().prepareSearch("test").setRequestCache(false).get();
                    } catch (Exception ignored) {
                        return;
                    }
                }
            });
            searchers[i].start();
        }
        latch.await();
        client().admin().indices().prepareDelete("test").get();
        stopped.set(true);
        for (Thread searcher : searchers) {
            searcher.join();
        }
    }

    public void testLookUpSearchContext() throws Exception {
        createIndex("index");
        SearchService searchService = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        List<ShardSearchContextId> contextIds = new ArrayList<>();
        int numContexts = randomIntBetween(1, 10);
        CountDownLatch latch = new CountDownLatch(1);
        indexShard.getThreadPool().executor(ThreadPool.Names.SEARCH).execute(() -> {
            try {
                for (int i = 0; i < numContexts; i++) {
                    ShardSearchRequest request = new ShardSearchRequest(
                        OriginalIndices.NONE,
                        new SearchRequest().allowPartialSearchResults(true),
                        indexShard.shardId(),
                        1,
                        new AliasFilter(null, Strings.EMPTY_ARRAY),
                        1.0f,
                        -1,
                        null,
                        null
                    );
                    final ReaderContext context = searchService.createAndPutReaderContext(
                        request,
                        indexService,
                        indexShard,
                        indexShard.acquireSearcherSupplier(),
                        randomBoolean()
                    );
                    assertThat(context.id().getId(), equalTo((long) (i + 1)));
                    contextIds.add(context.id());
                }
                assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                while (contextIds.isEmpty() == false) {
                    final ShardSearchContextId contextId = randomFrom(contextIds);
                    expectThrows(
                        SearchContextMissingException.class,
                        () -> searchService.freeReaderContext(new ShardSearchContextId(UUIDs.randomBase64UUID(), contextId.getId()))
                    );
                    assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                    if (randomBoolean()) {
                        assertTrue(searchService.freeReaderContext(contextId));
                    } else {
                        assertTrue(
                            searchService.freeReaderContext((new ShardSearchContextId(contextId.getSessionId(), contextId.getId())))
                        );
                    }
                    contextIds.remove(contextId);
                    assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                    assertFalse(searchService.freeReaderContext(contextId));
                    assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                }
            } finally {
                latch.countDown();
            }
        });
        latch.await();
    }

    public void testOpenReaderContext() {
        createIndex("index");
        SearchService searchService = getInstanceFromNode(SearchService.class);
        PlainActionFuture<ShardSearchContextId> future = new PlainActionFuture<>();
        searchService.createPitReaderContext(new ShardId(resolveIndex("index"), 0), TimeValue.timeValueMinutes(between(1, 10)), future);
        future.actionGet();
        assertThat(searchService.getActiveContexts(), equalTo(1));
        assertThat(searchService.getAllPITReaderContexts().size(), equalTo(1));
        assertTrue(searchService.freeReaderContext(future.actionGet()));
    }

    private ReaderContext createReaderContext(IndexService indexService, IndexShard indexShard) {
        return new ReaderContext(
            new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong()),
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            randomNonNegativeLong(),
            false
        );
    }

    public void testDeletePitReaderContext() throws ExecutionException, InterruptedException {
        createIndex("index");
        SearchService searchService = getInstanceFromNode(SearchService.class);
        PlainActionFuture<ShardSearchContextId> future = new PlainActionFuture<>();
        searchService.createPitReaderContext(new ShardId(resolveIndex("index"), 0), TimeValue.timeValueMinutes(between(1, 10)), future);
        List<PitSearchContextIdForNode> contextIds = new ArrayList<>();
        ShardSearchContextId shardSearchContextId = future.actionGet();
        PitSearchContextIdForNode pitSearchContextIdForNode = new PitSearchContextIdForNode(
            "1",
            new SearchContextIdForNode(null, "node1", shardSearchContextId)
        );
        contextIds.add(pitSearchContextIdForNode);

        assertThat(searchService.getActiveContexts(), equalTo(1));
        assertThat(searchService.getAllPITReaderContexts().size(), equalTo(1));
        validatePitStats("index", 1, 0, 0);
        DeletePitResponse deletePitResponse = searchService.freeReaderContextsIfFound(contextIds);
        assertTrue(deletePitResponse.getDeletePitResults().get(0).isSuccessful());
        // assert true for reader context not found
        deletePitResponse = searchService.freeReaderContextsIfFound(contextIds);
        assertTrue(deletePitResponse.getDeletePitResults().get(0).isSuccessful());
        // adding this assert to showcase behavior difference
        assertFalse(searchService.freeReaderContext(future.actionGet()));
        validatePitStats("index", 0, 1, 0);
    }

    public void testPitContextMaxKeepAlive() {
        createIndex("index");
        SearchService searchService = getInstanceFromNode(SearchService.class);
        PlainActionFuture<ShardSearchContextId> future = new PlainActionFuture<>();

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            searchService.createPitReaderContext(new ShardId(resolveIndex("index"), 0), TimeValue.timeValueHours(25), future);
            future.actionGet();
        });
        assertEquals(
            "Keep alive for request (1d) is too large. "
                + "It must be less than ("
                + SearchService.MAX_PIT_KEEPALIVE_SETTING.get(Settings.EMPTY)
                + "). "
                + "This limit can be set by changing the ["
                + SearchService.MAX_PIT_KEEPALIVE_SETTING.getKey()
                + "] cluster level setting.",
            ex.getMessage()
        );
        assertThat(searchService.getActiveContexts(), equalTo(0));
        assertThat(searchService.getAllPITReaderContexts().size(), equalTo(0));
    }

    public void testUpdatePitId() throws ExecutionException, InterruptedException {
        createIndex("index");
        SearchService searchService = getInstanceFromNode(SearchService.class);
        PlainActionFuture<ShardSearchContextId> future = new PlainActionFuture<>();
        searchService.createPitReaderContext(new ShardId(resolveIndex("index"), 0), TimeValue.timeValueMinutes(between(1, 10)), future);
        ShardSearchContextId id = future.actionGet();
        PlainActionFuture<UpdatePitContextResponse> updateFuture = new PlainActionFuture<>();
        UpdatePitContextRequest updateRequest = new UpdatePitContextRequest(
            id,
            "pitId",
            TimeValue.timeValueMinutes(between(1, 10)).millis(),
            System.currentTimeMillis()
        );
        searchService.updatePitIdAndKeepAlive(updateRequest, updateFuture);
        UpdatePitContextResponse updateResponse = updateFuture.actionGet();
        assertTrue(updateResponse.getPitId().equalsIgnoreCase("pitId"));
        assertTrue(updateResponse.getCreationTime() == updateRequest.getCreationTime());
        assertTrue(updateResponse.getKeepAlive() == updateRequest.getKeepAlive());
        assertTrue(updateResponse.getPitId().equalsIgnoreCase("pitId"));
        assertThat(searchService.getActiveContexts(), equalTo(1));
        assertThat(searchService.getAllPITReaderContexts().size(), equalTo(1));
        validatePitStats("index", 1, 0, 0);
        assertTrue(searchService.freeReaderContext(future.actionGet()));
        validatePitStats("index", 0, 1, 0);
    }

    public void testUpdatePitIdMaxKeepAlive() {
        createIndex("index");
        SearchService searchService = getInstanceFromNode(SearchService.class);
        PlainActionFuture<ShardSearchContextId> future = new PlainActionFuture<>();
        searchService.createPitReaderContext(new ShardId(resolveIndex("index"), 0), TimeValue.timeValueMinutes(between(1, 10)), future);
        ShardSearchContextId id = future.actionGet();

        UpdatePitContextRequest updateRequest = new UpdatePitContextRequest(
            id,
            "pitId",
            TimeValue.timeValueHours(25).millis(),
            System.currentTimeMillis()
        );
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            PlainActionFuture<UpdatePitContextResponse> updateFuture = new PlainActionFuture<>();
            searchService.updatePitIdAndKeepAlive(updateRequest, updateFuture);
        });

        assertEquals(
            "Keep alive for request (1d) is too large. "
                + "It must be less than ("
                + SearchService.MAX_PIT_KEEPALIVE_SETTING.get(Settings.EMPTY)
                + "). "
                + "This limit can be set by changing the ["
                + SearchService.MAX_PIT_KEEPALIVE_SETTING.getKey()
                + "] cluster level setting.",
            ex.getMessage()
        );
        assertThat(searchService.getActiveContexts(), equalTo(1));
        assertThat(searchService.getAllPITReaderContexts().size(), equalTo(1));
        assertTrue(searchService.freeReaderContext(future.actionGet()));
    }

    public void testUpdatePitIdWithInvalidReaderId() {
        SearchService searchService = getInstanceFromNode(SearchService.class);
        ShardSearchContextId id = new ShardSearchContextId("session", 9);

        UpdatePitContextRequest updateRequest = new UpdatePitContextRequest(
            id,
            "pitId",
            TimeValue.timeValueHours(23).millis(),
            System.currentTimeMillis()
        );
        SearchContextMissingException ex = expectThrows(SearchContextMissingException.class, () -> {
            PlainActionFuture<UpdatePitContextResponse> updateFuture = new PlainActionFuture<>();
            searchService.updatePitIdAndKeepAlive(updateRequest, updateFuture);
        });

        assertEquals("No search context found for id [" + id.getId() + "]", ex.getMessage());
        assertThat(searchService.getActiveContexts(), equalTo(0));
        assertThat(searchService.getAllPITReaderContexts().size(), equalTo(0));
    }

    public void validatePitStats(String index, long expectedPitCurrent, long expectedPitCount, int shardId) throws ExecutionException,
        InterruptedException {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(index));
        IndexShard indexShard = indexService.getShard(shardId);
        assertEquals(expectedPitCurrent, indexShard.searchStats().getTotal().getPitCurrent());
        assertEquals(expectedPitCount, indexShard.searchStats().getTotal().getPitCount());
    }

    /**
     * Test for ASC order search_after query.
     * Min = 0L, Max = 9L, search_after = 10L, missing = Long.MIN_VALUE
     * Expected result is canMatch = false
     */
    public void testCanMatchSearchAfterAscGreaterThanMaxAndMissing() throws IOException {
        FieldDoc searchAfter = new FieldDoc(0, 0, new Long[] { 10L });
        MinAndMax<?> minMax = new MinAndMax<Long>(0L, 9L);
        SortField sortField = new SortField("test", SortField.Type.LONG);
        sortField.setMissingValue(randomLongBetween(Long.MIN_VALUE, 9L));
        final SortAndFormats primarySort = new SortAndFormats(new Sort(sortField), new DocValueFormat[] { DocValueFormat.RAW });
        final FieldStats fieldStats = new FieldStats(minMax, randomBoolean());
        assertFalse(
            SearchService.canMatchSearchAfter(
                searchAfter,
                fieldStats,
                primarySort,
                SearchContext.TRACK_TOTAL_HITS_DISABLED,
                randomBoolean()
            )
        );
    }

    /**
     * Test for ASC order search_after query.
     * Min = 0L, Max = 9L, search_after = 10L, missing = 11L
     * Expected result is canMatch = true
     */
    public void testCanMatchSearchAfterAscGreaterThanMaxAndLessThanMissing() throws IOException {
        FieldDoc searchAfter = new FieldDoc(0, 0, new Long[] { 10L });
        MinAndMax<?> minMax = new MinAndMax<Long>(0L, 9L);
        SortField sortField = new SortField("test", SortField.Type.LONG);
        sortField.setMissingValue(11L);
        final SortAndFormats primarySort = new SortAndFormats(new Sort(sortField), new DocValueFormat[] { DocValueFormat.RAW });
        final FieldStats fieldStats = new FieldStats(minMax, randomBoolean());
        assertTrue(
            SearchService.canMatchSearchAfter(
                searchAfter,
                fieldStats,
                primarySort,
                SearchContext.TRACK_TOTAL_HITS_DISABLED,
                randomBoolean()
            )
        );
    }

    /**
     * Test for ASC order search_after query.
     * Min = 0L, Max = 9L, search_after = 7L, missing = Long.MIN_VALUE/10L
     * Expected result is canMatch = true
     */
    public void testCanMatchSearchAfterAscLessThanMax() throws IOException {
        FieldDoc searchAfter = new FieldDoc(0, 0, new Long[] { 7L });
        MinAndMax<?> minMax = new MinAndMax<Long>(0L, 9L);
        SortField sortField = new SortField("test", SortField.Type.LONG);
        sortField.setMissingValue(randomFrom(Long.MIN_VALUE, 10L));
        final SortAndFormats primarySort = new SortAndFormats(new Sort(sortField), new DocValueFormat[] { DocValueFormat.RAW });
        final FieldStats fieldStats = new FieldStats(minMax, randomBoolean());
        assertTrue(
            SearchService.canMatchSearchAfter(
                searchAfter,
                fieldStats,
                primarySort,
                SearchContext.TRACK_TOTAL_HITS_DISABLED,
                randomBoolean()
            )
        );
    }

    /**
     * Test for ASC order search_after query.
     * Min = 0L, Max = 9L, search_after = 9L, missing = 9L
     * Expected result is canMatch = true
     */
    public void testCanMatchSearchAfterAscEqualMaxAndMissing() throws IOException {
        FieldDoc searchAfter = new FieldDoc(0, 0, new Long[] { 9L });
        MinAndMax<?> minMax = new MinAndMax<Long>(0L, 9L);
        SortField sortField = new SortField("test", SortField.Type.LONG);
        sortField.setMissingValue(9L);
        final SortAndFormats primarySort = new SortAndFormats(new Sort(sortField), new DocValueFormat[] { DocValueFormat.RAW });
        final FieldStats fieldStats = new FieldStats(minMax, randomBoolean());
        final boolean singleSort = randomBoolean();
        assertEquals(
            singleSort == false,
            SearchService.canMatchSearchAfter(searchAfter, fieldStats, primarySort, SearchContext.TRACK_TOTAL_HITS_DISABLED, singleSort)
        );
    }

    /**
     * Test for DESC order search_after query.
     * Min = 0L, Max = 9L, search_after = 10L, missing = Long.MAX_VALUE/Long.MIN_VALUE
     * Expected result is canMatch = true
     */
    public void testCanMatchSearchAfterDescGreaterThanMin() throws IOException {
        FieldDoc searchAfter = new FieldDoc(0, 0, new Long[] { 10L });
        MinAndMax<?> minMax = new MinAndMax<Long>(0L, 9L);
        SortField sortField = new SortField("test", SortField.Type.LONG, true);
        sortField.setMissingValue(randomFrom(Long.MAX_VALUE, Long.MIN_VALUE));
        final SortAndFormats primarySort = new SortAndFormats(new Sort(sortField), new DocValueFormat[] { DocValueFormat.RAW });
        final FieldStats fieldStats = new FieldStats(minMax, randomBoolean());
        assertTrue(
            SearchService.canMatchSearchAfter(
                searchAfter,
                fieldStats,
                primarySort,
                SearchContext.TRACK_TOTAL_HITS_DISABLED,
                randomBoolean()
            )
        );
    }

    /**
     * Test for DESC order search_after query.
     * Min = 0L, Max = 9L, search_after = -1L, missing > search_after
     * Expected result is canMatch = false
     */
    public void testCanMatchSearchAfterDescLessThanMinAndMissing() throws IOException {
        FieldDoc searchAfter = new FieldDoc(0, 0, new Long[] { -1L });
        MinAndMax<?> minMax = new MinAndMax<Long>(0L, 9L);
        SortField sortField = new SortField("test", SortField.Type.LONG, true);
        sortField.setMissingValue(randomLongBetween(0L, Long.MAX_VALUE));
        final SortAndFormats primarySort = new SortAndFormats(new Sort(sortField), new DocValueFormat[] { DocValueFormat.RAW });
        final FieldStats fieldStats = new FieldStats(minMax, randomBoolean());
        assertFalse(
            SearchService.canMatchSearchAfter(
                searchAfter,
                fieldStats,
                primarySort,
                SearchContext.TRACK_TOTAL_HITS_DISABLED,
                randomBoolean()
            )
        );
    }

    /**
     * Test for DESC order search_after query.
     * Min = 0L, Max = 9L, search_after = 0L, missing = 0L
     * Expected result is canMatch = true
     */
    public void testCanMatchSearchAfterDescEqualMinAndMissing() throws IOException {
        FieldDoc searchAfter = new FieldDoc(0, 0, new Long[] { 0L });
        MinAndMax<?> minMax = new MinAndMax<Long>(0L, 9L);
        SortField sortField = new SortField("test", SortField.Type.LONG, true);
        sortField.setMissingValue(0L);
        final SortAndFormats primarySort = new SortAndFormats(new Sort(sortField), new DocValueFormat[] { DocValueFormat.RAW });
        final FieldStats fieldStats = new FieldStats(minMax, randomBoolean());
        final boolean singleSort = randomBoolean();
        assertEquals(
            singleSort == false,
            SearchService.canMatchSearchAfter(searchAfter, fieldStats, primarySort, SearchContext.TRACK_TOTAL_HITS_DISABLED, singleSort)
        );
    }

    /**
     * Test for DESC order search_after query with track_total_hits=true.
     * Min = 0L, Max = 9L, search_after = -1L, missing > search_after
     * With above min/max and search_after, it should not match, but since
     * track_total_hits = true,
     * Expected result is canMatch = true
     */
    public void testCanMatchSearchAfterDescLessThanMinAndMissingWithTrackTotalHits() throws IOException {
        FieldDoc searchAfter = new FieldDoc(0, 0, new Long[] { -1L });
        MinAndMax<?> minMax = new MinAndMax<Long>(0L, 9L);
        SortField sortField = new SortField("test", SortField.Type.LONG, true);
        sortField.setMissingValue(randomLongBetween(0L, Long.MAX_VALUE));
        final SortAndFormats primarySort = new SortAndFormats(new Sort(sortField), new DocValueFormat[] { DocValueFormat.RAW });
        final FieldStats fieldStats = new FieldStats(minMax, randomBoolean());
        assertTrue(SearchService.canMatchSearchAfter(searchAfter, fieldStats, primarySort, 1000, randomBoolean()));
    }
}

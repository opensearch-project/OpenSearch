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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.IndexCache;
import org.opensearch.index.cache.query.QueryCache;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.deciders.ConcurrentSearchDecision;
import org.opensearch.search.deciders.ConcurrentSearchRequestDecider;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.LegacyReaderContext;
import org.opensearch.search.internal.PitReaderContext;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.opensearch.index.IndexSettings.INDEX_SEARCH_THROTTLED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.when;

public class DefaultSearchContextTests extends OpenSearchTestCase {
    private final ExecutorService executor;

    @ParametersFactory
    public static Collection<Object[]> concurrency() {
        return Arrays.asList(new Integer[] { 0 }, new Integer[] { 5 });
    }

    public DefaultSearchContextTests(int concurrency) {
        this.executor = (concurrency > 0) ? Executors.newFixedThreadPool(concurrency) : null;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        if (executor != null) {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }

    public void testPreProcess() throws Exception {
        TimeValue timeout = new TimeValue(randomIntBetween(1, 100));
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);
        when(shardSearchRequest.shardId()).thenReturn(shardId);

        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = mock(IndexShard.class);
        QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        int maxResultWindow = randomIntBetween(50, 100);
        int maxRescoreWindow = randomIntBetween(50, 100);
        int maxSlicesPerScroll = randomIntBetween(50, 100);
        int maxSlicesPerPit = randomIntBetween(50, 100);
        Settings settings = Settings.builder()
            .put("index.max_result_window", maxResultWindow)
            .put("index.max_slices_per_scroll", maxSlicesPerScroll)
            .put("index.max_rescore_window", maxRescoreWindow)
            .put("index.max_slices_per_pit", maxSlicesPerPit)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .build();

        IndexService indexService = mock(IndexService.class);
        IndexCache indexCache = mock(IndexCache.class);
        QueryCache queryCache = mock(QueryCache.class);
        when(indexCache.query()).thenReturn(queryCache);
        when(indexService.cache()).thenReturn(indexCache);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(indexService.newQueryShardContext(eq(shardId.id()), any(), any(), nullable(String.class), anyBoolean(), anyBoolean()))
            .thenReturn(queryShardContext);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.hasNested()).thenReturn(randomBoolean());
        when(indexService.mapperService()).thenReturn(mapperService);

        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            final Supplier<Engine.SearcherSupplier> searcherSupplier = () -> new Engine.SearcherSupplier(Function.identity()) {
                @Override
                protected void doClose() {}

                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    try {
                        IndexReader reader = w.getReader();
                        return new Engine.Searcher(
                            "test",
                            reader,
                            IndexSearcher.getDefaultSimilarity(),
                            IndexSearcher.getDefaultQueryCache(),
                            IndexSearcher.getDefaultQueryCachingPolicy(),
                            reader
                        );
                    } catch (IOException exc) {
                        throw new AssertionError(exc);
                    }
                }
            };

            SearchShardTarget target = new SearchShardTarget("node", shardId, null, OriginalIndices.NONE);
            ReaderContext readerWithoutScroll = new ReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                randomNonNegativeLong(),
                false
            );

            DefaultSearchContext contextWithoutScroll = new DefaultSearchContext(
                readerWithoutScroll,
                shardSearchRequest,
                target,
                null,
                bigArrays,
                null,
                timeout,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            contextWithoutScroll.from(300);
            contextWithoutScroll.close();

            // resultWindow greater than maxResultWindow and scrollContext is null
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> contextWithoutScroll.preProcess(false));
            assertThat(
                exception.getMessage(),
                equalTo(
                    "Result window is too large, from + size must be less than or equal to:"
                        + " ["
                        + maxResultWindow
                        + "] but was [310]. See the scroll api for a more efficient way to request large data sets. "
                        + "This limit can be set by changing the ["
                        + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                        + "] index level setting."
                )
            );

            // resultWindow greater than maxResultWindow and scrollContext isn't null
            when(shardSearchRequest.scroll()).thenReturn(new Scroll(TimeValue.timeValueMillis(randomInt(1000))));
            ReaderContext readerContext = new LegacyReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                shardSearchRequest,
                randomNonNegativeLong()
            );
            DefaultSearchContext context1 = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                null,
                bigArrays,
                null,
                timeout,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            context1.from(300);
            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess(false));
            assertThat(
                exception.getMessage(),
                equalTo(
                    "Batch size is too large, size must be less than or equal to: ["
                        + maxResultWindow
                        + "] but was [310]. Scroll batch sizes cost as much memory as result windows so they are "
                        + "controlled by the ["
                        + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                        + "] index level setting."
                )
            );

            // resultWindow not greater than maxResultWindow and both rescore and sort are not null
            context1.from(0);
            DocValueFormat docValueFormat = mock(DocValueFormat.class);
            SortAndFormats sortAndFormats = new SortAndFormats(new Sort(), new DocValueFormat[] { docValueFormat });
            context1.sort(sortAndFormats);

            RescoreContext rescoreContext = mock(RescoreContext.class);
            when(rescoreContext.getWindowSize()).thenReturn(500);
            context1.addRescore(rescoreContext);

            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess(false));
            assertThat(exception.getMessage(), equalTo("Cannot use [sort] option in conjunction with [rescore]."));

            // rescore is null but sort is not null and rescoreContext.getWindowSize() exceeds maxResultWindow
            context1.sort(null);
            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess(false));

            assertThat(
                exception.getMessage(),
                equalTo(
                    "Rescore window ["
                        + rescoreContext.getWindowSize()
                        + "] is too large. "
                        + "It must be less than ["
                        + maxRescoreWindow
                        + "]. This prevents allocating massive heaps for storing the results "
                        + "to be rescored. This limit can be set by changing the ["
                        + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
                        + "] index level setting."
                )
            );

            readerContext.close();
            readerContext = new LegacyReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                shardSearchRequest,
                randomNonNegativeLong()
            );
            // rescore is null but sliceBuilder is not null
            DefaultSearchContext context2 = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                null,
                bigArrays,
                null,
                timeout,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );

            SliceBuilder sliceBuilder = mock(SliceBuilder.class);
            int numSlices = maxSlicesPerScroll + randomIntBetween(1, 100);
            when(sliceBuilder.getMax()).thenReturn(numSlices);
            context2.sliceBuilder(sliceBuilder);

            exception = expectThrows(IllegalArgumentException.class, () -> context2.preProcess(false));
            assertThat(
                exception.getMessage(),
                equalTo(
                    "The number of slices ["
                        + numSlices
                        + "] is too large. It must "
                        + "be less than ["
                        + maxSlicesPerScroll
                        + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_SLICES_PER_SCROLL.getKey()
                        + "] index level setting."
                )
            );

            // No exceptions should be thrown
            when(shardSearchRequest.getAliasFilter()).thenReturn(AliasFilter.EMPTY);
            when(shardSearchRequest.indexBoost()).thenReturn(AbstractQueryBuilder.DEFAULT_BOOST);

            DefaultSearchContext context3 = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                null,
                bigArrays,
                null,
                timeout,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            ParsedQuery parsedQuery = ParsedQuery.parsedMatchAllQuery();
            context3.sliceBuilder(null).parsedQuery(parsedQuery).preProcess(false);
            assertEquals(context3.query(), context3.buildFilteredQuery(parsedQuery.query()));
            // make sure getPreciseRelativeTimeInMillis is same as System.nanoTime()
            long timeToleranceInMs = 10;
            long currTime = TimeValue.nsecToMSec(System.nanoTime());
            assertTrue(Math.abs(context3.getRelativeTimeInMillis(false) - currTime) <= timeToleranceInMs);

            when(queryShardContext.getIndexSettings()).thenReturn(indexSettings);
            when(queryShardContext.fieldMapper(anyString())).thenReturn(mock(MappedFieldType.class));
            when(shardSearchRequest.indexRoutings()).thenReturn(new String[0]);

            readerContext.close();
            readerContext = new ReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                randomNonNegativeLong(),
                false
            );
            DefaultSearchContext context4 = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                null,
                bigArrays,
                null,
                timeout,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            context4.sliceBuilder(new SliceBuilder(1, 2)).parsedQuery(parsedQuery).preProcess(false);
            Query query1 = context4.query();
            context4.sliceBuilder(new SliceBuilder(0, 2)).parsedQuery(parsedQuery).preProcess(false);
            Query query2 = context4.query();
            assertTrue(query1 instanceof MatchNoDocsQuery || query2 instanceof MatchNoDocsQuery);

            readerContext.close();

            ReaderContext pitReaderContext = new PitReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                1000,
                true
            );
            DefaultSearchContext context5 = new DefaultSearchContext(
                pitReaderContext,
                shardSearchRequest,
                target,
                null,
                bigArrays,
                null,
                timeout,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            int numSlicesForPit = maxSlicesPerPit + randomIntBetween(1, 100);
            when(sliceBuilder.getMax()).thenReturn(numSlicesForPit);
            context5.sliceBuilder(sliceBuilder);

            OpenSearchRejectedExecutionException exception1 = expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> context5.preProcess(false)
            );
            assertThat(
                exception1.getMessage(),
                equalTo(
                    "The number of slices ["
                        + numSlicesForPit
                        + "] is too large. It must "
                        + "be less than ["
                        + maxSlicesPerPit
                        + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_SLICES_PER_PIT.getKey()
                        + "] index level setting."
                )
            );
            pitReaderContext.close();

            threadPool.shutdown();
        }
    }

    public void testClearQueryCancellationsOnClose() throws IOException {
        TimeValue timeout = new TimeValue(randomIntBetween(1, 100));
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);
        when(shardSearchRequest.shardId()).thenReturn(shardId);

        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = mock(IndexShard.class);
        QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        IndexService indexService = mock(IndexService.class);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(indexService.newQueryShardContext(eq(shardId.id()), any(), any(), nullable(String.class), anyBoolean(), anyBoolean()))
            .thenReturn(queryShardContext);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        try (
            Directory dir = newDirectory();
            RandomIndexWriter w = new RandomIndexWriter(random(), dir);
            IndexReader reader = w.getReader();
            Engine.Searcher searcher = new Engine.Searcher(
                "test",
                reader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(),
                reader
            )
        ) {

            Engine.SearcherSupplier searcherSupplier = new Engine.SearcherSupplier(Function.identity()) {
                @Override
                protected void doClose() {

                }

                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    return searcher;
                }
            };
            SearchShardTarget target = new SearchShardTarget("node", shardId, null, OriginalIndices.NONE);
            ReaderContext readerContext = new ReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier,
                randomNonNegativeLong(),
                false
            );

            DefaultSearchContext context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                null,
                bigArrays,
                null,
                timeout,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            assertThat(context.searcher().hasCancellations(), is(false));
            context.searcher().addQueryCancellation(() -> {});
            assertThat(context.searcher().hasCancellations(), is(true));

            context.close();
            assertThat(context.searcher().hasCancellations(), is(false));

        } finally {
            threadPool.shutdown();
        }
    }

    public void testSearchPathEvaluation() throws Exception {
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);
        when(shardSearchRequest.shardId()).thenReturn(shardId);

        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = mock(IndexShard.class);
        QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .build();

        IndexService indexService = mock(IndexService.class);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(indexService.newQueryShardContext(eq(shardId.id()), any(), any(), nullable(String.class), anyBoolean(), anyBoolean()))
            .thenReturn(queryShardContext);

        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        IndexShard systemIndexShard = mock(IndexShard.class);
        when(systemIndexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(systemIndexShard.getThreadPool()).thenReturn(threadPool);
        when(systemIndexShard.isSystem()).thenReturn(true);

        IndexShard throttledIndexShard = mock(IndexShard.class);
        when(throttledIndexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(throttledIndexShard.getThreadPool()).thenReturn(threadPool);
        IndexSettings throttledIndexSettings = new IndexSettings(
            indexMetadata,
            Settings.builder().put(INDEX_SEARCH_THROTTLED.getKey(), true).build()
        );
        when(throttledIndexShard.indexSettings()).thenReturn(throttledIndexSettings);

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            final Supplier<Engine.SearcherSupplier> searcherSupplier = () -> new Engine.SearcherSupplier(Function.identity()) {
                @Override
                protected void doClose() {}

                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    try {
                        IndexReader reader = w.getReader();
                        return new Engine.Searcher(
                            "test",
                            reader,
                            IndexSearcher.getDefaultSimilarity(),
                            IndexSearcher.getDefaultQueryCache(),
                            IndexSearcher.getDefaultQueryCachingPolicy(),
                            reader
                        );
                    } catch (IOException exc) {
                        throw new AssertionError(exc);
                    }
                }
            };

            SearchShardTarget target = new SearchShardTarget("node", shardId, null, OriginalIndices.NONE);
            ReaderContext readerContext = new ReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                randomNonNegativeLong(),
                false
            );

            final ClusterService clusterService = mock(ClusterService.class);
            final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            clusterSettings.registerSetting(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING);
            // clusterSettings.registerSetting(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE);
            clusterSettings.applySettings(
                Settings.builder().put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build()
            );
            when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
            DefaultSearchContext context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                clusterService,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );

            // Case1: if sort is on timestamp field, non-concurrent path is used
            context.sort(
                new SortAndFormats(new Sort(new SortField("@timestamp", SortField.Type.INT)), new DocValueFormat[] { DocValueFormat.RAW })
            );
            context.evaluateRequestShouldUseConcurrentSearch();
            assertFalse(context.shouldUseConcurrentSearch());
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // Case2: if sort is on other field, concurrent path is used
            context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                clusterService,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            context.sort(
                new SortAndFormats(new Sort(new SortField("test2", SortField.Type.INT)), new DocValueFormat[] { DocValueFormat.RAW })
            );
            context.evaluateRequestShouldUseConcurrentSearch();
            if (executor == null) {
                assertFalse(context.shouldUseConcurrentSearch());
            } else {
                assertTrue(context.shouldUseConcurrentSearch());
            }
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // Case 3: With no sort, concurrent path is used
            context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                clusterService,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            context.evaluateRequestShouldUseConcurrentSearch();
            if (executor == null) {
                assertFalse(context.shouldUseConcurrentSearch());
            } else {
                assertTrue(context.shouldUseConcurrentSearch());
            }
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // Case 4: With a system index concurrent segment search is not used
            readerContext = new ReaderContext(
                newContextId(),
                indexService,
                systemIndexShard,
                searcherSupplier.get(),
                randomNonNegativeLong(),
                false
            );
            context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                null,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            context.evaluateRequestShouldUseConcurrentSearch();
            assertFalse(context.shouldUseConcurrentSearch());
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // Case 5: When search is throttled concurrent segment search is not used
            readerContext = new ReaderContext(
                newContextId(),
                indexService,
                throttledIndexShard,
                searcherSupplier.get(),
                randomNonNegativeLong(),
                false
            );
            context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                null,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            context.evaluateRequestShouldUseConcurrentSearch();
            assertFalse(context.shouldUseConcurrentSearch());
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            if (clusterService.getClusterSettings().get(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING)) {
                assertSettingDeprecationsAndWarnings(new Setting[] { SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING });
            }

            // shutdown the threadpool
            threadPool.shutdown();
        }
    }

    public void testSearchPathEvaluationWithConcurrentSearchModeAsAuto() throws Exception {
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);
        when(shardSearchRequest.shardId()).thenReturn(shardId);

        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = mock(IndexShard.class);
        QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .build();

        IndexService indexService = mock(IndexService.class);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(indexService.newQueryShardContext(eq(shardId.id()), any(), any(), nullable(String.class), anyBoolean(), anyBoolean()))
            .thenReturn(queryShardContext);

        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        IndexShard systemIndexShard = mock(IndexShard.class);
        when(systemIndexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(systemIndexShard.getThreadPool()).thenReturn(threadPool);
        when(systemIndexShard.isSystem()).thenReturn(true);

        IndexShard throttledIndexShard = mock(IndexShard.class);
        when(throttledIndexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(throttledIndexShard.getThreadPool()).thenReturn(threadPool);
        IndexSettings throttledIndexSettings = new IndexSettings(
            indexMetadata,
            Settings.builder().put(INDEX_SEARCH_THROTTLED.getKey(), true).build()
        );
        when(throttledIndexShard.indexSettings()).thenReturn(throttledIndexSettings);

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            final Supplier<Engine.SearcherSupplier> searcherSupplier = () -> new Engine.SearcherSupplier(Function.identity()) {
                @Override
                protected void doClose() {}

                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    try {
                        IndexReader reader = w.getReader();
                        return new Engine.Searcher(
                            "test",
                            reader,
                            IndexSearcher.getDefaultSimilarity(),
                            IndexSearcher.getDefaultQueryCache(),
                            IndexSearcher.getDefaultQueryCachingPolicy(),
                            reader
                        );
                    } catch (IOException exc) {
                        throw new AssertionError(exc);
                    }
                }
            };

            SearchShardTarget target = new SearchShardTarget("node", shardId, null, OriginalIndices.NONE);
            ReaderContext readerContext = new ReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                randomNonNegativeLong(),
                false
            );

            final ClusterService clusterService = mock(ClusterService.class);
            final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            clusterSettings.registerSetting(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING);
            clusterSettings.registerSetting(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE);
            clusterSettings.applySettings(
                Settings.builder().put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), "auto").build()
            );
            when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
            when(clusterService.getSettings()).thenReturn(settings);

            DefaultSearchContext context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                clusterService,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );

            // Case1: if there is no agg in the query, non-concurrent path is used
            context.evaluateRequestShouldUseConcurrentSearch();
            assertFalse(context.shouldUseConcurrentSearch());
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // Case2: if un supported agg present, non-concurrent path is used
            SearchContextAggregations mockAggregations = mock(SearchContextAggregations.class);
            when(mockAggregations.factories()).thenReturn(mock(AggregatorFactories.class));
            when(mockAggregations.factories().allFactoriesSupportConcurrentSearch()).thenReturn(false);
            when(mockAggregations.multiBucketConsumer()).thenReturn(mock(MultiBucketConsumerService.MultiBucketConsumer.class));

            context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                clusterService,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );

            // add un-supported agg operation
            context.aggregations(mockAggregations);
            context.evaluateRequestShouldUseConcurrentSearch();
            if (executor == null) {
                assertFalse(context.shouldUseConcurrentSearch());
            } else {
                assertFalse(context.shouldUseConcurrentSearch());
            }
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // Case3: if supported agg present, concurrent path is used

            // set agg operation to be supported
            when(mockAggregations.factories().allFactoriesSupportConcurrentSearch()).thenReturn(true);

            context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                clusterService,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                Collections.emptyList()
            );
            // create a supported agg operation
            context.aggregations(mockAggregations);
            context.evaluateRequestShouldUseConcurrentSearch();
            if (executor == null) {
                assertFalse(context.shouldUseConcurrentSearch());
            } else {
                assertTrue(context.shouldUseConcurrentSearch());
            }
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // Case4: multiple deciders are registered and all of them opt out of decision-making
            // with supported agg query so concurrent path is used

            ConcurrentSearchRequestDecider decider1 = mock(ConcurrentSearchRequestDecider.class);

            ConcurrentSearchRequestDecider decider2 = mock(ConcurrentSearchRequestDecider.class);

            ConcurrentSearchRequestDecider.Factory factory1 = new ConcurrentSearchRequestDecider.Factory() {
                @Override
                public Optional<ConcurrentSearchRequestDecider> create(IndexSettings indexSettings) {
                    return Optional.ofNullable(decider1);
                }
            };

            ConcurrentSearchRequestDecider.Factory factory2 = new ConcurrentSearchRequestDecider.Factory() {
                @Override
                public Optional<ConcurrentSearchRequestDecider> create(IndexSettings indexSettings) {
                    return Optional.ofNullable(decider2);
                }
            };
            ConcurrentSearchRequestDecider.Factory factory3 = new ConcurrentSearchRequestDecider.Factory() {
                @Override
                public Optional<ConcurrentSearchRequestDecider> create(IndexSettings indexSettings) {
                    return Optional.empty();
                }
            };

            List<ConcurrentSearchRequestDecider.Factory> concurrentSearchRequestDeciders = new ArrayList<>();
            concurrentSearchRequestDeciders.add(factory1);
            concurrentSearchRequestDeciders.add(factory2);
            concurrentSearchRequestDeciders.add(factory3);

            context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                clusterService,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                concurrentSearchRequestDeciders
            );
            // create a supported agg operation
            context.aggregations(mockAggregations);
            context.evaluateRequestShouldUseConcurrentSearch();
            if (executor == null) {
                assertFalse(context.shouldUseConcurrentSearch());
            } else {
                assertTrue(context.shouldUseConcurrentSearch());
            }
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // Case5: multiple deciders are registered and one of them returns ConcurrentSearchDecision.DecisionStatus.NO
            // use non-concurrent path even if query contains supported agg
            when(decider1.getConcurrentSearchDecision()).thenReturn(
                new ConcurrentSearchDecision(ConcurrentSearchDecision.DecisionStatus.NO, "disable concurrent search")
            );

            // create a source so that query tree is parsed by visitor
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            sourceBuilder.query(queryBuilder);
            when(shardSearchRequest.source()).thenReturn(sourceBuilder);

            context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                clusterService,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                concurrentSearchRequestDeciders
            );

            // create a supported agg operation
            context.aggregations(mockAggregations);
            context.evaluateRequestShouldUseConcurrentSearch();
            if (executor == null) {
                assertFalse(context.shouldUseConcurrentSearch());
            } else {
                assertFalse(context.shouldUseConcurrentSearch());
            }
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // Case6: multiple deciders are registered and first decider returns ConcurrentSearchDecision.DecisionStatus.YES
            // while second decider returns ConcurrentSearchDecision.DecisionStatus.NO
            // use non-concurrent path even if query contains supported agg

            when(decider1.getConcurrentSearchDecision()).thenReturn(
                new ConcurrentSearchDecision(ConcurrentSearchDecision.DecisionStatus.YES, "enable concurrent search")
            );

            when(decider2.getConcurrentSearchDecision()).thenReturn(
                new ConcurrentSearchDecision(ConcurrentSearchDecision.DecisionStatus.NO, "disable concurrent search")
            );

            // create a source so that query tree is parsed by visitor
            when(shardSearchRequest.source()).thenReturn(sourceBuilder);

            context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                clusterService,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                concurrentSearchRequestDeciders
            );

            // create a supported agg operation
            context.aggregations(mockAggregations);
            context.evaluateRequestShouldUseConcurrentSearch();
            if (executor == null) {
                assertFalse(context.shouldUseConcurrentSearch());
            } else {
                assertFalse(context.shouldUseConcurrentSearch());
            }
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // Case7: multiple deciders are registered and all return ConcurrentSearchDecision.DecisionStatus.NO_OP
            // but un-supported agg query is present, use non-concurrent path

            when(decider1.getConcurrentSearchDecision()).thenReturn(
                new ConcurrentSearchDecision(ConcurrentSearchDecision.DecisionStatus.NO_OP, "noop")
            );

            when(decider2.getConcurrentSearchDecision()).thenReturn(
                new ConcurrentSearchDecision(ConcurrentSearchDecision.DecisionStatus.NO_OP, "noop")
            );

            when(mockAggregations.factories().allFactoriesSupportConcurrentSearch()).thenReturn(false);

            // create a source so that query tree is parsed by visitor
            when(shardSearchRequest.source()).thenReturn(sourceBuilder);

            context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                clusterService,
                bigArrays,
                null,
                null,
                null,
                false,
                Version.CURRENT,
                false,
                executor,
                null,
                concurrentSearchRequestDeciders
            );

            // create a supported agg operation
            context.aggregations(mockAggregations);
            context.evaluateRequestShouldUseConcurrentSearch();
            if (executor == null) {
                assertFalse(context.shouldUseConcurrentSearch());
            } else {
                assertFalse(context.shouldUseConcurrentSearch());
            }
            assertThrows(SetOnce.AlreadySetException.class, context::evaluateRequestShouldUseConcurrentSearch);

            // shutdown the threadpool
            threadPool.shutdown();
        }
    }

    private ShardSearchContextId newContextId() {
        return new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.StreamingSearchMode;
import org.opensearch.search.streaming.FlushMode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for DefaultSearchContext streaming flag propagation.
 * Validates that streaming flags, mode, and flush mode are correctly set when streaming search is requested.
 */
public class DefaultSearchContextStreamingTests extends OpenSearchTestCase {

    public void testStreamingFlagsSetWhenStreamingRequested() throws Exception {
        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.setStreamingScoring(true);
            searchRequest.setStreamingSearchMode(StreamingSearchMode.NO_SCORING.toString());

            ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
            when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
            when(shardSearchRequest.source()).thenReturn(searchRequest.source());
            when(shardSearchRequest.getStreamingSearchMode()).thenReturn(StreamingSearchMode.NO_SCORING.toString());

            ShardId shardId = new ShardId("test-index", UUID.randomUUID().toString(), 0);
            when(shardSearchRequest.shardId()).thenReturn(shardId);

            IndexShard indexShard = mock(IndexShard.class);
            QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
            when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
            when(indexShard.getThreadPool()).thenReturn(threadPool);

            org.opensearch.cluster.metadata.IndexMetadata indexMetadata = org.opensearch.cluster.metadata.IndexMetadata.builder(
                "test-index"
            )
                .settings(
                    Settings.builder()
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .build();
            org.opensearch.index.IndexSettings indexSettings = new org.opensearch.index.IndexSettings(indexMetadata, Settings.EMPTY);
            when(indexShard.indexSettings()).thenReturn(indexSettings);

            IndexService indexService = mock(IndexService.class);
            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

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

            SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
            ReaderContext readerContext = new ReaderContext(
                new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong()),
                indexService,
                indexShard,
                searcherSupplier.get(),
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
                null,
                null,
                false,
                Version.CURRENT,
                false,
                null,
                null,
                Collections.emptyList()
            );

            assertTrue(context.isStreamingSearch());
            assertEquals(StreamingSearchMode.NO_SCORING, context.getStreamingMode());
            assertEquals(FlushMode.PER_SEGMENT, context.getFlushMode());

            context.close();
        } finally {
            threadPool.shutdown();
        }
    }

    public void testStreamingFlagsScoredUnsortedMode() throws Exception {
        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
            when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
            when(shardSearchRequest.getStreamingSearchMode()).thenReturn(StreamingSearchMode.SCORED_UNSORTED.toString());

            ShardId shardId = new ShardId("test-index", UUID.randomUUID().toString(), 0);
            when(shardSearchRequest.shardId()).thenReturn(shardId);

            IndexShard indexShard = mock(IndexShard.class);
            QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
            when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
            when(indexShard.getThreadPool()).thenReturn(threadPool);

            org.opensearch.cluster.metadata.IndexMetadata indexMetadata = org.opensearch.cluster.metadata.IndexMetadata.builder(
                "test-index"
            )
                .settings(
                    Settings.builder()
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .build();
            org.opensearch.index.IndexSettings indexSettings = new org.opensearch.index.IndexSettings(indexMetadata, Settings.EMPTY);
            when(indexShard.indexSettings()).thenReturn(indexSettings);

            IndexService indexService = mock(IndexService.class);
            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

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

            SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
            ReaderContext readerContext = new ReaderContext(
                new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong()),
                indexService,
                indexShard,
                searcherSupplier.get(),
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
                null,
                null,
                false,
                Version.CURRENT,
                false,
                null,
                null,
                Collections.emptyList()
            );

            assertTrue(context.isStreamingSearch());
            assertEquals(StreamingSearchMode.SCORED_UNSORTED, context.getStreamingMode());
            assertEquals(FlushMode.PER_SEGMENT, context.getFlushMode());

            context.close();
        } finally {
            threadPool.shutdown();
        }
    }

    public void testNonStreamingDoesNotSetStreamingFlags() throws Exception {
        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
            when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
            when(shardSearchRequest.getStreamingSearchMode()).thenReturn(null);

            ShardId shardId = new ShardId("test-index", UUID.randomUUID().toString(), 0);
            when(shardSearchRequest.shardId()).thenReturn(shardId);

            IndexShard indexShard = mock(IndexShard.class);
            QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
            when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
            when(indexShard.getThreadPool()).thenReturn(threadPool);

            org.opensearch.cluster.metadata.IndexMetadata indexMetadata = org.opensearch.cluster.metadata.IndexMetadata.builder(
                "test-index"
            )
                .settings(
                    Settings.builder()
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .build();
            org.opensearch.index.IndexSettings indexSettings = new org.opensearch.index.IndexSettings(indexMetadata, Settings.EMPTY);
            when(indexShard.indexSettings()).thenReturn(indexSettings);

            IndexService indexService = mock(IndexService.class);
            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

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

            SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
            ReaderContext readerContext = new ReaderContext(
                new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong()),
                indexService,
                indexShard,
                searcherSupplier.get(),
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
                null,
                null,
                false,
                Version.CURRENT,
                false,
                null,
                null,
                Collections.emptyList()
            );

            assertFalse(context.isStreamingSearch());
            assertNull(context.getStreamingMode());

            context.close();
        } finally {
            threadPool.shutdown();
        }
    }
}

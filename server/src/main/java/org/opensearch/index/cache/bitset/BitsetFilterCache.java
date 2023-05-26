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

package org.opensearch.index.cache.bitset;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexWarmer;
import org.opensearch.index.IndexWarmer.TerminationHandle;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardUtils;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * This is a cache for {@link BitDocIdSet} based filters and is unbounded by size or time.
 * <p>
 * Use this cache with care, only components that require that a filter is to be materialized as a {@link BitDocIdSet}
 * and require that it should always be around should use this cache, otherwise the
 * {@link org.opensearch.index.cache.query.QueryCache} should be used instead.
 *
 * @opensearch.internal
 */
public final class BitsetFilterCache extends AbstractIndexComponent
    implements
        IndexReader.ClosedListener,
        RemovalListener<IndexReader.CacheKey, Cache<Query, BitsetFilterCache.Value>>,
        Closeable {

    public static final Setting<Boolean> INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING = Setting.boolSetting(
        "index.load_fixed_bitset_filters_eagerly",
        true,
        Property.IndexScope
    );

    private final boolean loadRandomAccessFiltersEagerly;
    private final Cache<IndexReader.CacheKey, Cache<Query, Value>> loadedFilters;
    private final Listener listener;

    public BitsetFilterCache(IndexSettings indexSettings, Listener listener) {
        super(indexSettings);
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        this.loadRandomAccessFiltersEagerly = this.indexSettings.getValue(INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING);
        this.loadedFilters = CacheBuilder.<IndexReader.CacheKey, Cache<Query, Value>>builder().removalListener(this).build();
        this.listener = listener;
    }

    public static BitSet bitsetFromQuery(Query query, LeafReaderContext context) throws IOException {
        final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
        final IndexSearcher searcher = new IndexSearcher(topLevelContext);
        searcher.setQueryCache(null);
        final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        Scorer s = weight.scorer(context);
        if (s == null) {
            return null;
        } else {
            return BitSet.of(s.iterator(), context.reader().maxDoc());
        }
    }

    public IndexWarmer.Listener createListener(ThreadPool threadPool) {
        return new BitSetProducerWarmer(threadPool);
    }

    public BitSetProducer getBitSetProducer(Query query) {
        return new QueryWrapperBitSetProducer(query);
    }

    @Override
    public void onClose(IndexReader.CacheKey ownerCoreCacheKey) {
        loadedFilters.invalidate(ownerCoreCacheKey);
    }

    @Override
    public void close() {
        clear("close");
    }

    public void clear(String reason) {
        logger.debug("clearing all bitsets because [{}]", reason);
        loadedFilters.invalidateAll();
    }

    private BitSet getAndLoadIfNotPresent(final Query query, final LeafReaderContext context) throws ExecutionException {
        final IndexReader.CacheHelper cacheHelper = FilterLeafReader.unwrap(context.reader()).getCoreCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + context.reader() + " does not support caching");
        }
        final IndexReader.CacheKey coreCacheReader = cacheHelper.getKey();
        final ShardId shardId = ShardUtils.extractShardId(context.reader());
        if (indexSettings.getIndex().equals(shardId.getIndex()) == false) {
            // insanity
            throw new IllegalStateException(
                "Trying to load bit set for index " + shardId.getIndex() + " with cache of index " + indexSettings.getIndex()
            );
        }
        Cache<Query, Value> filterToFbs = loadedFilters.computeIfAbsent(coreCacheReader, key -> {
            cacheHelper.addClosedListener(BitsetFilterCache.this);
            return CacheBuilder.<Query, Value>builder().build();
        });

        return filterToFbs.computeIfAbsent(query, key -> {
            final BitSet bitSet = bitsetFromQuery(query, context);
            Value value = new Value(bitSet, shardId);
            listener.onCache(shardId, value.bitset);
            return value;
        }).bitset;
    }

    @Override
    public void onRemoval(RemovalNotification<IndexReader.CacheKey, Cache<Query, Value>> notification) {
        if (notification.getKey() == null) {
            return;
        }

        Cache<Query, Value> valueCache = notification.getValue();
        if (valueCache == null) {
            return;
        }

        for (Value value : valueCache.values()) {
            listener.onRemoval(value.shardId, value.bitset);
            // if null then this means the shard has already been removed and the stats are 0 anyway for the shard this key belongs to
        }
    }

    /**
     * Value for bitset filter cache
     *
     * @opensearch.internal
     */
    public static final class Value {

        final BitSet bitset;
        final ShardId shardId;

        public Value(BitSet bitset, ShardId shardId) {
            this.bitset = bitset;
            this.shardId = shardId;
        }
    }

    final class QueryWrapperBitSetProducer implements BitSetProducer {

        final Query query;

        QueryWrapperBitSetProducer(Query query) {
            this.query = Objects.requireNonNull(query);
        }

        // TODO: convertToElastic might need to be renamed
        @Override
        public BitSet getBitSet(LeafReaderContext context) throws IOException {
            try {
                return getAndLoadIfNotPresent(query, context);
            } catch (ExecutionException e) {
                throw ExceptionsHelper.convertToOpenSearchException(e);
            }
        }

        @Override
        public String toString() {
            return "random_access(" + query + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof QueryWrapperBitSetProducer)) return false;
            return this.query.equals(((QueryWrapperBitSetProducer) o).query);
        }

        @Override
        public int hashCode() {
            return 31 * getClass().hashCode() + query.hashCode();
        }
    }

    final class BitSetProducerWarmer implements IndexWarmer.Listener {

        private final Executor executor;

        BitSetProducerWarmer(ThreadPool threadPool) {
            this.executor = threadPool.executor(ThreadPool.Names.WARMER);
        }

        @Override
        public IndexWarmer.TerminationHandle warmReader(final IndexShard indexShard, final OpenSearchDirectoryReader reader) {
            if (indexSettings.getIndex().equals(indexShard.indexSettings().getIndex()) == false) {
                // this is from a different index
                return TerminationHandle.NO_WAIT;
            }

            if (!loadRandomAccessFiltersEagerly) {
                return TerminationHandle.NO_WAIT;
            }

            boolean hasNested = false;
            final Set<Query> warmUp = new HashSet<>();
            final MapperService mapperService = indexShard.mapperService();
            DocumentMapper docMapper = mapperService.documentMapper();
            if (docMapper != null) {
                if (docMapper.hasNestedObjects()) {
                    hasNested = true;
                    for (ObjectMapper objectMapper : docMapper.objectMappers().values()) {
                        if (objectMapper.nested().isNested()) {
                            ObjectMapper parentObjectMapper = objectMapper.getParentObjectMapper(mapperService);
                            if (parentObjectMapper != null && parentObjectMapper.nested().isNested()) {
                                warmUp.add(parentObjectMapper.nestedTypeFilter());
                            }
                        }
                    }
                }
            }

            if (hasNested) {
                warmUp.add(Queries.newNonNestedFilter());
            }

            final CountDownLatch latch = new CountDownLatch(reader.leaves().size() * warmUp.size());
            for (final LeafReaderContext ctx : reader.leaves()) {
                for (final Query filterToWarm : warmUp) {
                    executor.execute(() -> {
                        try {
                            final long start = System.nanoTime();
                            getAndLoadIfNotPresent(filterToWarm, ctx);
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService()
                                    .logger()
                                    .trace(
                                        "warmed bitset for [{}], took [{}]",
                                        filterToWarm,
                                        TimeValue.timeValueNanos(System.nanoTime() - start)
                                    );
                            }
                        } catch (Exception e) {
                            indexShard.warmerService()
                                .logger()
                                .warn(() -> new ParameterizedMessage("failed to load " + "bitset for [{}]", filterToWarm), e);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
            }
            return () -> latch.await();
        }

    }

    Cache<IndexReader.CacheKey, Cache<Query, Value>> getLoadedFilters() {
        return loadedFilters;
    }

    /**
     *  A listener interface that is executed for each onCache / onRemoval event
     *
     * @opensearch.internal
     */
    public interface Listener {
        /**
         * Called for each cached bitset on the cache event.
         * @param shardId the shard id the bitset was cached for. This can be <code>null</code>
         * @param accountable the bitsets ram representation
         */
        void onCache(ShardId shardId, Accountable accountable);

        /**
         * Called for each cached bitset on the removal event.
         * @param shardId the shard id the bitset was cached for. This can be <code>null</code>
         * @param accountable the bitsets ram representation
         */
        void onRemoval(ShardId shardId, Accountable accountable);
    }
}

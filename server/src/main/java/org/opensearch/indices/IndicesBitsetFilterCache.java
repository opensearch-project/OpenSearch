/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.apache.lucene.util.BitSet;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexWarmer;
import org.opensearch.index.IndexWarmer.TerminationHandle;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.shard.IndexShard;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToLongBiFunction;

/**
 * Node-level cache for {@link BitSet} based filters. Manages a single flat cache shared across
 * all indices on the node, with a configurable size limit and async stale entry cleanup.
 *
 * @opensearch.api
 */
@ExperimentalApi
public class IndicesBitsetFilterCache
    implements
        IndexReader.ClosedListener,
        RemovalListener<IndicesBitsetFilterCache.BitsetCacheKey, IndicesBitsetFilterCache.Value>,
        Closeable {

    private static final Logger logger = LogManager.getLogger(IndicesBitsetFilterCache.class);

    public static final Setting<Boolean> INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING = Setting.boolSetting(
        "index.load_fixed_bitset_filters_eagerly",
        true,
        Property.IndexScope
    );

    public static final Setting<ByteSizeValue> INDICES_BITSET_FILTER_CACHE_SIZE_SETTING = Setting.memorySizeSetting(
        "indices.cache.bitset.size",
        "5%",
        Property.NodeScope
    );

    public static final Setting<TimeValue> INDICES_BITSET_FILTER_CACHE_CLEAN_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "indices.cache.bitset.cleanup_interval",
        TimeValue.timeValueSeconds(60),
        Property.NodeScope
    );

    private final Cache<BitsetCacheKey, Value> cache;
    private final Set<IndexReader.CacheKey> staleCacheKeys = ConcurrentCollections.newConcurrentSet();
    private final Set<IndexReader.CacheKey> registeredKeys = ConcurrentCollections.newConcurrentSet();
    private final BitsetCacheCleaner cacheCleaner;

    public IndicesBitsetFilterCache(Settings settings, ThreadPool threadPool) {
        long sizeInBytes = INDICES_BITSET_FILTER_CACHE_SIZE_SETTING.get(settings).getBytes();
        CacheBuilder<BitsetCacheKey, Value> cacheBuilder = CacheBuilder.<BitsetCacheKey, Value>builder().removalListener(this);
        if (sizeInBytes > 0) {
            cacheBuilder.setMaximumWeight(sizeInBytes).weigher(new BitsetWeigher());
        }
        this.cache = cacheBuilder.build();

        TimeValue cleanInterval = INDICES_BITSET_FILTER_CACHE_CLEAN_INTERVAL_SETTING.get(settings);
        this.cacheCleaner = new BitsetCacheCleaner(this, threadPool, cleanInterval);
        threadPool.schedule(cacheCleaner, cleanInterval, ThreadPool.Names.SAME);
    }

    public BitSetProducer getBitSetProducer(Query query, BitsetFilterCache.Listener listener) {
        return new QueryWrapperBitSetProducer(query, listener);
    }

    public IndexWarmer.Listener createListener(ThreadPool threadPool) {
        return new BitSetProducerWarmer(threadPool);
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

    BitSet getAndLoadIfNotPresent(final Query query, final LeafReaderContext context, final BitsetFilterCache.Listener listener)
        throws ExecutionException {
        final IndexReader.CacheHelper cacheHelper = FilterLeafReader.unwrap(context.reader()).getCoreCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + context.reader() + " does not support caching");
        }
        final IndexReader.CacheKey coreCacheReader = cacheHelper.getKey();
        final ShardId shardId = ShardUtils.extractShardId(context.reader());

        if (registeredKeys.add(coreCacheReader)) {
            cacheHelper.addClosedListener(this);
        }

        final BitsetCacheKey cacheKey = new BitsetCacheKey(coreCacheReader, query);
        return cache.computeIfAbsent(cacheKey, key -> {
            final BitSet bitSet = bitsetFromQuery(query, context);
            Value value = new Value(bitSet, shardId, listener);
            listener.onCache(shardId, value.bitset);
            return value;
        }).bitset;
    }

    @Override
    public void onClose(IndexReader.CacheKey ownerCoreCacheKey) {
        staleCacheKeys.add(ownerCoreCacheKey);
    }

    @Override
    public void close() {
        cacheCleaner.close();
        clear();
    }

    public void clear() {
        cache.invalidateAll();
        staleCacheKeys.clear();
        registeredKeys.clear();
    }

    @Override
    public void onRemoval(RemovalNotification<BitsetCacheKey, Value> notification) {
        Value value = notification.getValue();
        if (value == null || value.listener == null) {
            return;
        }
        value.listener.onRemoval(value.shardId, value.bitset);
    }

    public void purgeStaleEntries() {
        if (staleCacheKeys.isEmpty()) {
            return;
        }
        Set<IndexReader.CacheKey> staleSnapshot = new HashSet<>(staleCacheKeys);
        staleCacheKeys.removeAll(staleSnapshot);
        registeredKeys.removeAll(staleSnapshot);

        for (BitsetCacheKey key : cache.keys()) {
            if (staleSnapshot.contains(key.readerCacheKey)) {
                cache.invalidate(key);
            }
        }
    }

    public Cache<BitsetCacheKey, Value> getCache() {
        return cache;
    }

    /**
     * Composite key combining a reader segment key with a query.
     *
     * @opensearch.internal
     */
    @ExperimentalApi
    public static final class BitsetCacheKey {
        final IndexReader.CacheKey readerCacheKey;
        final Query query;

        public BitsetCacheKey(IndexReader.CacheKey readerCacheKey, Query query) {
            this.readerCacheKey = Objects.requireNonNull(readerCacheKey);
            this.query = Objects.requireNonNull(query);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BitsetCacheKey other)) return false;
            return readerCacheKey == other.readerCacheKey && query.equals(other.query);
        }

        @Override
        public int hashCode() {
            return 31 * System.identityHashCode(readerCacheKey) + query.hashCode();
        }
    }

    @ExperimentalApi
    public static final class Value {
        final BitSet bitset;
        final ShardId shardId;
        final BitsetFilterCache.Listener listener;

        Value(BitSet bitset, ShardId shardId, BitsetFilterCache.Listener listener) {
            this.bitset = bitset;
            this.shardId = shardId;
            this.listener = listener;
        }
    }

    static class BitsetWeigher implements ToLongBiFunction<BitsetCacheKey, Value> {
        @Override
        public long applyAsLong(BitsetCacheKey key, Value value) {
            long weight = (value.bitset != null) ? value.bitset.ramBytesUsed() : 0;
            return weight == 0 ? 1 : weight;
        }
    }

    final class QueryWrapperBitSetProducer implements BitSetProducer {
        final Query query;
        final BitsetFilterCache.Listener listener;

        QueryWrapperBitSetProducer(Query query, BitsetFilterCache.Listener listener) {
            this.query = Objects.requireNonNull(query);
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public BitSet getBitSet(LeafReaderContext context) throws IOException {
            try {
                return getAndLoadIfNotPresent(query, context, listener);
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
            if (!(o instanceof QueryWrapperBitSetProducer other)) return false;
            return this.query.equals(other.query);
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
            if (!indexShard.indexSettings().getValue(INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING)) {
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

            // Build a listener that routes stats to the correct shard.
            final BitsetFilterCache.Listener listener = new BitsetFilterCache.Listener() {
                @Override
                public void onCache(ShardId shardId, Accountable accountable) {
                    if (shardId != null && accountable != null) {
                        indexShard.shardBitsetFilterCache().onCached(accountable.ramBytesUsed());
                    }
                }

                @Override
                public void onRemoval(ShardId shardId, Accountable accountable) {
                    if (shardId != null && accountable != null) {
                        indexShard.shardBitsetFilterCache().onRemoval(accountable.ramBytesUsed());
                    }
                }
            };

            final CountDownLatch latch = new CountDownLatch(reader.leaves().size() * warmUp.size());
            for (final LeafReaderContext ctx : reader.leaves()) {
                for (final Query filterToWarm : warmUp) {
                    executor.execute(() -> {
                        try {
                            final long start = System.nanoTime();
                            getAndLoadIfNotPresent(filterToWarm, ctx, listener);
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
                                .warn(() -> new ParameterizedMessage("failed to load bitset for [{}]", filterToWarm), e);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
            }
            return () -> latch.await();
        }
    }

    private static final class BitsetCacheCleaner implements Runnable, Releasable {
        private final IndicesBitsetFilterCache cache;
        private final ThreadPool threadPool;
        private final TimeValue interval;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        BitsetCacheCleaner(IndicesBitsetFilterCache cache, ThreadPool threadPool, TimeValue interval) {
            this.cache = cache;
            this.threadPool = threadPool;
            this.interval = interval;
        }

        @Override
        public void run() {
            try {
                cache.purgeStaleEntries();
            } catch (Exception e) {
                logger.warn("Exception during periodic bitset filter cache cleanup:", e);
            }
            if (closed.get() == false) {
                threadPool.scheduleUnlessShuttingDown(interval, ThreadPool.Names.SAME, this);
            }
        }

        @Override
        public void close() {
            closed.compareAndSet(false, true);
        }
    }
}

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

package org.opensearch.indices.fielddata.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.LeafFieldData;
import org.opensearch.index.shard.ShardUtils;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.ToLongBiFunction;

/**
 * The field data cache for multiple indices
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndicesFieldDataCache implements RemovalListener<IndicesFieldDataCache.Key, Accountable>, Releasable {

    private static final Logger logger = LogManager.getLogger(IndicesFieldDataCache.class);

    public static final RatioValue MAX_SIZE_PERCENTAGE = new RatioValue(35);
    /**
     * The size after which the cache will begin to evict entries.
     */
    public static final Setting<ByteSizeValue> INDICES_FIELDDATA_CACHE_SIZE_KEY = Setting.memorySizeSetting(
        "indices.fielddata.cache.size",
        MAX_SIZE_PERCENTAGE.toString(),
        Property.NodeScope,
        Property.Dynamic
    );
    private final IndexFieldDataCache.Listener indicesFieldDataCacheListener;
    private final Cache<Key, Accountable> cache;
    private final ThreadPool threadPool;

    /**
     * Deprecated ctor. Use the ctor with the clusterService argument.
     */
    @Deprecated
    public IndicesFieldDataCache(Settings settings, IndexFieldDataCache.Listener indicesFieldDataCacheListener) {
        this(settings, indicesFieldDataCacheListener, null, null);
    }

    public IndicesFieldDataCache(
        Settings settings,
        IndexFieldDataCache.Listener indicesFieldDataCacheListener,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this.indicesFieldDataCacheListener = indicesFieldDataCacheListener;
        final long sizeInBytes = INDICES_FIELDDATA_CACHE_SIZE_KEY.get(settings).getBytes();
        CacheBuilder<Key, Accountable> cacheBuilder = CacheBuilder.<Key, Accountable>builder().removalListener(this);
        if (sizeInBytes > 0) {
            cacheBuilder.setMaximumWeight(sizeInBytes).weigher(new FieldDataWeigher());
        }
        cache = cacheBuilder.build();
        if (clusterService != null) {
            clusterService.getClusterSettings().addSettingsUpdateConsumer(INDICES_FIELDDATA_CACHE_SIZE_KEY, this::updateMaximumWeight);
        } else {
            logger.warn(
                "IndicesFieldDataCache ctor got null clusterService argument! Cluster setting updates for cache size will not work!"
            );
        }
        this.threadPool = threadPool;
        if (threadPool == null) {
            logger.warn("IndicesFieldDataCache ctor got null threadPool! Evictions on cache resize will happen on cluster applier thread!");
        }
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }

    public IndexFieldDataCache buildIndexFieldDataCache(IndexFieldDataCache.Listener listener, Index index, String fieldName) {
        return new IndexFieldCache(logger, cache, index, fieldName, indicesFieldDataCacheListener, listener);
    }

    public Cache<Key, Accountable> getCache() {
        return cache;
    }

    @Override
    public void onRemoval(RemovalNotification<Key, Accountable> notification) {
        Key key = notification.getKey();
        assert key != null && key.listeners != null;
        IndexFieldCache indexCache = key.indexCache;
        final Accountable value = notification.getValue();
        for (IndexFieldDataCache.Listener listener : key.listeners) {
            try {
                listener.onRemoval(
                    key.shardId,
                    indexCache.fieldName,
                    notification.getRemovalReason() == RemovalReason.EVICTED,
                    value.ramBytesUsed()
                );
            } catch (Exception e) {
                // load anyway since listeners should not throw exceptions
                logger.error("Failed to call listener on field data cache unloading", e);
            }
        }
    }

    /**
     * Computes a weight based on ramBytesUsed
     *
     * @opensearch.internal
     */
    public static class FieldDataWeigher implements ToLongBiFunction<Key, Accountable> {
        @Override
        public long applyAsLong(Key key, Accountable ramUsage) {
            int weight = (int) Math.min(ramUsage.ramBytesUsed(), Integer.MAX_VALUE);
            return weight == 0 ? 1 : weight;
        }
    }

    /**
     * A specific cache instance for the relevant parameters of it (index, fieldNames, fieldType).
     *
     * @opensearch.internal
     */
    static class IndexFieldCache implements IndexFieldDataCache, IndexReader.ClosedListener {
        private final Logger logger;
        final Index index;
        final String fieldName;
        private final Cache<Key, Accountable> cache;
        private final Listener[] listeners;

        IndexFieldCache(Logger logger, final Cache<Key, Accountable> cache, Index index, String fieldName, Listener... listeners) {
            this.logger = logger;
            this.listeners = listeners;
            this.index = index;
            this.fieldName = fieldName;
            this.cache = cache;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(final LeafReaderContext context, final IFD indexFieldData)
            throws Exception {
            final ShardId shardId = ShardUtils.extractShardId(context.reader());
            final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                throw new IllegalArgumentException("Reader " + context.reader() + " does not support caching");
            }
            final Key key = new Key(this, cacheHelper.getKey(), shardId);
            // noinspection unchecked
            final Accountable accountable = cache.computeIfAbsent(key, k -> {
                cacheHelper.addClosedListener(IndexFieldCache.this);
                Collections.addAll(k.listeners, this.listeners);
                final LeafFieldData fieldData = indexFieldData.loadDirect(context);
                for (Listener listener : k.listeners) {
                    try {
                        listener.onCache(shardId, fieldName, fieldData);
                    } catch (Exception e) {
                        // load anyway since listeners should not throw exceptions
                        logger.error("Failed to call listener on atomic field data loading", e);
                    }
                }
                return fieldData;
            });
            return (FD) accountable;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(
            final DirectoryReader indexReader,
            final IFD indexFieldData
        ) throws Exception {
            final ShardId shardId = ShardUtils.extractShardId(indexReader);
            final IndexReader.CacheHelper cacheHelper = indexReader.getReaderCacheHelper();
            if (cacheHelper == null) {
                throw new IllegalArgumentException("Reader " + indexReader + " does not support caching");
            }
            final Key key = new Key(this, cacheHelper.getKey(), shardId);
            // noinspection unchecked
            final Accountable accountable = cache.computeIfAbsent(key, k -> {
                OpenSearchDirectoryReader.addReaderCloseListener(indexReader, IndexFieldCache.this);
                Collections.addAll(k.listeners, this.listeners);
                final Accountable ifd = (Accountable) indexFieldData.loadGlobalDirect(indexReader);
                for (Listener listener : k.listeners) {
                    try {
                        listener.onCache(shardId, fieldName, ifd);
                    } catch (Exception e) {
                        // load anyway since listeners should not throw exceptions
                        logger.error("Failed to call listener on global ordinals loading", e);
                    }
                }
                return ifd;
            });
            return (IFD) accountable;
        }

        @Override
        public void onClose(CacheKey key) throws IOException {
            cache.invalidate(new Key(this, key, null));
            // don't call cache.cleanUp here as it would have bad performance implications
        }

        @Override
        public void clear() {
            for (Key key : cache.keys()) {
                if (key.indexCache.index.equals(index)) {
                    cache.invalidate(key);
                }
            }
            // force eviction
            cache.refresh();
        }

        @Override
        public void clear(String fieldName) {
            for (Key key : cache.keys()) {
                if (key.indexCache.index.equals(index)) {
                    if (key.indexCache.fieldName.equals(fieldName)) {
                        cache.invalidate(key);
                    }
                }
            }
            // we call refresh because this is a manual operation, should happen
            // rarely and probably means the user wants to see memory returned as
            // soon as possible
            cache.refresh();
        }
    }

    private void updateMaximumWeight(ByteSizeValue newMaximumWeight) {
        long oldMaximumWeight = cache.getMaximumWeight();
        cache.setMaximumWeight(newMaximumWeight.getBytes());
        if (newMaximumWeight.getBytes() < oldMaximumWeight) {
            // Evict entries if needed, if the new size is smaller than the old.
            // Do it asynchronously to avoid blocking cluster applier thread
            if (threadPool != null) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(cache::refresh);
            } else {
                cache.refresh();
            }
        }
    }

    /**
     * Key for the indices field data cache
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Key {
        public final IndexFieldCache indexCache;
        public final IndexReader.CacheKey readerKey;
        public final ShardId shardId;

        public final List<IndexFieldDataCache.Listener> listeners = new ArrayList<>();

        Key(IndexFieldCache indexCache, IndexReader.CacheKey readerKey, @Nullable ShardId shardId) {
            this.indexCache = indexCache;
            this.readerKey = readerKey;
            this.shardId = shardId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            if (!indexCache.equals(key.indexCache)) return false;
            if (!readerKey.equals(key.readerKey)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = indexCache.hashCode();
            result = 31 * result + readerKey.hashCode();
            return result;
        }
    }

}

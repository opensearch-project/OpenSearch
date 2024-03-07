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

package org.opensearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.service.CacheService;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.ToLongBiFunction;

/**
 * The indices request cache allows to cache a shard level request stage responses, helping with improving
 * similar requests that are potentially expensive (because of aggs for example). The cache is fully coherent
 * with the semantics of NRT (the index reader cache key is part of the cache key), and relies on size based
 * eviction to evict old reader associated cache entries as well as scheduler reaper to clean readers that
 * are no longer used or closed shards.
 * <p>
 * Currently, the cache is only enabled for count requests, and can only be opted in on an index
 * level setting that can be dynamically changed and defaults to false.
 * <p>
 * There are still several TODOs left in this class, some easily addressable, some more complex, but the support
 * is functional.
 *
 * @opensearch.internal
 */
public final class IndicesRequestCache implements RemovalListener<IndicesRequestCache.Key, BytesReference>, Closeable {

    private static final Logger logger = LogManager.getLogger(IndicesRequestCache.class);

    /**
     * A setting to enable or disable request caching on an index level. Its dynamic by default
     * since we are checking on the cluster state IndexMetadata always.
     */
    public static final Setting<Boolean> INDEX_CACHE_REQUEST_ENABLED_SETTING = Setting.boolSetting(
        "index.requests.cache.enable",
        true,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDICES_CACHE_QUERY_SIZE = Setting.memorySizeSetting(
        "indices.requests.cache.size",
        "1%",
        Property.NodeScope
    );
    public static final Setting<TimeValue> INDICES_CACHE_QUERY_EXPIRE = Setting.positiveTimeSetting(
        "indices.requests.cache.expire",
        new TimeValue(0),
        Property.NodeScope
    );

    private final static long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Key.class);

    private final ConcurrentMap<CleanupKey, Boolean> registeredClosedListeners = ConcurrentCollections.newConcurrentMap();
    private final Set<CleanupKey> keysToClean = ConcurrentCollections.newConcurrentSet();
    private final ByteSizeValue size;
    private final TimeValue expire;
    private final ICache<Key, BytesReference> cache;
    private final Function<ShardId, Optional<CacheEntity>> cacheEntityLookup;

    IndicesRequestCache(Settings settings, Function<ShardId, Optional<CacheEntity>> cacheEntityFunction, CacheService cacheService) {
        this.size = INDICES_CACHE_QUERY_SIZE.get(settings);
        this.expire = INDICES_CACHE_QUERY_EXPIRE.exists(settings) ? INDICES_CACHE_QUERY_EXPIRE.get(settings) : null;
        long sizeInBytes = size.getBytes();
        ToLongBiFunction<Key, BytesReference> weigher = (k, v) -> k.ramBytesUsed() + v.ramBytesUsed();
        this.cacheEntityLookup = cacheEntityFunction;
        this.cache = cacheService.createCache(
            new CacheConfig.Builder<Key, BytesReference>().setSettings(settings)
                .setWeigher(weigher)
                .setValueType(BytesReference.class)
                .setKeyType(Key.class)
                .setRemovalListener(this)
                .setMaxSizeInBytes(sizeInBytes) // for backward compatibility
                .setExpireAfterAccess(expire) // for backward compatibility
                .build(),
            CacheType.INDICES_REQUEST_CACHE
        );
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }

    void clear(CacheEntity entity) {
        keysToClean.add(new CleanupKey(entity, null));
        cleanCache();
    }

    @Override
    public void onRemoval(RemovalNotification<Key, BytesReference> notification) {
        // In case this event happens for an old shard, we can safely ignore this as we don't keep track for old
        // shards as part of request cache.
        cacheEntityLookup.apply(notification.getKey().shardId).ifPresent(entity -> entity.onRemoval(notification));
    }

    BytesReference getOrCompute(
        IndicesService.IndexShardCacheEntity cacheEntity,
        CheckedSupplier<BytesReference, IOException> loader,
        DirectoryReader reader,
        BytesReference cacheKey
    ) throws Exception {
        assert reader.getReaderCacheHelper() != null;
        assert reader.getReaderCacheHelper() instanceof OpenSearchDirectoryReader.DelegatingCacheHelper;

        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper = (OpenSearchDirectoryReader.DelegatingCacheHelper) reader
            .getReaderCacheHelper();
        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
        assert readerCacheKeyId != null;
        final Key key = new Key(((IndexShard) cacheEntity.getCacheIdentity()).shardId(), cacheKey, readerCacheKeyId);
        Loader cacheLoader = new Loader(cacheEntity, loader);
        BytesReference value = cache.computeIfAbsent(key, cacheLoader);
        if (cacheLoader.isLoaded()) {
            cacheEntity.onMiss();
            // see if its the first time we see this reader, and make sure to register a cleanup key
            CleanupKey cleanupKey = new CleanupKey(cacheEntity, readerCacheKeyId);
            if (!registeredClosedListeners.containsKey(cleanupKey)) {
                Boolean previous = registeredClosedListeners.putIfAbsent(cleanupKey, Boolean.TRUE);
                if (previous == null) {
                    OpenSearchDirectoryReader.addReaderCloseListener(reader, cleanupKey);
                }
            }
        } else {
            cacheEntity.onHit();
        }
        return value;
    }

    /**
     * Invalidates the given the cache entry for the given key and it's context
     * @param cacheEntity the cache entity to invalidate for
     * @param reader the reader to invalidate the cache entry for
     * @param cacheKey the cache key to invalidate
     */
    void invalidate(IndicesService.IndexShardCacheEntity cacheEntity, DirectoryReader reader, BytesReference cacheKey) {
        assert reader.getReaderCacheHelper() != null;
        String readerCacheKeyId = null;
        if (reader instanceof OpenSearchDirectoryReader) {
            IndexReader.CacheHelper cacheHelper = ((OpenSearchDirectoryReader) reader).getDelegatingCacheHelper();
            readerCacheKeyId = ((OpenSearchDirectoryReader.DelegatingCacheHelper) cacheHelper).getDelegatingCacheKey().getId();
        }
        cache.invalidate(new Key(((IndexShard) cacheEntity.getCacheIdentity()).shardId(), cacheKey, readerCacheKeyId));
    }

    /**
     * Loader for the request cache
     *
     * @opensearch.internal
     */
    private static class Loader implements LoadAwareCacheLoader<Key, BytesReference> {

        private final CacheEntity entity;
        private final CheckedSupplier<BytesReference, IOException> loader;
        private boolean loaded;

        Loader(CacheEntity entity, CheckedSupplier<BytesReference, IOException> loader) {
            this.entity = entity;
            this.loader = loader;
        }

        public boolean isLoaded() {
            return this.loaded;
        }

        @Override
        public BytesReference load(Key key) throws Exception {
            BytesReference value = loader.get();
            entity.onCached(key, value);
            loaded = true;
            return value;
        }
    }

    /**
     * Basic interface to make this cache testable.
     */
    interface CacheEntity extends Accountable {

        /**
         * Called after the value was loaded.
         */
        void onCached(Key key, BytesReference value);

        /**
         * Returns <code>true</code> iff the resource behind this entity is still open ie.
         * entities associated with it can remain in the cache. ie. IndexShard is still open.
         */
        boolean isOpen();

        /**
         * Returns the cache identity. this is, similar to {@link #isOpen()} the resource identity behind this cache entity.
         * For instance IndexShard is the identity while a CacheEntity is per DirectoryReader. Yet, we group by IndexShard instance.
         */
        Object getCacheIdentity();

        /**
         * Called each time this entity has a cache hit.
         */
        void onHit();

        /**
         * Called each time this entity has a cache miss.
         */
        void onMiss();

        /**
         * Called when this entity instance is removed
         */
        void onRemoval(RemovalNotification<Key, BytesReference> notification);

    }

    /**
     * Unique key for the cache
     *
     * @opensearch.internal
     */
    static class Key implements Accountable, Writeable {
        public final ShardId shardId; // use as identity equality
        public final String readerCacheKeyId;
        public final BytesReference value;

        Key(ShardId shardId, BytesReference value, String readerCacheKeyId) {
            this.shardId = shardId;
            this.value = value;
            this.readerCacheKeyId = Objects.requireNonNull(readerCacheKeyId);
        }

        Key(StreamInput in) throws IOException {
            this.shardId = in.readOptionalWriteable(ShardId::new);
            this.readerCacheKeyId = in.readOptionalString();
            this.value = in.readBytesReference();
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + shardId.getBaseRamBytesUsed() + value.length();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            // TODO: more detailed ram usage?
            return Collections.emptyList();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            if (!Objects.equals(readerCacheKeyId, key.readerCacheKeyId)) return false;
            if (!shardId.equals(key.shardId)) return false;
            if (!value.equals(key.value)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = shardId.hashCode();
            result = 31 * result + readerCacheKeyId.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(shardId);
            out.writeOptionalString(readerCacheKeyId);
            out.writeBytesReference(value);
        }
    }

    private class CleanupKey implements IndexReader.ClosedListener {
        final CacheEntity entity;
        final String readerCacheKeyId;

        private CleanupKey(CacheEntity entity, String readerCacheKeyId) {
            this.entity = entity;
            this.readerCacheKeyId = readerCacheKeyId;
        }

        @Override
        public void onClose(IndexReader.CacheKey cacheKey) {
            Boolean remove = registeredClosedListeners.remove(this);
            if (remove != null) {
                keysToClean.add(this);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CleanupKey that = (CleanupKey) o;
            if (!Objects.equals(readerCacheKeyId, that.readerCacheKeyId)) return false;
            if (!entity.getCacheIdentity().equals(that.entity.getCacheIdentity())) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = entity.getCacheIdentity().hashCode();
            result = 31 * result + Objects.hashCode(readerCacheKeyId);
            return result;
        }
    }

    /**
     * Logic to clean up in-memory cache.
     */
    synchronized void cleanCache() {
        final Set<CleanupKey> currentKeysToClean = new HashSet<>();
        final Set<Object> currentFullClean = new HashSet<>();
        currentKeysToClean.clear();
        currentFullClean.clear();
        for (Iterator<CleanupKey> iterator = keysToClean.iterator(); iterator.hasNext();) {
            CleanupKey cleanupKey = iterator.next();
            iterator.remove();
            if (cleanupKey.readerCacheKeyId == null || !cleanupKey.entity.isOpen()) {
                // null indicates full cleanup, as does a closed shard
                currentFullClean.add(((IndexShard) cleanupKey.entity.getCacheIdentity()).shardId());
            } else {
                currentKeysToClean.add(cleanupKey);
            }
        }
        if (!currentKeysToClean.isEmpty() || !currentFullClean.isEmpty()) {
            for (Iterator<Key> iterator = cache.keys().iterator(); iterator.hasNext();) {
                Key key = iterator.next();
                if (currentFullClean.contains(key.shardId)) {
                    iterator.remove();
                } else {
                    // If the flow comes here, then we should have a open shard available on node.
                    if (currentKeysToClean.contains(
                        new CleanupKey(cacheEntityLookup.apply(key.shardId).orElse(null), key.readerCacheKeyId)
                    )) {
                        iterator.remove();
                    }
                }
            }
        }
        cache.refresh();
    }

    /**
     * Returns the current size of the cache
     */
    long count() {
        return cache.count();
    }

    int numRegisteredCloseListeners() { // for testing
        return registeredClosedListeners.size();
    }
}

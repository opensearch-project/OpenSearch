/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.ordinals;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * A node-level cache dedicated to <em>group-scoped</em> global ordinals (the {@code scope_prefix} optimisation on
 * {@code has_child}/{@code has_parent} queries).
 *
 * <p>Unlike the shared {@link org.opensearch.indices.fielddata.cache.IndicesFieldDataCache}, this cache holds only the
 * small per-group {@code OrdinalMap}s produced by
 * {@link GlobalOrdinalsBuilder#buildScoped}. Keeping them separate ensures a burst of many distinct group prefixes can
 * never evict the shared fielddata / full global ordinals that unscoped and non-join queries depend on.
 *
 * <p>Entries are keyed by {@code (field, reader-generation, shard, prefix)}. The reader-generation component means a
 * refresh naturally invalidates scoped maps (same freshness semantics as the shared cache), and a reader-close listener
 * evicts the entry — at which point the reserved circuit-breaker bytes are released via the entry's
 * {@link GlobalOrdinalsIndexFieldData#getBreakerReleasable() breaker releasable}.
 */
@InternalApi
public final class ScopedGlobalOrdinalsCache implements RemovalListener<ScopedGlobalOrdinalsCache.Key, Accountable>, Releasable {

    private final Cache<Key, Accountable> cache;

    /**
     * Reader generations for which a close listener has already been registered. Used so that, no matter how many
     * distinct group prefixes miss on the same reader, we register the reader-close listener at most once per reader
     * generation (otherwise N prefixes would register N listeners, each triggering an O(cache) {@link #invalidateReader}
     * scan on close).
     */
    private final Set<Object> readersWithCloseListener = ConcurrentHashMap.newKeySet();

    public ScopedGlobalOrdinalsCache(ByteSizeValue maxSize, TimeValue expireAfterAccess) {
        CacheBuilder<Key, Accountable> builder = CacheBuilder.<Key, Accountable>builder().removalListener(this);
        long sizeInBytes = maxSize.getBytes();
        if (sizeInBytes > 0) {
            builder.setMaximumWeight(sizeInBytes).weigher((k, v) -> v.ramBytesUsed());
        }
        if (expireAfterAccess != null && expireAfterAccess.nanos() > 0) {
            builder.setExpireAfterAccess(expireAfterAccess);
        }
        this.cache = builder.build();
    }

    /**
     * Return the group-scoped global ordinals for {@code (readerKey, prefix)}, building (and caching) them on a miss.
     * Reader-generation eviction is arranged by the caller ({@code IndexFieldCache}), which registers a reader-close
     * listener the first time an entry for a given reader generation is created.
     */
    public IndexOrdinalsFieldData computeIfAbsent(
        org.opensearch.core.index.Index index,
        String fieldName,
        ShardId shardId,
        Object readerKey,
        BytesRef prefix,
        java.util.function.Supplier<IndexOrdinalsFieldData> loader
    ) {
        final Key key = new Key(index, fieldName, shardId, readerKey, BytesRef.deepCopyOf(prefix));
        try {
            return (IndexOrdinalsFieldData) cache.computeIfAbsent(key, k -> (Accountable) loader.get());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build scoped global ordinals for prefix [" + prefix.utf8ToString() + "]", e);
        }
    }

    /**
     * Register a reader-close listener for the given reader generation <em>at most once</em>, regardless of how many
     * prefixes are cached against it. The {@code registrar} is invoked (to actually attach the listener to the reader)
     * only the first time this reader key is seen; subsequent calls for the same reader are no-ops.
     */
    public void registerReaderCloseListener(Object readerKey, Consumer<Object> registrar) {
        if (readersWithCloseListener.add(readerKey)) {
            registrar.accept(readerKey);
        }
    }

    /** Invalidate all scoped entries built against the given reader generation (called on reader close). */
    public void invalidateReader(Object readerKey) {
        readersWithCloseListener.remove(readerKey);
        for (Key k : cache.keys()) {
            if (Objects.equals(k.readerKey, readerKey)) {
                cache.invalidate(k);
            }
        }
    }

    /**
     * Release the circuit-breaker bytes reserved by an evicted scoped map. The scoped {@link GlobalOrdinalsIndexFieldData}
     * reserved fielddata-breaker bytes at build time; since this cache owns the lifecycle, we free them here exactly once
     * on removal (size eviction, TTL, reader close, or explicit clear).
     */
    @Override
    public void onRemoval(RemovalNotification<Key, Accountable> notification) {
        final Accountable value = notification.getValue();
        if (value instanceof GlobalOrdinalsIndexFieldData) {
            final Releasable breakerReleasable = ((GlobalOrdinalsIndexFieldData) value).getBreakerReleasable();
            if (breakerReleasable != null) {
                breakerReleasable.close();
            }
        }
    }

    /** Current number of cached scoped maps (for stats/testing). */
    public int count() {
        return cache.count();
    }

    /** Current total RAM (bytes) of cached scoped maps (for stats/testing). */
    public long ramBytesUsed() {
        return cache.weight();
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }

    /** Builds group-scoped global ordinals for a reader + prefix on a cache miss. */
    @FunctionalInterface
    public interface ScopedOrdinalsLoader {
        IndexOrdinalsFieldData load(DirectoryReader indexReader, BytesRef prefix) throws Exception;
    }

    /** Cache key: a scoped map is unique per (index, field, shard, reader generation, group prefix). */
    public static final class Key {
        private final org.opensearch.core.index.Index index;
        private final String fieldName;
        private final ShardId shardId;
        final Object readerKey;
        private final BytesRef prefix;

        Key(org.opensearch.core.index.Index index, String fieldName, ShardId shardId, Object readerKey, BytesRef prefix) {
            this.index = index;
            this.fieldName = fieldName;
            this.shardId = shardId;
            this.readerKey = readerKey;
            this.prefix = prefix;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Objects.equals(readerKey, key.readerKey)
                && Objects.equals(shardId, key.shardId)
                && Objects.equals(index, key.index)
                && Objects.equals(fieldName, key.fieldName)
                && Objects.equals(prefix, key.prefix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, fieldName, shardId, readerKey, prefix);
        }
    }
}

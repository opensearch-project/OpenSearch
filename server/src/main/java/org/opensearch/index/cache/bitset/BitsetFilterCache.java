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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexWarmer;
import org.opensearch.indices.IndicesBitsetFilterCache;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;

/**
 * Per-index view into the node-level {@link IndicesBitsetFilterCache}.
 * <p>
 * This is a cache for {@link BitDocIdSet} based filters. Use this cache with care, only components
 * that require a filter to be materialized as a {@link BitDocIdSet} and require that it should always
 * be around should use this cache, otherwise the
 * {@link org.opensearch.index.cache.query.QueryCache} should be used instead.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class BitsetFilterCache extends AbstractIndexComponent
    implements
        IndexReader.ClosedListener,
        RemovalListener<IndexReader.CacheKey, Cache<Query, BitsetFilterCache.Value>>,
        Closeable {

    /**
     * @deprecated Use {@link IndicesBitsetFilterCache#INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING} instead.
     */
    @Deprecated
    public static final Setting<Boolean> INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING =
        IndicesBitsetFilterCache.INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING;

    private final IndicesBitsetFilterCache indicesCache;
    private final Listener listener;

    /**
     * @deprecated Use {@link #BitsetFilterCache(IndexSettings, IndicesBitsetFilterCache, Listener)} instead.
     */
    @Deprecated
    public BitsetFilterCache(IndexSettings indexSettings, Listener listener) {
        this(indexSettings, null, listener);
    }

    public BitsetFilterCache(IndexSettings indexSettings, IndicesBitsetFilterCache indicesCache, Listener listener) {
        super(indexSettings);
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        this.indicesCache = indicesCache;
        this.listener = listener;
    }

    public static BitSet bitsetFromQuery(Query query, LeafReaderContext context) throws IOException {
        return IndicesBitsetFilterCache.bitsetFromQuery(query, context);
    }

    /**
     * @deprecated The warmer is now created by {@link IndicesBitsetFilterCache#createListener(ThreadPool)}.
     */
    @Deprecated
    public IndexWarmer.Listener createListener(ThreadPool threadPool) {
        if (indicesCache != null) {
            return indicesCache.createListener(threadPool);
        }
        return null;
    }

    public BitSetProducer getBitSetProducer(Query query) {
        if (indicesCache != null) {
            return indicesCache.getBitSetProducer(query, listener);
        }
        throw new IllegalStateException("IndicesBitsetFilterCache is not available");
    }

    @Override
    public void onClose(IndexReader.CacheKey ownerCoreCacheKey) {
        // Delegated to node-level cache
    }

    @Override
    public void close() {
        // Per-index close is a no-op; entries are cleaned up by the node-level
        // periodic stale-key purge after the index's readers close.
    }

    public void clear(String reason) {
        logger.debug("clearing all bitsets because [{}]", reason);
        // Per-index clear is a no-op; entries are evicted by the node-level cache.
    }

    @Override
    public void onRemoval(RemovalNotification<IndexReader.CacheKey, Cache<Query, Value>> notification) {
        // Delegated to node-level cache
    }

    /**
     * Value for bitset filter cache
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static final class Value {

        final BitSet bitset;
        final ShardId shardId;

        public Value(BitSet bitset, ShardId shardId) {
            this.bitset = bitset;
            this.shardId = shardId;
        }
    }

    /**
     * A listener interface that is executed for each onCache / onRemoval event
     *
     * @opensearch.internal
     */
    @PublicApi(since = "1.0.0")
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

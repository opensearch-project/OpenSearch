/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugins.BlockCacheRegistry;
import org.opensearch.repositories.NativeStoreRepository;

/**
 * Per-format factory that produces a {@link DataFormatStoreHandler} for a shard.
 *
 * <p>Returned by {@link StoreStrategy#storeHandler()} for formats that
 * need native file tracking (e.g. parquet with a Rust reader). The store
 * layer invokes {@link #create} once per shard.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
@FunctionalInterface
public interface DataFormatStoreHandlerFactory {

    /**
     * Creates a per-shard store handler.
     *
     * @param shardId        the shard id
     * @param isWarm         true if the shard is on a warm node
     * @param repo           the native remote store repository, or {@link NativeStoreRepository#EMPTY}
     *                       when no native store is available
     * @param cacheRegistry  registry for looking up block caches by name; the handler may use this
     *                       to resolve its preferred cache via a {@link org.opensearch.plugins.BuiltInBlockCaches}
     *                       constant. {@code null} if no block cache support is available.
     * @return a live handler; the caller owns it and must close it
     */
    DataFormatStoreHandler create(ShardId shardId, boolean isWarm, NativeStoreRepository repo, BlockCacheRegistry cacheRegistry);
}

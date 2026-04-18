/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.shard.ShardPath;

/**
 * Factory that creates a shard-scoped native (Rust) object store handle.
 *
 * <p>Implementations capture the repository-level native store internally and
 * scope it to a shard path when {@link #create(ShardPath)} is called. This
 * avoids exposing the repository-level store to shard-level consumers.
 *
 * <p>The returned {@link ShardNativeStore} is owned by the caller and must
 * be closed when the shard is closed.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
@FunctionalInterface
public interface NativeStoreFactory {

    /** A no-op factory that always returns {@link ShardNativeStore#EMPTY}. */
    NativeStoreFactory EMPTY = (shardPath) -> ShardNativeStore.EMPTY;

    /**
     * Creates a shard-scoped native object store handle.
     *
     * @param shardPath the shard path used to scope the store
     * @return a live shard store, or {@link ShardNativeStore#EMPTY} if not supported
     */
    ShardNativeStore create(ShardPath shardPath);
}

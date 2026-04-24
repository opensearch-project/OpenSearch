/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.plugins.NativeStoreHandle;

/**
 * Shard-scoped wrapper around a native (Rust) object store handle.
 *
 * <p>Distinguishes shard-level native stores from repository-level ones
 * ({@link org.opensearch.repositories.NativeStoreRepository}) at the type level.
 * The underlying {@link NativeStoreHandle} points to a Rust {@code Arc<dyn ObjectStore>}
 * that is scoped to this shard's path prefix (e.g., via {@code PrefixStore}).
 *
 * <p>Owned by {@link org.opensearch.index.engine.DataFormatAwareEngine} and closed
 * when the shard shuts down.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ShardNativeStore implements AutoCloseable {

    /** A no-op instance for shards that do not use a native object store. */
    public static final ShardNativeStore EMPTY = new ShardNativeStore(NativeStoreHandle.EMPTY);

    private final NativeStoreHandle handle;

    public ShardNativeStore(NativeStoreHandle handle) {
        this.handle = handle != null ? handle : NativeStoreHandle.EMPTY;
    }

    /**
     * Returns the underlying native store handle.
     */
    public NativeStoreHandle getHandle() {
        return handle;
    }

    /**
     * Returns the raw native pointer for FFM calls.
     */
    public long getPointer() {
        return handle.getPointer();
    }

    /**
     * Returns {@code true} if this store wraps a live native pointer.
     */
    public boolean isLive() {
        return this != EMPTY && handle != NativeStoreHandle.EMPTY;
    }

    @Override
    public void close() {
        handle.close();
    }
}

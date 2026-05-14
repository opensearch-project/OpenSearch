/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * A <em>borrowed</em>, non-owning reference to a native block-cache instance.
 *
 * <p>Unlike {@link NativeStoreHandle}, which owns the native resource and
 * destroys it on {@code close()}, {@code NativeCacheHandle} only holds a
 * pointer — the cache is owned by the component that created it (e.g.
 * {@code NodeCacheOrchestrator}) and has a longer lifetime than any individual
 * shard handler.
 *
 * <p>Callers must not attempt to free the pointer. The handle is valid for as
 * long as the owning {@link BlockCache} is alive.
 *
 * <p>An empty sentinel ({@link #EMPTY}) is provided for the no-cache case.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class NativeCacheHandle {

    /**
     * Sentinel value representing "no cache" — returned by default
     * implementations of {@link BlockCache#nativeCacheHandle()}.
     */
    public static final NativeCacheHandle EMPTY = new NativeCacheHandle(0L);

    private final long ptr;

    private NativeCacheHandle(long ptr) {
        this.ptr = ptr;
    }

    /**
     * Create a borrowed handle for {@code ptr}.
     *
     * @param ptr native pointer; must be positive
     * @throws IllegalArgumentException if {@code ptr <= 0}
     */
    public static NativeCacheHandle of(long ptr) {
        if (ptr <= 0) {
            throw new IllegalArgumentException("NativeCacheHandle pointer must be positive, got: " + ptr);
        }
        return new NativeCacheHandle(ptr);
    }

    /**
     * Returns the raw native pointer.
     *
     * @return the pointer; always positive when {@link #isLive()} is {@code true}
     */
    public long getPointer() {
        return ptr;
    }

    /**
     * Returns {@code true} if this handle points to a live native cache instance.
     */
    public boolean isLive() {
        return ptr > 0;
    }

    @Override
    public String toString() {
        return isLive() ? "NativeCacheHandle(ptr=" + ptr + ")" : "NativeCacheHandle(EMPTY)";
    }
}

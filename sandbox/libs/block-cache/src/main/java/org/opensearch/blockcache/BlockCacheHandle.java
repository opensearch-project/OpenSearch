/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.Objects;

/**
 * Lifecycle owner of a node-level {@link BlockCache}.
 *
 * <p>Created once at node startup and closed after all plugins have stopped.
 * Provides a stable, named type for dependency injection — callers inject
 * {@code BlockCacheHandle} rather than the raw {@link BlockCache} interface,
 * which avoids ambiguity when multiple {@link BlockCache} bindings exist.
 *
 * <p>The concrete {@link BlockCache} implementation is injected at construction
 * time, keeping this class entirely decoupled from any specific backend:
 *
 * <pre>{@code
 * // Foyer-backed (native):
 * BlockCacheHandle handle = new BlockCacheHandle(new FoyerBlockCache(diskBytes, diskDir));
 *
 * // No-op (tests / cache disabled):
 * BlockCacheHandle handle = new BlockCacheHandle(new NoOpBlockCache());
 * }</pre>
 *
 * <p>Thread-safe. The {@link BlockCache} reference is {@code final}; per JLS §17.5,
 * {@code final} fields have safe-publication semantics.
 *
 * @opensearch.experimental
 */
public final class BlockCacheHandle implements Closeable {

    private static final Logger logger = LogManager.getLogger(BlockCacheHandle.class);

    private final BlockCache cache;

    /**
     * Creates a {@code BlockCacheHandle} wrapping the given {@link BlockCache}.
     *
     * @param cache the {@link BlockCache} implementation to manage; must not be null
     * @throws NullPointerException if {@code cache} is null
     */
    public BlockCacheHandle(BlockCache cache) {
        this.cache = Objects.requireNonNull(cache, "cache must not be null");
    }

    /**
     * Returns the {@link BlockCache} managed by this handle.
     *
     * <p>Callers that need to hand the cache to a native layer should
     * pattern-match against the concrete implementation:
     * <pre>{@code
     * BlockCache bc = handle.getCache();
     * if (bc instanceof FoyerBlockCache foyer) {
     *     nativeRuntime.attachCache(foyer.nativeCachePtr());
     * }
     * }</pre>
     *
     * @return the {@link BlockCache} instance; never {@code null}
     */
    public BlockCache getCache() {
        return cache;
    }

    /**
     * Closes the underlying cache. Delegates to {@link BlockCache#close()}.
     *
     * <p>Idempotency is enforced by the {@link BlockCache} implementation.
     */
    @Override
    public void close() {
        cache.close();
        logger.info("Block cache closed");
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.pagecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.Objects;

/**
 * Lifecycle owner of a node-level {@link PageCache}.
 *
 * <p>Created once at node startup and closed after all plugins have stopped.
 * Provides a stable, named type for dependency injection — callers inject
 * {@code PageCacheHandle} rather than the raw {@link PageCache} interface,
 * which avoids ambiguity when multiple {@link PageCache} bindings exist.
 *
 * <p>The concrete {@link PageCache} implementation is injected at construction
 * time, keeping this class entirely decoupled from any specific backend:
 *
 * <pre>{@code
 * // Foyer-backed (native):
 * PageCacheHandle handle = new PageCacheHandle(new FoyerPageCache(diskBytes, diskDir));
 *
 * // Pure-Java alternative:
 * PageCacheHandle handle = new PageCacheHandle(new CaffeinePageCache(diskBytes));
 *
 * // No-op (tests / cache disabled):
 * PageCacheHandle handle = new PageCacheHandle(new NoOpPageCache());
 * }</pre>
 *
 * <p>Thread-safe. The {@link PageCache} reference is {@code final}; per JLS §17.5,
 * {@code final} fields have safe-publication semantics.
 *
 * @opensearch.experimental
 */
public final class PageCacheHandle implements Closeable {

    private static final Logger logger = LogManager.getLogger(PageCacheHandle.class);

    private final PageCache cache;

    /**
     * Creates a {@code PageCacheHandle} wrapping the given {@link PageCache}.
     *
     * @param cache the {@link PageCache} implementation to manage; must not be null
     * @throws NullPointerException if {@code cache} is null
     */
    public PageCacheHandle(PageCache cache) {
        this.cache = Objects.requireNonNull(cache, "cache must not be null");
    }

    /**
     * Returns the {@link PageCache} managed by this handle.
     *
     * <p>Callers that need to hand the cache to a native layer should
     * pattern-match against the concrete implementation:
     * <pre>{@code
     * PageCache pc = handle.getCache();
     * if (pc instanceof FoyerPageCache foyer) {
     *     nativeRuntime.attachCache(foyer.nativeCachePtr());
     * }
     * }</pre>
     *
     * @return the {@link PageCache} instance; never {@code null}
     */
    public PageCache getCache() {
        return cache;
    }

    /**
     * Closes the underlying cache. Delegates to {@link PageCache#close()}.
     *
     * <p>Idempotency is enforced by the {@link PageCache} implementation.
     */
    @Override
    public void close() {
        cache.close();
        logger.info("Page cache closed");
    }
}

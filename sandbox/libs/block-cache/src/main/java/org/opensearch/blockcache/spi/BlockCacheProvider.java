/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.spi;

import org.opensearch.blockcache.BlockCacheHandle;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * SPI implemented by a native-repository plugin to describe how its
 * repository type integrates with a node-level block cache.
 *
 * <p>Follows the same discovery pattern as
 * {@link org.opensearch.plugins.NativeRemoteObjectStoreProvider}: each
 * implementing plugin registers it via
 * {@code META-INF/services/org.opensearch.blockcache.spi.BlockCacheProvider}
 * and is loaded by {@code block-cache-foyer} through
 * {@link org.opensearch.plugins.ExtensiblePlugin#loadExtensions}.
 *
 * <p>The provider is <em>stateless</em>. It does not own the cache; it
 * only describes (a) which repository type it handles and (b) how to
 * attach a cache handle to a native store pointer. Lifecycle of the
 * cache itself is owned by {@code block-cache-foyer}, identically to
 * how native store lifecycle is owned today by each native-repository
 * plugin.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BlockCacheProvider {

    /**
     * Repository type this provider wires the cache to
     * ({@code "s3"}, {@code "gcs"}, {@code "azure"}, {@code "fs"}).
     * Must match {@link org.opensearch.plugins.NativeRemoteObjectStoreProvider#repositoryType()}
     * for the same plugin so the two SPIs stay aligned.
     */
    String repositoryType();

    /**
     * Called once per repository instance, after the native store has
     * been created by
     * {@link org.opensearch.plugins.NativeRemoteObjectStoreProvider#createNativeStore}.
     *
     * <p>Receives the node-level handle and the repository metadata,
     * and is responsible for telling the provider's own FFM bridge to
     * route reads through the cache. Implementations typically resolve
     * {@code handle.getCache()} to a {@code FoyerBlockCache}, extract
     * the native cache pointer, and pass it to their Rust side.
     *
     * <p>Idempotent: calling more than once per repository is a no-op.
     * Safe to call before or after the repository is registered in
     * {@code RepositoriesService} — order is not guaranteed across plugins.
     *
     * @param handle   node-level block cache handle; never null
     * @param metadata the repository being attached; {@code metadata.type()}
     *                 equals {@link #repositoryType()}
     * @throws IllegalStateException if the cache handle is already closed
     */
    void attach(BlockCacheHandle handle, RepositoryMetadata metadata);
}

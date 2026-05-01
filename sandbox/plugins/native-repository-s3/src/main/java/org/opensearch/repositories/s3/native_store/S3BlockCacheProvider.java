/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.native_store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.blockcache.BlockCacheHandle;
import org.opensearch.blockcache.foyer.FoyerBlockCache;
import org.opensearch.blockcache.spi.BlockCacheProvider;
import org.opensearch.cluster.metadata.RepositoryMetadata;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link BlockCacheProvider} implementation that wires the Foyer-backed
 * node-level block cache into the S3 native object store.
 *
 * <p>Discovered by {@code BlockCacheFoyerPlugin.loadExtensions} via
 * {@code META-INF/services/org.opensearch.blockcache.spi.BlockCacheProvider}.
 * The constructor takes the extending plugin instance per
 * {@code PluginsService.createExtension} contract.
 *
 * <p>The actual cache-to-native-store wiring requires a Rust-side FFM entry
 * point (e.g. {@code s3_attach_cache(store_ptr, cache_ptr, repo_name)}) which
 * does not yet exist. Until that symbol lands, this provider performs only
 * structural validation and idempotency tracking; the real {@code attach} is
 * marked with a TODO comment at the exact call site.
 *
 * @opensearch.experimental
 */
public final class S3BlockCacheProvider implements BlockCacheProvider {

    private static final Logger logger = LogManager.getLogger(S3BlockCacheProvider.class);

    private final S3NativeObjectStorePlugin storePlugin;
    private final Set<String> attachedRepoNames = ConcurrentHashMap.newKeySet();

    /**
     * Constructor invoked by
     * {@link org.opensearch.plugins.PluginsService#createExtension} with the
     * extending plugin instance.
     *
     * @param storePlugin the native object-store plugin this provider wires into
     */
    public S3BlockCacheProvider(final S3NativeObjectStorePlugin storePlugin) {
        this.storePlugin = Objects.requireNonNull(storePlugin, "storePlugin must not be null");
    }

    @Override
    public String repositoryType() {
        return S3NativeObjectStorePlugin.TYPE;
    }

    @Override
    public void attach(final BlockCacheHandle handle, final RepositoryMetadata metadata) {
        Objects.requireNonNull(handle, "handle must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");
        if (repositoryType().equals(metadata.type()) == false) {
            throw new IllegalArgumentException(
                "Provider handles type [" + repositoryType() + "] but received metadata for type [" + metadata.type() + "]"
            );
        }

        // Idempotent per requirement 2.5: second call with same metadata is a no-op.
        if (attachedRepoNames.add(metadata.name()) == false) {
            logger.debug("S3BlockCacheProvider.attach skipped (already attached) for repo [{}]", metadata.name());
            return;
        }

        if (handle.getCache() instanceof FoyerBlockCache foyer) {
            final long nativeCachePtr = foyer.nativeCachePtr();
            // TODO: Invoke the Rust-side FFM attach symbol once it exists:
            // S3NativeBridge.attachCache(storePlugin.nativeStorePtrFor(metadata), nativeCachePtr, metadata.name());
            // The native-store pointer lives in a NativeStoreRepository held by RepositoriesService;
            // surfacing it to this provider requires either (a) adding a lookup method on
            // S3NativeObjectStorePlugin (e.g. nativeStorePtrFor(RepositoryMetadata)), or (b) invoking
            // attach at repo-creation time from within createNativeStore. Track under a follow-on.
            logger.info(
                "S3BlockCacheProvider attach (structural; native FFM pending): cachePtr={}, repo=[{}], type=[{}]",
                nativeCachePtr,
                metadata.name(),
                metadata.type()
            );
        } else {
            logger.info(
                "S3BlockCacheProvider: handle is {} (not FoyerBlockCache); skipping native attach for repo [{}]",
                handle.getCache().getClass().getSimpleName(),
                metadata.name()
            );
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure.native_store;

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
 * node-level block cache into the Azure native object store.
 *
 * <p>Discovered by {@code BlockCacheFoyerPlugin.loadExtensions} via
 * {@code META-INF/services/org.opensearch.blockcache.spi.BlockCacheProvider}.
 * The constructor takes the extending plugin instance per
 * {@code PluginsService.createExtension} contract.
 *
 * <p>The actual cache-to-native-store wiring requires a Rust-side FFM entry
 * point (e.g. {@code azure_attach_cache(store_ptr, cache_ptr, repo_name)}) which
 * does not yet exist. Until that symbol lands, this provider performs only
 * structural validation and idempotency tracking; the real {@code attach} is
 * marked with a TODO comment at the exact call site.
 *
 * @opensearch.experimental
 */
public final class AzureBlockCacheProvider implements BlockCacheProvider {

    private static final Logger logger = LogManager.getLogger(AzureBlockCacheProvider.class);

    private final AzureNativeObjectStorePlugin storePlugin;
    private final Set<String> attachedRepoNames = ConcurrentHashMap.newKeySet();

    public AzureBlockCacheProvider(final AzureNativeObjectStorePlugin storePlugin) {
        this.storePlugin = Objects.requireNonNull(storePlugin, "storePlugin must not be null");
    }

    @Override
    public String repositoryType() {
        return AzureNativeObjectStorePlugin.TYPE;
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
        if (attachedRepoNames.add(metadata.name()) == false) {
            logger.debug("AzureBlockCacheProvider.attach skipped (already attached) for repo [{}]", metadata.name());
            return;
        }
        if (handle.getCache() instanceof FoyerBlockCache foyer) {
            final long nativeCachePtr = foyer.nativeCachePtr();
            // TODO: Invoke the Rust-side FFM attach symbol once it exists:
            // AzureNativeBridge.attachCache(storePlugin.nativeStorePtrFor(metadata), nativeCachePtr, metadata.name());
            logger.info(
                "AzureBlockCacheProvider attach (structural; native FFM pending): cachePtr={}, repo=[{}], type=[{}]",
                nativeCachePtr,
                metadata.name(),
                metadata.type()
            );
        } else {
            logger.info(
                "AzureBlockCacheProvider: handle is {} (not FoyerBlockCache); skipping native attach for repo [{}]",
                handle.getCache().getClass().getSimpleName(),
                metadata.name()
            );
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.plugins.NativeRemoteObjectStoreProvider;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-level service that manages native (Rust) remote ObjectStore instances.
 *
 * <p>Mirrors the {@link RepositoriesService} pattern: aggregates all
 * {@link NativeRemoteObjectStoreProvider} implementations discovered via
 * plugin loading, routes store creation by repository type, and caches
 * pointers for reuse across consumers.
 *
 * <p>Created by {@code Node.java} during startup, alongside
 * {@link RepositoriesService}. Any module or plugin that needs native
 * remote store pointers injects this service.
 *
 * <p>Thread-safe. Store pointers are cached in a {@link ConcurrentHashMap}
 * keyed by repository name. Each pointer represents a
 * {@code Box<Arc<dyn ObjectStore>>} on the Rust side.
 *
 * <p>TODO: Upgrade to {@code AbstractLifecycleComponent} and implement
 * {@code ClusterStateListener} to handle dynamic repository changes
 * (settings updates, repo deletion). Currently stores are created on
 * first use and only destroyed on node shutdown. If repo settings change
 * (e.g. SSE-KMS key rotation), the cached store retains the old config
 * until node restart.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class NativeRemoteObjectStoreService implements Closeable {

    private static final Logger logger = LogManager.getLogger(NativeRemoteObjectStoreService.class);

    /** Sentinel value returned by {@link #getStorePtr} when no store exists for the given repo. */
    public static final long STORE_NOT_FOUND = -1L;

    private final Map<String, NativeRemoteObjectStoreProvider> providersByType;
    private final ConcurrentHashMap<String, StoreEntry> stores = new ConcurrentHashMap<>();

    /**
     * @param providers discovered via {@code pluginsService.filterPlugins(NativeRemoteObjectStoreProvider.class)}
     */
    public NativeRemoteObjectStoreService(final List<NativeRemoteObjectStoreProvider> providers) {
        Objects.requireNonNull(providers, "providers must not be null");
        final Map<String, NativeRemoteObjectStoreProvider> map = new HashMap<>();
        for (final NativeRemoteObjectStoreProvider provider : providers) {
            final NativeRemoteObjectStoreProvider existing = map.put(provider.repositoryType(), provider);
            if (existing != null) {
                throw new IllegalArgumentException(
                    "Duplicate NativeRemoteObjectStoreProvider for type [" + provider.repositoryType() + "]"
                );
            }
        }
        this.providersByType = Collections.unmodifiableMap(map);
        logger.info("NativeRemoteObjectStoreService initialized with providers: {}", providersByType.keySet());
    }

    /**
     * Ensure a native store exists for the given repository. Creates one if not cached.
     *
     * @param repoName repository name (cache key)
     * @param repoType repository type (e.g. "s3") — used to find the provider
     * @param configJson backend-specific config JSON
     * @return native store pointer ({@code > 0})
     * @throws RepositoryException if no provider for the type or creation fails
     */
    public long ensureStore(final String repoName, final String repoType, final String configJson) {
        return ensureStore(repoName, repoType, configJson, 0L);
    }

    /**
     * Ensure a native store exists, with optional custom credential provider.
     *
     * @param repoName repository name (cache key)
     * @param repoType repository type (e.g. "s3") — used to find the provider
     * @param configJson backend-specific config JSON
     * @param credProviderPtr native credential provider pointer, or 0 for default chain
     * @return native store pointer ({@code > 0})
     * @throws RepositoryException if no provider for the type or creation fails
     */
    public long ensureStore(final String repoName, final String repoType, final String configJson, final long credProviderPtr) {
        final StoreEntry entry = stores.computeIfAbsent(repoName, name -> {
            final NativeRemoteObjectStoreProvider provider = providersByType.get(repoType);
            if (provider == null) {
                throw new RepositoryException(repoName, "no NativeRemoteObjectStoreProvider for type [" + repoType + "]");
            }
            final long ptr = provider.createNativeStore(configJson, credProviderPtr);
            if (ptr <= 0) {
                throw new RepositoryException(repoName, "native store creation failed for type [" + repoType + "], ptr=" + ptr);
            }
            try {
                final StoreEntry result = new StoreEntry(ptr, provider);
                logger.debug("Created native remote store for repo [{}] type [{}] ptr={}", repoName, repoType, ptr);
                return result;
            } catch (final Throwable t) {
                // Prevent native memory leak if StoreEntry allocation fails (e.g. OOM)
                try {
                    provider.destroyNativeStore(ptr);
                } catch (final Exception destroyEx) {
                    logger.warn("Failed to clean up native store after error for repo [{}]: {}", repoName, destroyEx.getMessage());
                }
                throw t;
            }
        });
        return entry.ptr;
    }

    /**
     * Get the cached native store pointer for a repository.
     *
     * @return pointer, or {@link #STORE_NOT_FOUND} if not created
     */
    public long getStorePtr(final String repoName) {
        final StoreEntry entry = stores.get(repoName);
        return entry != null ? entry.ptr : STORE_NOT_FOUND;
    }

    /**
     * Get all cached native store pointers.
     *
     * @return unmodifiable map of repo name to pointer
     */
    public Map<String, Long> getAllStores() {
        final Map<String, Long> result = new HashMap<>(stores.size());
        stores.forEach((name, entry) -> result.put(name, entry.ptr));
        return Collections.unmodifiableMap(result);
    }

    /**
     * Check if a provider exists for the given repository type.
     */
    public boolean hasProvider(final String repoType) {
        return providersByType.containsKey(repoType);
    }

    @Override
    public void close() {
        stores.forEach((name, entry) -> {
            try {
                entry.provider.destroyNativeStore(entry.ptr);
                logger.debug("Destroyed native remote store for repo [{}]", name);
            } catch (final Exception e) {
                logger.warn("Failed to destroy native remote store for repo [{}]: {}", name, e.getMessage());
            }
        });
        stores.clear();
    }

    /**
     * Cached native store pointer paired with the provider that created it,
     * so {@link #close()} can call the correct {@code destroyNativeStore}.
     */
    private static final class StoreEntry {
        final long ptr;
        final NativeRemoteObjectStoreProvider provider;

        StoreEntry(final long ptr, final NativeRemoteObjectStoreProvider provider) {
            this.ptr = ptr;
            this.provider = provider;
        }
    }
}

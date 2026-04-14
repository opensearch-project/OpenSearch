/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;

/**
 * SPI for plugins that provide native (Rust) remote ObjectStore backends.
 *
 * <p>Each repository plugin that supports native reads implements this interface
 * as a stateless factory. Consumers discover providers via
 * {@code pluginsService.filterPlugins(NativeRemoteObjectStoreProvider.class)}.
 *
 * <p>The returned pointer is an opaque {@code long} representing a
 * {@code Box<Arc<dyn ObjectStore>>} on the Rust side. Callers are responsible
 * for lifecycle management — typically the {@code Repository} that owns the
 * pointer destroys it on close.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface NativeRemoteObjectStoreProvider {

    /**
     * The repository type this provider handles (e.g. "s3", "gcs", "azure", "fs").
     */
    String repositoryType();

    /**
     * Create a native Rust ObjectStore from the given config JSON.
     * Uses the default credential chain on the Rust side.
     *
     * @param configJson JSON string with backend-specific settings
     * @return native store pointer ({@code > 0} on success)
     */
    long createNativeStore(String configJson);

    /**
     * Create a native Rust ObjectStore with a custom credential provider.
     * If {@code credProviderPtr} is 0, uses the default credential chain.
     *
     * @param configJson JSON string with backend-specific settings
     * @param credProviderPtr native credential provider pointer, or 0 for default
     * @return native store pointer ({@code > 0} on success)
     */
    default long createNativeStore(String configJson, long credProviderPtr) {
        return createNativeStore(configJson);
    }

    /**
     * Create a native Rust ObjectStore from repository metadata and node settings.
     *
     * <p>The provider resolves backend-specific configuration (bucket, region,
     * endpoint, credentials) from the repository metadata and node settings
     * (which include keystore values). This is the preferred method when
     * creating native stores from existing repositories.
     *
     * <p>Default implementation returns {@code -1} (not supported). Providers
     * should override this to extract settings and delegate to
     * {@link #createNativeStore(String)}.
     *
     * @param metadata repository metadata with type-specific settings
     * @param nodeSettings node-level settings including keystore entries
     * @return native store pointer ({@code > 0} on success), or {@code -1} if not supported
     */
    default long createNativeStoreFromMetadata(RepositoryMetadata metadata, Settings nodeSettings) {
        return -1;
    }

    /**
     * Free a native store previously created by {@link #createNativeStore}.
     * After this call the pointer is invalid.
     *
     * @param ptr the pointer returned by {@link #createNativeStore}
     */
    void destroyNativeStore(long ptr);
}

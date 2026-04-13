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
 * SPI for plugins that provide native (Rust) remote ObjectStore backends.
 *
 * <p>Each repository plugin that supports native reads implements this interface
 * as a stateless factory. Consumers discover providers via
 * {@code pluginsService.filterPlugins(NativeRemoteObjectStoreProvider.class)}.
 *
 * <p>The returned pointer is an opaque {@code long} representing a
 * {@code Box<Arc<dyn ObjectStore>>} on the Rust side. Callers are responsible
 * for caching and lifecycle management (see {@code NativeRemoteObjectStoreService}).
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
     * Free a native store previously created by {@link #createNativeStore}.
     * After this call the pointer is invalid.
     *
     * @param ptr the pointer returned by {@link #createNativeStore}
     */
    void destroyNativeStore(long ptr);
}

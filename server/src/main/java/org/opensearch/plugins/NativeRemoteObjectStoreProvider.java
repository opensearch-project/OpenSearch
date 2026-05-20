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
import org.opensearch.repositories.NativeStoreRepository;

/**
 * SPI for plugins that provide native (Rust) remote ObjectStore backends.
 *
 * <p>Each repository plugin that supports native reads implements this interface
 * as a stateless factory. Consumers discover providers via
 * {@link org.opensearch.plugins.ExtensiblePlugin} extension loading.
 *
 * <p>Returns a {@link NativeStoreRepository} that wraps the native pointer
 * with use-after-close protection. The repository is {@link AutoCloseable}
 * — callers close it to free the native resource.
 *
 * <p>JSON serialization for the FFM boundary is an implementation detail
 * of each provider plugin, not part of this SPI.
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
     * Create a native Rust ObjectStore from repository metadata and node settings.
     *
     * <p>The provider resolves backend-specific configuration (bucket, region,
     * endpoint, credentials) from the repository metadata and node settings
     * (which include keystore values), serializes it for the FFM boundary,
     * and returns a {@link NativeStoreRepository} wrapping the native pointer.
     *
     * <p>Returns {@link NativeStoreRepository#EMPTY} if native store creation is
     * not supported or the provider cannot create a store for this metadata.
     *
     * @param metadata repository metadata with type-specific settings
     * @param nodeSettings node-level settings including keystore entries
     * @return a live native store, or {@link NativeStoreRepository#EMPTY}
     */
    NativeStoreRepository createNativeStore(RepositoryMetadata metadata, Settings nodeSettings);
}

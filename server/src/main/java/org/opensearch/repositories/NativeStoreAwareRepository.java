/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.NativeRemoteObjectStoreProvider;

/**
 * Mixin interface for {@link Repository} implementations that support native
 * (Rust) object store backends.
 *
 * <p>{@link RepositoriesService} calls {@link #initNativeStore} after
 * {@code repository.start()} if a {@link NativeRemoteObjectStoreProvider}
 * exists for the repository type. The repository owns the native pointer
 * and must destroy it in {@code doClose()}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface NativeStoreAwareRepository {

    /**
     * Initialize the native object store for this repository.
     *
     * <p>Called by {@link RepositoriesService} after the repository is started,
     * if a native provider exists for this repository type. Implementations
     * should store the returned pointer and destroy it on close.
     *
     * @param provider the native store provider for this repository type
     * @param nodeSettings node-level settings (includes keystore for credential resolution)
     */
    void initNativeStore(NativeRemoteObjectStoreProvider provider, Settings nodeSettings);
}

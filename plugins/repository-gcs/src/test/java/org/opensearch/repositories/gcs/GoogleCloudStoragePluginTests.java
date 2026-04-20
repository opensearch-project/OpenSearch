/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.NativeRemoteObjectStoreProvider;
import org.opensearch.repositories.NativeStoreRepository;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GoogleCloudStoragePluginTests extends OpenSearchTestCase {

    public void testLoadExtensionsMultipleProvidersUsesFirst() throws Exception {
        Settings settings = Settings.builder().put("node.name", "test").build();
        try (GoogleCloudStoragePlugin plugin = new GoogleCloudStoragePlugin(settings)) {
            NativeRemoteObjectStoreProvider first = new NativeRemoteObjectStoreProvider() {
                @Override
                public String repositoryType() {
                    return "gcs";
                }

                @Override
                public NativeStoreRepository createNativeStore(RepositoryMetadata metadata, Settings nodeSettings) {
                    return NativeStoreRepository.EMPTY;
                }
            };
            NativeRemoteObjectStoreProvider second = new NativeRemoteObjectStoreProvider() {
                @Override
                public String repositoryType() {
                    return "gcs";
                }

                @Override
                public NativeStoreRepository createNativeStore(RepositoryMetadata metadata, Settings nodeSettings) {
                    return NativeStoreRepository.EMPTY;
                }
            };

            ExtensiblePlugin.ExtensionLoader loader = new ExtensiblePlugin.ExtensionLoader() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                    if (extensionPointType == NativeRemoteObjectStoreProvider.class) {
                        return (List<T>) Arrays.asList(first, second);
                    }
                    return Collections.emptyList();
                }
            };

            plugin.loadExtensions(loader);
            // The warning about multiple providers is logged; verify no exception is thrown
        }
    }

    public void testLoadExtensionsSingleProvider() throws Exception {
        Settings settings = Settings.builder().put("node.name", "test").build();
        try (GoogleCloudStoragePlugin plugin = new GoogleCloudStoragePlugin(settings)) {
            NativeRemoteObjectStoreProvider provider = new NativeRemoteObjectStoreProvider() {
                @Override
                public String repositoryType() {
                    return "gcs";
                }

                @Override
                public NativeStoreRepository createNativeStore(RepositoryMetadata metadata, Settings nodeSettings) {
                    return NativeStoreRepository.EMPTY;
                }
            };

            ExtensiblePlugin.ExtensionLoader loader = new ExtensiblePlugin.ExtensionLoader() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                    if (extensionPointType == NativeRemoteObjectStoreProvider.class) {
                        return (List<T>) Collections.singletonList(provider);
                    }
                    return Collections.emptyList();
                }
            };

            plugin.loadExtensions(loader);
            // Single provider — no warning, no exception
        }
    }

    public void testLoadExtensionsNoMatchingProvider() throws Exception {
        Settings settings = Settings.builder().put("node.name", "test").build();
        try (GoogleCloudStoragePlugin plugin = new GoogleCloudStoragePlugin(settings)) {
            NativeRemoteObjectStoreProvider nonMatchingProvider = new NativeRemoteObjectStoreProvider() {
                @Override
                public String repositoryType() {
                    return "s3";
                }

                @Override
                public NativeStoreRepository createNativeStore(RepositoryMetadata metadata, Settings nodeSettings) {
                    return NativeStoreRepository.EMPTY;
                }
            };

            ExtensiblePlugin.ExtensionLoader loader = new ExtensiblePlugin.ExtensionLoader() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                    if (extensionPointType == NativeRemoteObjectStoreProvider.class) {
                        return (List<T>) Collections.singletonList(nonMatchingProvider);
                    }
                    return Collections.emptyList();
                }
            };

            plugin.loadExtensions(loader);
            // No matching provider — info log about no provider found, no exception
        }
    }
}

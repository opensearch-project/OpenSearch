/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.NativeRemoteObjectStoreProvider;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.repositories.NativeStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;

public class AzureRepositoryPluginTests extends OpenSearchTestCase {

    public void testLoadExtensionsMultipleProvidersUsesFirst() throws Exception {
        Settings settings = Settings.builder().put("node.name", "test").build();
        try (AzureRepositoryPlugin plugin = new AzureRepositoryPlugin(settings)) {
            NativeRemoteObjectStoreProvider first = new NativeRemoteObjectStoreProvider() {
                @Override
                public String repositoryType() {
                    return "azure";
                }

                @Override
                public NativeStoreRepository createNativeStore(RepositoryMetadata metadata, Settings nodeSettings) {
                    return NativeStoreRepository.EMPTY;
                }
            };
            NativeRemoteObjectStoreProvider second = new NativeRemoteObjectStoreProvider() {
                @Override
                public String repositoryType() {
                    return "azure";
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
        try (AzureRepositoryPlugin plugin = new AzureRepositoryPlugin(settings)) {
            NativeRemoteObjectStoreProvider provider = new NativeRemoteObjectStoreProvider() {
                @Override
                public String repositoryType() {
                    return "azure";
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
        try (AzureRepositoryPlugin plugin = new AzureRepositoryPlugin(settings)) {
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

    public void testRepositoryWithLiveNativeStore() {
        AzureStorageService storageService = mock(AzureStorageService.class);
        ClusterService clusterService = BlobStoreTestUtil.mockClusterService();
        Settings repoSettings = Settings.builder().put("container", "test-container").build();
        RepositoryMetadata metadata = new RepositoryMetadata("test", "azure", repoSettings);
        RecoverySettings recoverySettings = new RecoverySettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        AtomicBoolean destroyed = new AtomicBoolean(false);
        NativeStoreHandle liveHandle = new NativeStoreHandle(99L, ptr -> destroyed.set(true));
        NativeStoreRepository liveStore = new NativeStoreRepository(liveHandle);

        NativeRemoteObjectStoreProvider provider = new NativeRemoteObjectStoreProvider() {
            @Override
            public String repositoryType() {
                return "azure";
            }

            @Override
            public NativeStoreRepository createNativeStore(RepositoryMetadata md, Settings nodeSettings) {
                return liveStore;
            }
        };

        AzureRepository repository = new AzureRepository(
            metadata,
            NamedXContentRegistry.EMPTY,
            storageService,
            clusterService,
            recoverySettings,
            provider
        );

        assertFalse("Live store should not have been destroyed", destroyed.get());
        assertSame(liveStore, repository.getNativeStore());
        assertTrue(repository.getNativeStore().isLive());

        // Close the repository to release the native handle
        repository.close();
        assertTrue("Native handle should be destroyed after repository close", destroyed.get());
    }

    public void testRepositoryWithNullProvider() {
        AzureStorageService storageService = mock(AzureStorageService.class);
        ClusterService clusterService = BlobStoreTestUtil.mockClusterService();
        Settings repoSettings = Settings.builder().put("container", "test-container").build();
        RepositoryMetadata metadata = new RepositoryMetadata("test", "azure", repoSettings);
        RecoverySettings recoverySettings = new RecoverySettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        AzureRepository repository = new AzureRepository(
            metadata,
            NamedXContentRegistry.EMPTY,
            storageService,
            clusterService,
            recoverySettings,
            null
        );

        assertSame(NativeStoreRepository.EMPTY, repository.getNativeStore());
    }
}

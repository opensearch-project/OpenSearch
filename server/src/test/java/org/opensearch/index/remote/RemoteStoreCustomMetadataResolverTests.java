/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.Version;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.getRemoteStoreTranslogRepo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteStoreCustomMetadataResolverTests extends OpenSearchTestCase {

    RepositoriesService repositoriesService = mock(RepositoriesService.class);
    Settings settings = Settings.EMPTY;

    public void testGetPathStrategyMinVersionOlder() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), randomFrom(PathType.values())).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_13_0,
            () -> repositoriesService,
            settings
        );
        assertEquals(PathType.FIXED, resolver.getPathStrategy().getType());
        assertNull(resolver.getPathStrategy().getHashAlgorithm());
    }

    public void testGetPathStrategyMinVersionNewer() {
        PathType pathType = randomFrom(PathType.values());
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), pathType).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_14_0,
            () -> repositoriesService,
            settings
        );
        assertEquals(pathType, resolver.getPathStrategy().getType());
        if (pathType.requiresHashAlgorithm()) {
            assertNotNull(resolver.getPathStrategy().getHashAlgorithm());
        } else {
            assertNull(resolver.getPathStrategy().getHashAlgorithm());
        }
    }

    public void testGetPathStrategyStrategy() {
        // FIXED type
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.FIXED).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_14_0,
            () -> repositoriesService,
            settings
        );
        assertEquals(PathType.FIXED, resolver.getPathStrategy().getType());

        // FIXED type with hash algorithm
        settings = Settings.builder()
            .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.FIXED)
            .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), randomFrom(PathHashAlgorithm.values()))
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> Version.V_2_14_0, () -> repositoriesService, settings);
        assertEquals(PathType.FIXED, resolver.getPathStrategy().getType());

        // HASHED_PREFIX type with FNV_1A_COMPOSITE
        settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX).build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> Version.V_2_14_0, () -> repositoriesService, settings);
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.getPathStrategy().getHashAlgorithm());

        // HASHED_PREFIX type with FNV_1A_COMPOSITE
        settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX).build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> Version.V_2_14_0, () -> repositoriesService, settings);
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.getPathStrategy().getHashAlgorithm());

        // HASHED_PREFIX type with FNV_1A_BASE64
        settings = Settings.builder()
            .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX)
            .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> Version.V_2_14_0, () -> repositoriesService, settings);
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.getPathStrategy().getHashAlgorithm());

        // HASHED_PREFIX type with FNV_1A_BASE64
        settings = Settings.builder()
            .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX)
            .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> Version.V_2_14_0, () -> repositoriesService, settings);
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.getPathStrategy().getHashAlgorithm());
    }

    public void testGetPathStrategyStrategyWithDynamicUpdate() {

        // Default value
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_14_0,
            () -> repositoriesService,
            settings
        );
        assertEquals(PathType.FIXED, resolver.getPathStrategy().getType());
        assertNull(resolver.getPathStrategy().getHashAlgorithm());

        // Set HASHED_PREFIX with default hash algorithm
        clusterSettings.applySettings(
            Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX).build()
        );
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.getPathStrategy().getHashAlgorithm());

        // Set HASHED_PREFIX with FNV_1A_BASE64 hash algorithm
        clusterSettings.applySettings(
            Settings.builder()
                .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX)
                .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
                .build()
        );
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.getPathStrategy().getHashAlgorithm());

        // Set HASHED_INFIX with default hash algorithm
        clusterSettings.applySettings(
            Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_INFIX).build()
        );
        assertEquals(PathType.HASHED_INFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.getPathStrategy().getHashAlgorithm());

        // Set HASHED_INFIX with FNV_1A_BASE64 hash algorithm
        clusterSettings.applySettings(
            Settings.builder()
                .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_INFIX)
                .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
                .build()
        );
        assertEquals(PathType.HASHED_INFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.getPathStrategy().getHashAlgorithm());
    }

    public void testTranslogMetadataAllowedTrueWithMinVersionNewer() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        BlobStoreRepository repositoryMock = mock(BlobStoreRepository.class);
        when(repositoriesService.repository(getRemoteStoreTranslogRepo(settings))).thenReturn(repositoryMock);
        BlobStore blobStoreMock = mock(BlobStore.class);
        when(repositoryMock.blobStore()).thenReturn(blobStoreMock);
        when(blobStoreMock.isBlobMetadataEnabled()).thenReturn(true);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_15_0,
            () -> repositoriesService,
            settings
        );
        assertTrue(resolver.isTranslogMetadataEnabled());
    }

    public void testTranslogMetadataAllowedFalseWithMinVersionNewer() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), false).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_15_0,
            () -> repositoriesService,
            settings
        );
        assertFalse(resolver.isTranslogMetadataEnabled());
    }

    public void testTranslogMetadataAllowedMinVersionOlder() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), randomBoolean()).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_14_0,
            () -> repositoriesService,
            settings
        );
        assertFalse(resolver.isTranslogMetadataEnabled());
    }

}

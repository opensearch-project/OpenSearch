/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.Version;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import org.mockito.Mockito;

import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.getRemoteStoreTranslogRepo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteStoreCustomMetadataResolverTests extends OpenSearchTestCase {

    RepositoriesService repositoriesService = mock(RepositoriesService.class);

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
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertNotNull(resolver.getPathStrategy().getHashAlgorithm());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.getPathStrategy().getHashAlgorithm());

        // Set FIXED with null hash algorithm
        clusterSettings.applySettings(Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.FIXED).build());
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

    public void testTranslogMetadataUsesS3HistoricalMinimumVersion() {
        Settings settings = translogMetadataSettings(true);
        BlobStore blobStore = new TestBlobStore(true, Optional.of(Version.V_2_15_0));

        assertFalse(translogMetadataResolver(settings, Version.V_2_14_0, blobStore).isTranslogMetadataEnabled());
        assertTrue(translogMetadataResolver(settings, Version.V_2_15_0, blobStore).isTranslogMetadataEnabled());
    }

    public void testTranslogMetadataUsesAzureMinimumVersion() {
        Settings settings = translogMetadataSettings(true);
        BlobStore blobStore = new TestBlobStore(true, Optional.of(Version.V_3_10_0));

        assertFalse(translogMetadataResolver(settings, Version.V_3_9_1, blobStore).isTranslogMetadataEnabled());
        assertTrue(translogMetadataResolver(settings, Version.V_3_10_0, blobStore).isTranslogMetadataEnabled());
    }

    public void testTranslogMetadataDisabledBySetting() {
        Settings settings = translogMetadataSettings(false);
        BlobStore blobStore = new TestBlobStore(true, Optional.of(Version.V_2_15_0));

        assertFalse(translogMetadataResolver(settings, Version.CURRENT, blobStore).isTranslogMetadataEnabled());
    }

    public void testTranslogMetadataDisabledStore() {
        Settings settings = translogMetadataSettings(true);
        BlobStore blobStore = new TestBlobStore(false, Optional.of(Version.V_2_15_0));

        assertFalse(translogMetadataResolver(settings, Version.CURRENT, blobStore).isTranslogMetadataEnabled());
    }

    public void testTranslogMetadataRequiresDeclaredSupportVersion() {
        Settings settings = translogMetadataSettings(true);
        BlobStore blobStore = new TestBlobStore(true, Optional.empty());

        assertFalse(translogMetadataResolver(settings, Version.CURRENT, blobStore).isTranslogMetadataEnabled());
    }

    public void testTranslogPathFixedPathSetting() {

        // Default settings
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        assertEquals("", remoteStoreSettings.getTranslogPathFixedPrefix());

        // Any other random value
        String randomPrefix = randomAlphaOfLengthBetween(2, 5);
        settings = Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.getKey(), randomPrefix).build();
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        assertEquals(randomPrefix, remoteStoreSettings.getTranslogPathFixedPrefix());

        // Set any other random value, the setting still points to the old value
        clusterSettings.applySettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.getKey(), randomAlphaOfLengthBetween(2, 5))
                .build()
        );
        assertEquals(randomPrefix, remoteStoreSettings.getTranslogPathFixedPrefix());
    }

    public void testSegmentsPathFixedPathSetting() {

        // Default settings
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        assertEquals("", remoteStoreSettings.getSegmentsPathFixedPrefix());

        // Any other random value
        String randomPrefix = randomAlphaOfLengthBetween(2, 5);
        settings = Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.getKey(), randomPrefix).build();
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        assertEquals(randomPrefix, remoteStoreSettings.getSegmentsPathFixedPrefix());

        // Set any other random value, the setting still points to the old value
        clusterSettings.applySettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.getKey(), randomAlphaOfLengthBetween(2, 5))
                .build()
        );
        assertEquals(randomPrefix, remoteStoreSettings.getSegmentsPathFixedPrefix());
    }

    public void testIsRemoteStoreRepoServerSideEncryptionEnabled() {
        Settings settings = Settings.builder().put(CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);

        BlobStoreRepository repositoryMock = mock(BlobStoreRepository.class);
        when(repositoryMock.isSeverSideEncryptionEnabled()).thenReturn(Boolean.TRUE);
        when(repositoriesService.repository(Mockito.any())).thenReturn(repositoryMock);

        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_3_4_0,
            () -> repositoriesService,
            settings
        );
        assertTrue(resolver.isRemoteStoreRepoServerSideEncryptionEnabled());
    }

    public void testIsRemoteStoreRepoServerSideEncryptionDisabled() {
        Settings settings = Settings.builder().put(CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);

        BlobStoreRepository repositoryMock = mock(BlobStoreRepository.class);
        when(repositoryMock.isSeverSideEncryptionEnabled()).thenReturn(Boolean.FALSE);
        when(repositoriesService.repository(Mockito.any())).thenReturn(repositoryMock);

        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_3_4_0,
            () -> repositoriesService,
            settings
        );
        assertFalse(resolver.isRemoteStoreRepoServerSideEncryptionEnabled());
    }

    public void testIsRemoteStoreRepoServerSideEncryptionWithOldVersion() {
        Settings settings = Settings.builder().put(CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        when(repositoriesService.repository(Mockito.any())).thenThrow(new RepositoryMissingException("Repository missing"));

        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_3_1_0,
            () -> repositoriesService,
            settings
        );
        expectThrows(IllegalArgumentException.class, resolver::isRemoteStoreRepoServerSideEncryptionEnabled);
    }

    private Settings translogMetadataSettings(boolean enabled) {
        return Settings.builder()
            .put(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), enabled)
            .put("node.attr.remote_store.translog.repository", "my-translog-repo")
            .build();
    }

    private RemoteStoreCustomMetadataResolver translogMetadataResolver(Settings settings, Version minNodeVersion, BlobStore blobStore) {
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        when(repository.blobStore()).thenReturn(blobStore);
        when(repositoriesService.repository(getRemoteStoreTranslogRepo(settings))).thenReturn(repository);
        return new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> minNodeVersion, () -> repositoriesService, settings);
    }

    private static class TestBlobStore implements BlobStore {
        private final boolean metadataEnabled;
        private final Optional<Version> metadataSupportVersion;

        private TestBlobStore(boolean metadataEnabled, Optional<Version> metadataSupportVersion) {
            this.metadataEnabled = metadataEnabled;
            this.metadataSupportVersion = metadataSupportVersion;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isBlobMetadataEnabled() {
            return metadataEnabled;
        }

        @Override
        public Optional<Version> getBlobMetadataSupportVersion() {
            return metadataSupportVersion;
        }

        @Override
        public void close() {}
    }
}

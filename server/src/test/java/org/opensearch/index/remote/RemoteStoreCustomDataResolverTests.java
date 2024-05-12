/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.Version;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_CKP_UPLOAD_SETTING;

public class RemoteStoreCustomDataResolverTests extends OpenSearchTestCase {

    public void testGetMinVersionOlder() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), randomFrom(PathType.values())).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomDataResolver resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.V_2_13_0);
        assertEquals(PathType.FIXED, resolver.get().getType());
        assertNull(resolver.get().getHashAlgorithm());
    }

    public void testGetMinVersionNewer() {
        PathType pathType = randomFrom(PathType.values());
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), pathType).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomDataResolver resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.V_2_14_0);
        assertEquals(pathType, resolver.get().getType());
        if (pathType.requiresHashAlgorithm()) {
            assertNotNull(resolver.get().getHashAlgorithm());
        } else {
            assertNull(resolver.get().getHashAlgorithm());
        }
    }

    public void testGetStrategy() {
        // FIXED type
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.FIXED).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomDataResolver resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.V_2_14_0);
        assertEquals(PathType.FIXED, resolver.get().getType());

        // FIXED type with hash algorithm
        settings = Settings.builder()
            .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.FIXED)
            .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), randomFrom(PathHashAlgorithm.values()))
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.V_2_14_0);
        assertEquals(PathType.FIXED, resolver.get().getType());

        // HASHED_PREFIX type with FNV_1A_COMPOSITE
        settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX).build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.V_2_14_0);
        assertEquals(PathType.HASHED_PREFIX, resolver.get().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.get().getHashAlgorithm());

        // HASHED_PREFIX type with FNV_1A_COMPOSITE
        settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX).build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.V_2_14_0);
        assertEquals(PathType.HASHED_PREFIX, resolver.get().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.get().getHashAlgorithm());

        // HASHED_PREFIX type with FNV_1A_BASE64
        settings = Settings.builder()
            .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX)
            .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.V_2_14_0);
        assertEquals(PathType.HASHED_PREFIX, resolver.get().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.get().getHashAlgorithm());

        // HASHED_PREFIX type with FNV_1A_BASE64
        settings = Settings.builder()
            .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX)
            .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.V_2_14_0);
        assertEquals(PathType.HASHED_PREFIX, resolver.get().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.get().getHashAlgorithm());
    }

    public void testGetStrategyWithDynamicUpdate() {

        // Default value
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomDataResolver resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.V_2_14_0);
        assertEquals(PathType.FIXED, resolver.get().getType());
        assertNull(resolver.get().getHashAlgorithm());

        // Set HASHED_PREFIX with default hash algorithm
        clusterSettings.applySettings(
            Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX).build()
        );
        assertEquals(PathType.HASHED_PREFIX, resolver.get().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.get().getHashAlgorithm());

        // Set HASHED_PREFIX with FNV_1A_BASE64 hash algorithm
        clusterSettings.applySettings(
            Settings.builder()
                .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX)
                .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
                .build()
        );
        assertEquals(PathType.HASHED_PREFIX, resolver.get().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.get().getHashAlgorithm());

        // Set HASHED_INFIX with default hash algorithm
        clusterSettings.applySettings(
            Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_INFIX).build()
        );
        assertEquals(PathType.HASHED_INFIX, resolver.get().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.get().getHashAlgorithm());

        // Set HASHED_INFIX with FNV_1A_BASE64 hash algorithm
        clusterSettings.applySettings(
            Settings.builder()
                .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_INFIX)
                .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
                .build()
        );
        assertEquals(PathType.HASHED_INFIX, resolver.get().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.get().getHashAlgorithm());
    }

    public void testTranslogCkpAsMetadataAllowedMinVersionNewer() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_TRANSLOG_CKP_UPLOAD_SETTING.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomDataResolver resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.CURRENT);
        assertTrue(resolver.getRemoteStoreTranslogCkpAsMetadataAllowed());
    }

    public void testTranslogCkpAsMetadataAllowed2MinVersionNewer() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_TRANSLOG_CKP_UPLOAD_SETTING.getKey(), false).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomDataResolver resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.CURRENT);
        assertFalse(resolver.getRemoteStoreTranslogCkpAsMetadataAllowed());
    }

    public void testTranslogCkpAsMetadataAllowedMinVersionOlder() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_TRANSLOG_CKP_UPLOAD_SETTING.getKey(), randomBoolean()).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomDataResolver resolver = new RemoteStoreCustomDataResolver(remoteStoreSettings, () -> Version.V_2_14_0);
        assertFalse(resolver.getRemoteStoreTranslogCkpAsMetadataAllowed());
    }

}

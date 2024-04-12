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
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STORE_PATH_PREFIX_TYPE_SETTING;

public class RemoteStorePathStrategyResolverTests extends OpenSearchTestCase {

    public void testGetMinVersionOlder() {
        Settings settings = Settings.builder()
            .put(CLUSTER_REMOTE_STORE_PATH_PREFIX_TYPE_SETTING.getKey(), randomFrom(PathType.values()))
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStorePathStrategyResolver resolver = new RemoteStorePathStrategyResolver(clusterSettings, () -> Version.V_2_13_0);
        assertEquals(PathType.FIXED, resolver.get().getType());
        assertNull(resolver.get().getHashAlgorithm());
    }

    public void testGetMinVersionNewer() {
        PathType pathType = randomFrom(PathType.values());
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_PREFIX_TYPE_SETTING.getKey(), pathType).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStorePathStrategyResolver resolver = new RemoteStorePathStrategyResolver(clusterSettings, () -> Version.CURRENT);
        assertEquals(pathType, resolver.get().getType());
        if (pathType.requiresHashAlgorithm()) {
            assertNotNull(resolver.get().getHashAlgorithm());
        } else {
            assertNull(resolver.get().getHashAlgorithm());
        }

    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs.native_store;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.gcs.GoogleCloudStoragePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration test verifying GCS native store discovery via ExtensiblePlugin + META-INF/services.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class GcsNativeStoreIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                GcsNativeObjectStorePlugin.class.getName(),
                "native GCS object store provider",
                "NA",
                Version.CURRENT,
                "1.8",
                GcsNativeObjectStorePlugin.class.getName(),
                null,
                List.of(GoogleCloudStoragePlugin.class.getName()),
                false
            )
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("gcs.client.default.project_id", "test-project").build();
    }

    public void testGcsRepoGetsNativeStoreViaExtensiblePlugin() {
        client().admin()
            .cluster()
            .preparePutRepository("test-gcs-repo")
            .setType("gcs")
            .setVerify(false)
            .setSettings(Settings.builder().put("bucket", "test-bucket").put("client", "default"))
            .get();

        RepositoriesService repoService = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class);
        Repository repo = repoService.repository("test-gcs-repo");

        long ptr = repo.getNativeStorePtr();
        assertThat("Native store pointer should be > 0", ptr, greaterThan(0L));
        assertEquals("Pointer should be consistent across calls", ptr, repo.getNativeStorePtr());
    }
}

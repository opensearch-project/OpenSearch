/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure.native_store;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.azure.AzureRepositoryPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration test verifying Azure native store discovery via ExtensiblePlugin + META-INF/services.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AzureNativeStoreIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AzureRepositoryPlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                AzureNativeObjectStorePlugin.class.getName(),
                "native Azure object store provider",
                "NA",
                Version.CURRENT,
                "1.8",
                AzureNativeObjectStorePlugin.class.getName(),
                null,
                List.of(AzureRepositoryPlugin.class.getName()),
                false
            )
        );
    }

    public void testAzureRepoGetsNativeStoreViaExtensiblePlugin() {
        internalCluster().startNode();

        client().admin()
            .cluster()
            .preparePutRepository("test-azure-repo")
            .setType("azure")
            .setVerify(false)
            .setSettings(Settings.builder().put("container", "test-container"))
            .get();

        RepositoriesService repoService = internalCluster().getInstance(RepositoriesService.class);
        Repository repo = repoService.repository("test-azure-repo");

        long ptr = repo.getNativeStorePtr();
        assertThat("Native store pointer should be > 0", ptr, greaterThan(0L));
        assertEquals("Pointer should be consistent across calls", ptr, repo.getNativeStorePtr());
    }
}

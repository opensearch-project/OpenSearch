/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.applicationtemplates;

import org.opensearch.cluster.service.applicationtemplates.TestSystemTemplatesRepositoryPlugin;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.mockito.Mockito.when;

public class SystemTemplatesServiceTest extends OpenSearchTestCase {

    private SystemTemplatesService systemTemplatesService;

    public void testSystemTemplatesLoaded() throws IOException {
        setupService(true);

        systemTemplatesService.onClusterManager();
        SystemTemplatesService.Stats stats = systemTemplatesService.stats();
        assertNotNull(stats);
        assertEquals(stats.getTemplatesLoaded(), 1L);
        assertEquals(stats.getFailedLoadingTemplates(), 0L);
        assertEquals(stats.getFailedLoadingRepositories(), 1L);
    }

    public void testSystemTemplatesVerify() throws IOException {
        setupService(false);

        systemTemplatesService.verifyRepositories();

        SystemTemplatesService.Stats stats = systemTemplatesService.stats();
        assertNotNull(stats);
        assertEquals(stats.getTemplatesLoaded(), 0L);
        assertEquals(stats.getFailedLoadingTemplates(), 0L);
        assertEquals(stats.getFailedLoadingRepositories(), 0L);
    }

    public void testSystemTemplatesVerifyWithFailingRepository() throws IOException {
        setupService(true);

        assertThrows(IllegalStateException.class, () -> systemTemplatesService.verifyRepositories());

        SystemTemplatesService.Stats stats = systemTemplatesService.stats();
        assertNotNull(stats);
        assertEquals(stats.getTemplatesLoaded(), 0L);
        assertEquals(stats.getFailedLoadingTemplates(), 0L);
        assertEquals(stats.getFailedLoadingRepositories(), 1L);
    }

    void setupService(boolean errorFromMockPlugin) throws IOException {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES, true).build());

        ThreadPool mockPool = Mockito.mock(ThreadPool.class);
        when(mockPool.generic()).thenReturn(OpenSearchExecutors.newDirectExecutorService());

        List<SystemTemplatesPlugin> plugins = new ArrayList<>();
        plugins.add(new TestSystemTemplatesRepositoryPlugin());

        if (errorFromMockPlugin) {
            SystemTemplatesPlugin mockPlugin = Mockito.mock(SystemTemplatesPlugin.class);
            when(mockPlugin.loadRepository()).thenThrow(new IOException());
            plugins.add(mockPlugin);
        }

        ClusterSettings mockSettings = new ClusterSettings(Settings.EMPTY, BUILT_IN_CLUSTER_SETTINGS);
        systemTemplatesService = new SystemTemplatesService(
            plugins,
            mockPool,
            mockSettings,
            Settings.builder().put(SystemTemplatesService.SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED.getKey(), true).build()
        );
    }
}

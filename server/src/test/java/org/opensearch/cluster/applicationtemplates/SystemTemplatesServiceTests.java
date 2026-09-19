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
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.common.util.FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES;
import static org.mockito.Mockito.when;

public class SystemTemplatesServiceTests extends OpenSearchTestCase {

    private SystemTemplatesService systemTemplatesService;

    @LockFeatureFlag(APPLICATION_BASED_CONFIGURATION_TEMPLATES)
    public void testSystemTemplatesLoaded() throws IOException {
        setupService(true);

        // First time load should happen, second time should short circuit.
        for (int iter = 1; iter <= 2; iter++) {
            systemTemplatesService.onClusterManager();
            SystemTemplatesService.Stats stats = systemTemplatesService.stats();
            assertNotNull(stats);
            assertEquals(stats.getTemplatesLoaded(), iter % 2);
            assertEquals(stats.getFailedLoadingTemplates(), 0L);
            assertEquals(stats.getFailedLoadingRepositories(), iter % 2);
        }
    }

    @LockFeatureFlag(APPLICATION_BASED_CONFIGURATION_TEMPLATES)
    public void testSystemTemplatesVerifyAndLoad() throws IOException {
        setupService(false);

        systemTemplatesService.verifyRepositories();
        SystemTemplatesService.Stats stats = systemTemplatesService.stats();
        assertNotNull(stats);
        assertEquals(stats.getTemplatesLoaded(), 0L);
        assertEquals(stats.getFailedLoadingTemplates(), 0L);
        assertEquals(stats.getFailedLoadingRepositories(), 0L);

        systemTemplatesService.onClusterManager();
        stats = systemTemplatesService.stats();
        assertNotNull(stats);
        assertEquals(stats.getTemplatesLoaded(), 1L);
        assertEquals(stats.getFailedLoadingTemplates(), 0L);
        assertEquals(stats.getFailedLoadingRepositories(), 0L);
    }

    @LockFeatureFlag(APPLICATION_BASED_CONFIGURATION_TEMPLATES)
    public void testSystemTemplatesVerifyWithFailingRepository() throws IOException {
        setupService(true);

        // Do it multiple times to ensure verify checks are always executed.
        for (int i = 0; i < 2; i++) {
            assertThrows(IllegalStateException.class, () -> systemTemplatesService.verifyRepositories());

            SystemTemplatesService.Stats stats = systemTemplatesService.stats();
            assertNotNull(stats);
            assertEquals(stats.getTemplatesLoaded(), 0L);
            assertEquals(stats.getFailedLoadingTemplates(), 0L);
            assertEquals(stats.getFailedLoadingRepositories(), 1L);
        }
    }

    private void setupService(boolean errorFromMockPlugin) throws IOException {
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

    public void testEnablingWithoutFeatureFlagRejectedAtValidation() {
        // Feature flag intentionally NOT locked on: enabling the setting must be rejected during settings-update
        // validation (before commit), not at apply time.
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, BUILT_IN_CLUSTER_SETTINGS);
        Settings enable = Settings.builder()
            .put(SystemTemplatesService.SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED.getKey(), true)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> cs.validate(enable, true));
        assertTrue(e.getMessage().contains("experimental feature"));
    }

    public void testValidatorDoesNotThrowOnDefaultOrDisableWhenFlagOff() {
        // Regression guard for the DoS hazard: settings-update validation runs a setting's validator against its
        // CURRENT/DEFAULT value on every _cluster/settings update. If this validator threw on false, every cluster
        // settings update would be rejected whenever the (off-by-default) experimental flag is disabled. So with the
        // flag off it must accept both the default and an explicit false. Asserting the validator directly, because
        // ClusterSettings.validate only invokes a setting's validator when that setting's key is present in the update.
        SystemTemplatesService.ApplicationTemplatesEnabledValidator validator =
            new SystemTemplatesService.ApplicationTemplatesEnabledValidator();
        validator.validate(false); // default / explicit disable — must not throw with the flag off

        // And an explicit disable through the settings path is accepted too.
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, BUILT_IN_CLUSTER_SETTINGS);
        Settings disable = Settings.builder()
            .put(SystemTemplatesService.SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED.getKey(), false)
            .build();
        cs.validate(disable, true);
    }
}

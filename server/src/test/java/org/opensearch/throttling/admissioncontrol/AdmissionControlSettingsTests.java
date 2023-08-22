/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.admissioncontrol;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.throttling.admissioncontrol.enums.AdmissionControlMode;

import java.util.Arrays;
import java.util.Set;

public class AdmissionControlSettingsTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("admission_controller_settings_test");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testSettingsExists() {
        Set<Setting<?>> settings = ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
        assertTrue(
            "All the admission controller settings should be supported built in settings",
            settings.containsAll(
                Arrays.asList(
                    AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE,
                    AdmissionControlSettings.ADMISSION_CONTROL_FORCE_ENABLE_DEFAULT_SETTING
                )
            )
        );
    }

    public void testDefaultSettings() {
        AdmissionControlSettings admissionControlSettings = new AdmissionControlSettings(
            clusterService.getClusterSettings(),
            Settings.EMPTY
        );

        assertFalse(admissionControlSettings.isTransportLayerAdmissionControlEnabled());
        assertFalse(admissionControlSettings.isTransportLayerAdmissionControlEnforced());
        assertEquals(admissionControlSettings.getAdmissionControlTransportLayerMode().getMode(), AdmissionControlSettings.Defaults.MODE);
        assertFalse(admissionControlSettings.isForceDefaultSettingsEnabled());
    }

    public void testGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
            .build();

        AdmissionControlSettings admissionControlSettings = new AdmissionControlSettings(clusterService.getClusterSettings(), settings);

        assertTrue(admissionControlSettings.isTransportLayerAdmissionControlEnabled());
        assertTrue(admissionControlSettings.isTransportLayerAdmissionControlEnforced());
        assertFalse(admissionControlSettings.isForceDefaultSettingsEnabled());
    }

    public void testUpdateAfterGetDefaultSettings() {
        AdmissionControlSettings admissionControlSettings = new AdmissionControlSettings(
            clusterService.getClusterSettings(),
            Settings.EMPTY
        );
        Settings settings = Settings.builder()
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.MONITOR.getMode())
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        assertTrue(admissionControlSettings.isTransportLayerAdmissionControlEnabled());
        assertFalse(admissionControlSettings.isTransportLayerAdmissionControlEnforced());
        assertFalse(admissionControlSettings.isForceDefaultSettingsEnabled());
    }

    public void testUpdateAfterGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.MONITOR.getMode())
            .build();

        AdmissionControlSettings admissionControlSettings = new AdmissionControlSettings(clusterService.getClusterSettings(), settings);

        Settings newSettings = Settings.builder()
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
            .build();

        clusterService.getClusterSettings().applySettings(newSettings);

        assertTrue(admissionControlSettings.isTransportLayerAdmissionControlEnabled());
        assertTrue(admissionControlSettings.isTransportLayerAdmissionControlEnforced());
        assertFalse(admissionControlSettings.isForceDefaultSettingsEnabled());

        newSettings = Settings.builder()
            .put(newSettings)
            .put(AdmissionControlSettings.ADMISSION_CONTROL_FORCE_ENABLE_DEFAULT_SETTING.getKey(), true)
            .build();

        clusterService.getClusterSettings().applySettings(newSettings);
        assertTrue(admissionControlSettings.isForceDefaultSettingsEnabled());
        assertTrue(admissionControlSettings.isTransportLayerAdmissionControlEnabled());
        assertTrue(admissionControlSettings.isTransportLayerAdmissionControlEnforced());
    }
}

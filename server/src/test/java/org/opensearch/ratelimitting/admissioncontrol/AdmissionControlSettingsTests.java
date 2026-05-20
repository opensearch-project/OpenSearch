/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Set;

public class AdmissionControlSettingsTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("admission_controller_settings_test");
        clusterService = ClusterServiceUtils.createClusterService(
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
            settings.containsAll(List.of(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE))
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
    }

    public void testGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
            .build();

        AdmissionControlSettings admissionControlSettings = new AdmissionControlSettings(clusterService.getClusterSettings(), settings);

        assertTrue(admissionControlSettings.isTransportLayerAdmissionControlEnabled());
        assertTrue(admissionControlSettings.isTransportLayerAdmissionControlEnforced());
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
    }
}

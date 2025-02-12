/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import static org.mockito.Mockito.mock;

public class RemoteStorePressureSettingsTests extends OpenSearchTestCase {

    private ClusterService clusterService;

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_refresh_segment_pressure_settings_test");
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

    public void testGetDefaultSettings() {
        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(
            clusterService,
            Settings.EMPTY,
            mock(RemoteStorePressureService.class)
        );

        // Check remote refresh segment pressure enabled is false
        assertTrue(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold default value
        assertEquals(10.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold default value
        assertEquals(10.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit default value
        assertEquals(5, pressureSettings.getMinConsecutiveFailuresLimit());
    }

    public void testGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .build();
        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(
            clusterService,
            settings,
            mock(RemoteStorePressureService.class)
        );

        // Check remote refresh segment pressure enabled is true
        assertTrue(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold configured value
        assertEquals(50.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold configured value
        assertEquals(60.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit configured value
        assertEquals(121, pressureSettings.getMinConsecutiveFailuresLimit());
    }

    public void testUpdateAfterGetDefaultSettings() {
        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(
            clusterService,
            Settings.EMPTY,
            mock(RemoteStorePressureService.class)
        );

        Settings newSettings = Settings.builder()
            .put(RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);

        // Check updated remote refresh segment pressure enabled is false
        assertTrue(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold updated
        assertEquals(50.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold updated
        assertEquals(60.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit updated
        assertEquals(121, pressureSettings.getMinConsecutiveFailuresLimit());
    }

    public void testUpdateAfterGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .build();
        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(
            clusterService,
            settings,
            mock(RemoteStorePressureService.class)
        );

        Settings newSettings = Settings.builder()
            .put(RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 40.0)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 111)
            .build();

        clusterService.getClusterSettings().applySettings(newSettings);

        // Check updated remote refresh segment pressure enabled is true
        assertTrue(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold updated
        assertEquals(40.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold updated
        assertEquals(50.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit updated
        assertEquals(111, pressureSettings.getMinConsecutiveFailuresLimit());
    }
}

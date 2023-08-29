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
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class RemoteStorePressureSettingsTests extends OpenSearchTestCase {

    private ClusterService clusterService;

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_refresh_segment_pressure_settings_test");
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

    public void testGetDefaultSettings() {
        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(
            clusterService,
            Settings.EMPTY,
            mock(RemoteStorePressureService.class)
        );

        // Check remote refresh segment pressure enabled is false
        assertFalse(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold default value
        assertEquals(10.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold default value
        assertEquals(10.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit default value
        assertEquals(5, pressureSettings.getMinConsecutiveFailuresLimit());

        // Check moving average window size default value
        assertEquals(20, pressureSettings.getMovingAverageWindowSize());
    }

    public void testGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .put(
                RemoteStorePressureSettings.MOVING_AVERAGE_WINDOW_SIZE.getKey(),
                RemoteStorePressureSettings.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE
            )
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

        // Check moving average window size configured value
        assertEquals(
            RemoteStorePressureSettings.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            pressureSettings.getMovingAverageWindowSize()
        );
    }

    public void testInvalidMovingAverageWindowSize() {
        Settings settings = Settings.builder()
            .put(
                RemoteStorePressureSettings.MOVING_AVERAGE_WINDOW_SIZE.getKey(),
                RemoteStorePressureSettings.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE - 1
            )
            .build();
        assertThrows(
            "Failed to parse value",
            IllegalArgumentException.class,
            () -> new RemoteStorePressureSettings(clusterService, settings, mock(RemoteStorePressureService.class))
        );
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
            .put(RemoteStorePressureSettings.MOVING_AVERAGE_WINDOW_SIZE.getKey(), 102)
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

        // Check moving average window size updated
        assertEquals(102, pressureSettings.getMovingAverageWindowSize());
    }

    public void testUpdateAfterGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .put(RemoteStorePressureSettings.MOVING_AVERAGE_WINDOW_SIZE.getKey(), 102)
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
            .put(RemoteStorePressureSettings.MOVING_AVERAGE_WINDOW_SIZE.getKey(), 112)
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

        // Check moving average window size updated
        assertEquals(112, pressureSettings.getMovingAverageWindowSize());
    }

    public void testUpdateTriggeredInRemotePressureServiceOnUpdateSettings() {

        int toUpdateVal1 = 1121;

        AtomicInteger movingAverageWindowSize = new AtomicInteger();

        RemoteStorePressureService pressureService = mock(RemoteStorePressureService.class);

        // Upload bytes
        doAnswer(invocation -> {
            movingAverageWindowSize.set(invocation.getArgument(0));
            return null;
        }).when(pressureService).updateMovingAverageWindowSize(anyInt());

        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(clusterService, Settings.EMPTY, pressureService);
        Settings newSettings = Settings.builder()
            .put(RemoteStorePressureSettings.MOVING_AVERAGE_WINDOW_SIZE.getKey(), toUpdateVal1)
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);

        // Assertions
        assertEquals(toUpdateVal1, pressureSettings.getMovingAverageWindowSize());
        assertEquals(toUpdateVal1, movingAverageWindowSize.get());
    }
}

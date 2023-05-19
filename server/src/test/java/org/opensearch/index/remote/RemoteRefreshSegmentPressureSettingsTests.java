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

public class RemoteRefreshSegmentPressureSettingsTests extends OpenSearchTestCase {

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
        RemoteRefreshSegmentPressureSettings pressureSettings = new RemoteRefreshSegmentPressureSettings(
            clusterService,
            Settings.EMPTY,
            mock(RemoteRefreshSegmentPressureService.class)
        );

        // Check remote refresh segment pressure enabled is false
        assertFalse(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold default value
        assertEquals(10.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold default value
        assertEquals(10.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit default value
        assertEquals(5, pressureSettings.getMinConsecutiveFailuresLimit());

        // Check upload bytes moving average window size default value
        assertEquals(20, pressureSettings.getUploadBytesMovingAverageWindowSize());

        // Check upload bytes per sec moving average window size default value
        assertEquals(20, pressureSettings.getUploadBytesPerSecMovingAverageWindowSize());

        // Check upload time moving average window size default value
        assertEquals(20, pressureSettings.getUploadTimeMovingAverageWindowSize());
    }

    public void testGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(RemoteRefreshSegmentPressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteRefreshSegmentPressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteRefreshSegmentPressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 102)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 103)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 104)
            .build();
        RemoteRefreshSegmentPressureSettings pressureSettings = new RemoteRefreshSegmentPressureSettings(
            clusterService,
            settings,
            mock(RemoteRefreshSegmentPressureService.class)
        );

        // Check remote refresh segment pressure enabled is true
        assertTrue(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold configured value
        assertEquals(50.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold configured value
        assertEquals(60.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit configured value
        assertEquals(121, pressureSettings.getMinConsecutiveFailuresLimit());

        // Check upload bytes moving average window size configured value
        assertEquals(102, pressureSettings.getUploadBytesMovingAverageWindowSize());

        // Check upload bytes per sec moving average window size configured value
        assertEquals(103, pressureSettings.getUploadBytesPerSecMovingAverageWindowSize());

        // Check upload time moving average window size configured value
        assertEquals(104, pressureSettings.getUploadTimeMovingAverageWindowSize());
    }

    public void testUpdateAfterGetDefaultSettings() {
        RemoteRefreshSegmentPressureSettings pressureSettings = new RemoteRefreshSegmentPressureSettings(
            clusterService,
            Settings.EMPTY,
            mock(RemoteRefreshSegmentPressureService.class)
        );

        Settings newSettings = Settings.builder()
            .put(RemoteRefreshSegmentPressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteRefreshSegmentPressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteRefreshSegmentPressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 102)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 103)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 104)
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

        // Check upload bytes moving average window size updated
        assertEquals(102, pressureSettings.getUploadBytesMovingAverageWindowSize());

        // Check upload bytes per sec moving average window size updated
        assertEquals(103, pressureSettings.getUploadBytesPerSecMovingAverageWindowSize());

        // Check upload time moving average window size updated
        assertEquals(104, pressureSettings.getUploadTimeMovingAverageWindowSize());
    }

    public void testUpdateAfterGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(RemoteRefreshSegmentPressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteRefreshSegmentPressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteRefreshSegmentPressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 102)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 103)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 104)
            .build();
        RemoteRefreshSegmentPressureSettings pressureSettings = new RemoteRefreshSegmentPressureSettings(
            clusterService,
            settings,
            mock(RemoteRefreshSegmentPressureService.class)
        );

        Settings newSettings = Settings.builder()
            .put(RemoteRefreshSegmentPressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 40.0)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteRefreshSegmentPressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 111)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 112)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 113)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 114)
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

        // Check upload bytes moving average window size updated
        assertEquals(112, pressureSettings.getUploadBytesMovingAverageWindowSize());

        // Check upload bytes per sec moving average window size updated
        assertEquals(113, pressureSettings.getUploadBytesPerSecMovingAverageWindowSize());

        // Check upload time moving average window size updated
        assertEquals(114, pressureSettings.getUploadTimeMovingAverageWindowSize());
    }

    public void testUpdateTriggeredInRemotePressureServiceOnUpdateSettings() {

        int toUpdateVal1 = 1121, toUpdateVal2 = 1123, toUpdateVal3 = 1125;

        AtomicInteger updatedUploadBytesWindowSize = new AtomicInteger();
        AtomicInteger updatedUploadBytesPerSecWindowSize = new AtomicInteger();
        AtomicInteger updatedUploadTimeWindowSize = new AtomicInteger();

        RemoteRefreshSegmentPressureService pressureService = mock(RemoteRefreshSegmentPressureService.class);

        // Upload bytes
        doAnswer(invocation -> {
            updatedUploadBytesWindowSize.set(invocation.getArgument(0));
            return null;
        }).when(pressureService).updateUploadBytesMovingAverageWindowSize(anyInt());

        // Upload bytes per sec
        doAnswer(invocation -> {
            updatedUploadBytesPerSecWindowSize.set(invocation.getArgument(0));
            return null;
        }).when(pressureService).updateUploadBytesPerSecMovingAverageWindowSize(anyInt());

        // Upload time
        doAnswer(invocation -> {
            updatedUploadTimeWindowSize.set(invocation.getArgument(0));
            return null;
        }).when(pressureService).updateUploadTimeMsMovingAverageWindowSize(anyInt());

        RemoteRefreshSegmentPressureSettings pressureSettings = new RemoteRefreshSegmentPressureSettings(
            clusterService,
            Settings.EMPTY,
            pressureService
        );
        Settings newSettings = Settings.builder()
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), toUpdateVal1)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), toUpdateVal2)
            .put(RemoteRefreshSegmentPressureSettings.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), toUpdateVal3)
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);

        // Assertions
        assertEquals(toUpdateVal1, pressureSettings.getUploadBytesMovingAverageWindowSize());
        assertEquals(toUpdateVal1, updatedUploadBytesWindowSize.get());
        assertEquals(toUpdateVal2, pressureSettings.getUploadBytesPerSecMovingAverageWindowSize());
        assertEquals(toUpdateVal2, updatedUploadBytesPerSecWindowSize.get());
        assertEquals(toUpdateVal3, pressureSettings.getUploadTimeMovingAverageWindowSize());
        assertEquals(toUpdateVal3, updatedUploadTimeWindowSize.get());
    }
}

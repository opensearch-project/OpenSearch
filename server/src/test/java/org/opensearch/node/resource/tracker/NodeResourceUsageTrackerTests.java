/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

/**
 * Tests to assert resource usage trackers retrieving resource utilization averages
 */
public class NodeResourceUsageTrackerTests extends OpenSearchSingleNodeTestCase {
    ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    public void testStats() throws Exception {
        Settings settings = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), new TimeValue(500, TimeUnit.MILLISECONDS))
            .build();
        NodeResourceUsageTracker tracker = new NodeResourceUsageTracker(
            mock(FsService.class),
            threadPool,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        tracker.start();
        /**
         * Asserting memory utilization to be greater than 0
         * cpu percent used is mostly 0, so skipping assertion for that
         */
        assertBusy(() -> assertThat(tracker.getMemoryUtilizationPercent(), greaterThan(0.0)), 5, TimeUnit.SECONDS);
        tracker.stop();
        tracker.close();
    }

    public void testUpdateSettings() {
        NodeResourceUsageTracker tracker = new NodeResourceUsageTracker(
            mock(FsService.class),
            threadPool,
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        assertEquals(tracker.getResourceTrackerSettings().getCpuWindowDuration().getSeconds(), 30);
        assertEquals(tracker.getResourceTrackerSettings().getMemoryWindowDuration().getSeconds(), 30);
        assertEquals(tracker.getResourceTrackerSettings().getIoWindowDuration().getSeconds(), 120);

        Settings settings = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), "10s")
            .build();
        ClusterUpdateSettingsResponse response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
        assertEquals(
            "10s",
            response.getPersistentSettings().get(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey())
        );

        Settings jvmsettings = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), "5s")
            .build();
        response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(jvmsettings).get();
        assertEquals(
            "5s",
            response.getPersistentSettings().get(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey())
        );
        Settings ioSettings = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), "20s")
            .build();
        response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(ioSettings).get();
        assertEquals(
            "20s",
            response.getPersistentSettings().get(ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING.getKey())
        );
    }
}

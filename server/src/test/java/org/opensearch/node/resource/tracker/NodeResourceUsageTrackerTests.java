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
import static org.hamcrest.Matchers.equalTo;
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
        assertEquals(tracker.getResourceTrackerSettings().getNativeMemoryWindowDuration().getSeconds(), 30);

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

        Settings nativeMemorySettings = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), "15s")
            .build();
        response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(nativeMemorySettings).get();
        assertEquals(
            "15s",
            response.getPersistentSettings().get(ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING.getKey())
        );
    }

    public void testNativeMemoryLimitAndBufferDynamicUpdate() throws Exception {
        // Default for node.native_memory.limit is now ram - heap (machine-dependent), so the
        // "limit=0 bytes" baseline this test wants to start from must be set explicitly.
        Settings unconfiguredAc = Settings.builder().put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "0b").build();
        NodeResourceUsageTracker tracker = new NodeResourceUsageTracker(
            mock(FsService.class),
            threadPool,
            unconfiguredAc,
            new ClusterSettings(unconfiguredAc, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        try {
            // Explicitly unconfigured: limit=0 bytes, buffer=0 percent.
            assertThat(tracker.getResourceTrackerSettings().getNativeMemoryLimitBytes(), equalTo(0L));
            assertThat(tracker.getResourceTrackerSettings().getNativeMemoryBufferPercent(), equalTo(0));

            // Update both via cluster settings API and verify they propagate to the holder.
            Settings updated = Settings.builder()
                .put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "2GB")
                .put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_BUFFER_PERCENT_SETTING.getKey(), 25)
                .build();
            ClusterUpdateSettingsResponse response = client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(updated)
                .get();
            assertEquals("2GB", response.getPersistentSettings().get(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey()));
            assertEquals(
                "25",
                response.getPersistentSettings().get(ResourceTrackerSettings.NODE_NATIVE_MEMORY_BUFFER_PERCENT_SETTING.getKey())
            );

            // The holder is owned by the tracker constructed in this test, so the single-node
            // cluster's update consumer does not reach it directly. Exercise the setter path
            // explicitly to prove it writes to the volatile fields used by the tracker.
            tracker.getResourceTrackerSettings().setNativeMemoryLimitBytes(2L * 1024 * 1024 * 1024);
            tracker.getResourceTrackerSettings().setNativeMemoryBufferPercent(25);
            assertThat(tracker.getResourceTrackerSettings().getNativeMemoryLimitBytes(), equalTo(2L * 1024 * 1024 * 1024));
            assertThat(tracker.getResourceTrackerSettings().getNativeMemoryBufferPercent(), equalTo(25));
        } finally {
            tracker.close();
        }
    }

    /**
     * Exercises {@link ResourceTrackerSettings#deriveNativeMemoryLimitDefault(long, long)}
     * fallback paths so the {@code "0b"} branch is covered without depending on the
     * test machine's RAM/heap configuration. The single-arg overload that reads from
     * {@link org.opensearch.monitor.os.OsProbe} delegates to this one.
     */
    public void testDeriveNativeMemoryLimitDefaultFallbackPaths() {
        // Probe returned 0 (restricted container, /proc/meminfo unreadable).
        assertEquals("unknown ram → unconfigured", "0b", ResourceTrackerSettings.deriveNativeMemoryLimitDefault(0, 1L << 30));
        // Negative ram (defensive against probe quirks).
        assertEquals("negative ram → unconfigured", "0b", ResourceTrackerSettings.deriveNativeMemoryLimitDefault(-1, 1L << 30));
        // Heap >= ram (degenerate config, e.g. -Xmx larger than container limit).
        long ram = 8L * 1024 * 1024 * 1024;
        assertEquals("heap >= ram → unconfigured", "0b", ResourceTrackerSettings.deriveNativeMemoryLimitDefault(ram, ram));
        assertEquals("heap > ram → unconfigured", "0b", ResourceTrackerSettings.deriveNativeMemoryLimitDefault(ram, ram + 1));
        // Happy path: 64 GB / 16 GB heap → 79% of (RAM - heap) = 79% of 48 GB.
        long sixtyFourGB = 64L * 1024 * 1024 * 1024;
        long sixteenGB = 16L * 1024 * 1024 * 1024;
        long expected = (sixtyFourGB - sixteenGB) * 79 / 100;
        assertEquals(
            "happy path → 79% of (ram - heap)",
            expected + "b",
            ResourceTrackerSettings.deriveNativeMemoryLimitDefault(sixtyFourGB, sixteenGB)
        );
    }
}

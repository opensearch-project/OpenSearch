/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTask;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.backpressure.NativeMemoryUsageService;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.settings.SearchTaskSettings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Optional;

import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;

public class CpuUsageTrackerTests extends OpenSearchTestCase {
    private static final SearchBackpressureSettings mockSettings = new SearchBackpressureSettings(
        Settings.builder()
            .put(SearchShardTaskSettings.SETTING_CPU_TIME_MILLIS_THRESHOLD.getKey(), 15)  // 15 ms
            .put(SearchTaskSettings.SETTING_CPU_TIME_MILLIS_THRESHOLD.getKey(), 25)   // 25 ms
            .build(),
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    @Before
    public void resetServiceBefore() {
        // isCpuTrackingSupported() reads the singleton; clear it so a leak from another test
        // doesn't make the gate look "off" when it should be on.
        NativeMemoryUsageService.getInstance().resetForTesting();
    }

    @After
    public void resetServiceAfter() {
        NativeMemoryUsageService.getInstance().resetForTesting();
    }

    public void testSearchTaskEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(SearchTask.class, 100000000, 200, randomNonNegativeLong());
        CpuUsageTracker tracker = new CpuUsageTracker(mockSettings.getSearchTaskSettings()::getCpuTimeNanosThreshold);

        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(1, reason.get().getCancellationScore());
        assertEquals("cpu usage exceeded [100ms >= 25ms]", reason.get().getMessage());
    }

    public void testSearchShardTaskEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 200000000, 200, randomNonNegativeLong());
        CpuUsageTracker tracker = new CpuUsageTracker(mockSettings.getSearchShardTaskSettings()::getCpuTimeNanosThreshold);

        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(1, reason.get().getCancellationScore());
        assertEquals("cpu usage exceeded [200ms >= 15ms]", reason.get().getMessage());
    }

    public void testNotEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 5000000, 200, randomNonNegativeLong());
        CpuUsageTracker tracker = new CpuUsageTracker(mockSettings.getSearchShardTaskSettings()::getCpuTimeNanosThreshold);

        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }

    public void testIsCpuTrackingSupportedDefaultsToTrue() {
        // No analytics backend has installed a snapshot supplier — CPU tracking must remain on.
        // resetServiceBefore() guarantees a clean state.
        assertTrue(CpuUsageTracker.isCpuTrackingSupported());
    }

    public void testIsCpuTrackingSupportedDisabledWhenBackendInstalled() {
        // Once any backend installs a snapshot supplier (the proxy for "analytics backend present"),
        // the JVM-thread-based CPU tracker is no longer meaningful — the native-memory tracker takes
        // over the resource-protection role.
        NativeMemoryUsageService.getInstance().setSnapshotSupplier(Collections::emptyMap);
        assertFalse(CpuUsageTracker.isCpuTrackingSupported());
    }

    public void testIsCpuTrackingSupportedReEnabledAfterReset() {
        // After clearing state (simulating a no-backend node) the gate must flip back to true.
        // This protects future refactors that change visibility/lifecycle of the supplier flag.
        NativeMemoryUsageService.getInstance().setSnapshotSupplier(Collections::emptyMap);
        assertFalse(CpuUsageTracker.isCpuTrackingSupported());

        NativeMemoryUsageService.getInstance().resetForTesting();
        assertTrue(CpuUsageTracker.isCpuTrackingSupported());
    }
}

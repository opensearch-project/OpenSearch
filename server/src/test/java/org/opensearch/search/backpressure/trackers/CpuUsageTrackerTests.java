/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;

public class CpuUsageTrackerTests extends OpenSearchTestCase {
    private static final SearchBackpressureSettings mockSettings = new SearchBackpressureSettings(
        Settings.builder()
            .put(CpuUsageTracker.SETTING_CPU_TIME_MILLIS_THRESHOLD.getKey(), 15)  // 15 ms
            .build(),
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    public void testEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 200000000, 200);
        CpuUsageTracker tracker = new CpuUsageTracker(mockSettings);

        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(1, reason.get().getCancellationScore());
        assertEquals("cpu usage exceeded [200ms >= 15ms]", reason.get().getMessage());
    }

    public void testNotEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 5000000, 200);
        CpuUsageTracker tracker = new CpuUsageTracker(mockSettings);

        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }
}

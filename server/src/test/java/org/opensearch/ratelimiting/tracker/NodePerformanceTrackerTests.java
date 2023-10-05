/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimiting.tracker;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests to assert node performance trackers retrieving resource utilization averages
 */
public class NodePerformanceTrackerTests extends OpenSearchTestCase {
    ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    public void testStats() throws Exception {
        Settings settings = Settings.builder()
            .put(PerformanceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), new TimeValue(500, TimeUnit.MILLISECONDS))
            .build();
        NodePerformanceTracker tracker = new NodePerformanceTracker(
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
}

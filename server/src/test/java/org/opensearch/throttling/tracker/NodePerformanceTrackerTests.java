/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.tracker;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.PerformanceCollectorService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }

    public void testStats() throws InterruptedException {
        PerformanceCollectorService performanceCollectorService = new PerformanceCollectorService(mockClusterService());

        NodePerformanceTracker tracker = new NodePerformanceTracker(
            performanceCollectorService,
            threadPool,
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        tracker.start();
        Thread.sleep(2000);
        double memory = tracker.getMemoryPercentUsed();
        assertThat(tracker.getMemoryPercentUsed(), greaterThan(0.0));
        // cpu percent used is mostly 0, so skipping assertion for that
        tracker.stop();
        assertTrue(performanceCollectorService.getNodeStatistics(NodePerformanceTracker.LOCAL_NODE).isPresent());
        PerformanceCollectorService.NodePerformanceStatistics perfStats = performanceCollectorService.getNodeStatistics(
            NodePerformanceTracker.LOCAL_NODE
        ).get();
        assertEquals(memory, perfStats.getMemoryPercent(), 0.0);
        assertTrue(performanceCollectorService.getNodeStatistics("Invalid").isEmpty());
    }
}

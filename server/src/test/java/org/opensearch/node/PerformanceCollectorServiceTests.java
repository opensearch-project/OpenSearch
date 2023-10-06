/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.ratelimiting.tracker.NodePerformanceTracker;
import org.opensearch.ratelimiting.tracker.PerformanceTrackerSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for PerformanceCollectorService where we test collect method, get method and whether schedulers
 * are working as expected
 */
public class PerformanceCollectorServiceTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private PerformanceCollectorService collector;
    private ThreadPool threadpool;
    NodePerformanceTracker tracker;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        threadpool = new TestThreadPool("performance_collector_tests");

        clusterService = createClusterService(threadpool);

        Settings settings = Settings.builder()
            .put(PerformanceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), new TimeValue(500, TimeUnit.MILLISECONDS))
            .build();
        tracker = new NodePerformanceTracker(
            threadpool,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        collector = new PerformanceCollectorService(tracker, clusterService, threadpool);
        tracker.start();
        collector.start();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadpool.shutdownNow();
        clusterService.close();
        collector.stop();
        tracker.stop();
        collector.close();
        tracker.close();
    }

    public void testNodePerformanceStats() {
        collector.collectNodePerfStatistics("node1", System.currentTimeMillis(), 97, 99);
        Map<String, NodePerformanceStats> nodeStats = collector.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertEquals(99.0, nodeStats.get("node1").cpuUtilizationPercent, 0.0);
        assertEquals(97.0, nodeStats.get("node1").memoryUtilizationPercent, 0.0);

        Optional<NodePerformanceStats> nodePerformanceStatistics = collector.getNodeStatistics("node1");

        assertNotNull(nodePerformanceStatistics.get());
        assertEquals(99.0, nodePerformanceStatistics.get().cpuUtilizationPercent, 0.0);
        assertEquals(97.0, nodePerformanceStatistics.get().memoryUtilizationPercent, 0.0);

        nodePerformanceStatistics = collector.getNodeStatistics("node2");
        assertTrue(nodePerformanceStatistics.isEmpty());
    }

    public void testScheduler() throws Exception {
        /**
         * Wait for cluster state to be ready so that localNode().getId() is ready and we add the values to the map
         */
        assertBusy(() -> assertTrue(collector.getNodeStatistics(clusterService.localNode().getId()).isPresent()), 5, TimeUnit.SECONDS);
        assertTrue(collector.getNodeStatistics(clusterService.localNode().getId()).isPresent());
        /**
         * Wait for memory utilization to be reported greater than 0
         */
        assertBusy(
            () -> assertThat(
                collector.getNodeStatistics(clusterService.localNode().getId()).get().getMemoryUtilizationPercent(),
                greaterThan(0.0)
            ),
            5,
            TimeUnit.SECONDS
        );
        assertTrue(collector.getNodeStatistics("Invalid").isEmpty());
    }

    /*
     * Test that concurrently adding values and removing nodes does not cause exceptions
     */
    public void testConcurrentAddingAndRemovingNodes() throws Exception {
        String[] nodes = new String[] { "a", "b", "c", "d" };

        final CountDownLatch latch = new CountDownLatch(5);

        Runnable f = () -> {
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                fail("should not be interrupted");
            }
            for (int i = 0; i < randomIntBetween(100, 200); i++) {
                if (randomBoolean()) {
                    collector.removeNodePerfStatistics(randomFrom(nodes));
                }
                collector.collectNodePerfStatistics(
                    randomFrom(nodes),
                    System.currentTimeMillis(),
                    randomIntBetween(1, 100),
                    randomIntBetween(1, 100)
                );
            }
        };

        Thread t1 = new Thread(f);
        Thread t2 = new Thread(f);
        Thread t3 = new Thread(f);
        Thread t4 = new Thread(f);

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        latch.countDown();
        t1.join();
        t2.join();
        t3.join();
        t4.join();

        final Map<String, NodePerformanceStats> nodeStats = collector.getAllNodeStatistics();
        logger.info("--> got stats: {}", nodeStats);
        for (String nodeId : nodes) {
            if (nodeStats.containsKey(nodeId)) {
                assertThat(nodeStats.get(nodeId).memoryUtilizationPercent, greaterThan(0.0));
                assertThat(nodeStats.get(nodeId).cpuUtilizationPercent, greaterThan(0.0));
            }
        }
    }

    public void testNodeRemoval() {
        collector.collectNodePerfStatistics("node1", System.currentTimeMillis(), randomIntBetween(1, 100), randomIntBetween(1, 100));
        collector.collectNodePerfStatistics("node2", System.currentTimeMillis(), randomIntBetween(1, 100), randomIntBetween(1, 100));

        ClusterState previousState = ClusterState.builder(new ClusterName("cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "node1"))
                    .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9201), "node2"))
            )
            .build();
        ClusterState newState = ClusterState.builder(previousState)
            .nodes(DiscoveryNodes.builder(previousState.nodes()).remove("node2"))
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, previousState);

        collector.clusterChanged(event);
        final Map<String, NodePerformanceStats> nodeStats = collector.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertFalse(nodeStats.containsKey("node2"));
    }
}

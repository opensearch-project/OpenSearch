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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.After;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.ratelimitting.admissioncontrol.AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for ResourceUsageCollectorService where we test collect method, get method and whether schedulers
 * are working as expected
 */
public class ResourceUsageCollectorServiceTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500))
            .put(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500))
            .put(ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(5000))
            .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED)
            .build();
    }

    @After
    public void cleanup() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    public void testResourceUsageStats() {
        ResourceUsageCollectorService resourceUsageCollectorService = getInstanceFromNode(ResourceUsageCollectorService.class);
        resourceUsageCollectorService.collectNodeResourceUsageStats("node1", System.currentTimeMillis(), 97, 99, new IoUsageStats(98));
        Map<String, NodeResourceUsageStats> nodeStats = resourceUsageCollectorService.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertEquals(99.0, nodeStats.get("node1").cpuUtilizationPercent, 0.0);
        assertEquals(97.0, nodeStats.get("node1").memoryUtilizationPercent, 0.0);
        assertEquals(98, nodeStats.get("node1").getIoUsageStats().getIoUtilisationPercent(), 0.0);

        Optional<NodeResourceUsageStats> nodeResourceUsageStatsOptional = resourceUsageCollectorService.getNodeStatistics("node1");

        assertNotNull(nodeResourceUsageStatsOptional.get());
        assertEquals(99.0, nodeResourceUsageStatsOptional.get().cpuUtilizationPercent, 0.0);
        assertEquals(97.0, nodeResourceUsageStatsOptional.get().memoryUtilizationPercent, 0.0);
        assertEquals(98, nodeResourceUsageStatsOptional.get().getIoUsageStats().getIoUtilisationPercent(), 0.0);

        nodeResourceUsageStatsOptional = resourceUsageCollectorService.getNodeStatistics("node2");
        assertTrue(nodeResourceUsageStatsOptional.isEmpty());
    }

    public void testScheduler() throws Exception {
        /**
         * Wait for cluster state to be ready so that localNode().getId() is ready and we add the values to the map
         */
        ResourceUsageCollectorService resourceUsageCollectorService = getInstanceFromNode(ResourceUsageCollectorService.class);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        assertBusy(() -> assertEquals(1, resourceUsageCollectorService.getAllNodeStatistics().size()));

        /**
         * Wait for memory utilization to be reported greater than 0
         */
        assertBusy(
            () -> assertThat(
                resourceUsageCollectorService.getNodeStatistics(clusterService.localNode().getId()).get().getMemoryUtilizationPercent(),
                greaterThan(0.0)
            ),
            5,
            TimeUnit.SECONDS
        );
        assertTrue(resourceUsageCollectorService.getNodeStatistics("Invalid").isEmpty());
    }

    /*
     * Test that concurrently adding values and removing nodes does not cause exceptions
     */
    public void testConcurrentAddingAndRemovingNodes() throws Exception {
        ResourceUsageCollectorService resourceUsageCollectorService = getInstanceFromNode(ResourceUsageCollectorService.class);
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
                    resourceUsageCollectorService.removeNodeResourceUsageStats(randomFrom(nodes));
                }
                resourceUsageCollectorService.collectNodeResourceUsageStats(
                    randomFrom(nodes),
                    System.currentTimeMillis(),
                    randomIntBetween(1, 100),
                    randomIntBetween(1, 100),
                    new IoUsageStats(randomIntBetween(1, 100))
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

        final Map<String, NodeResourceUsageStats> nodeStats = resourceUsageCollectorService.getAllNodeStatistics();
        for (String nodeId : nodes) {
            if (nodeStats.containsKey(nodeId)) {
                assertThat(nodeStats.get(nodeId).memoryUtilizationPercent, greaterThan(0.0));
                assertThat(nodeStats.get(nodeId).cpuUtilizationPercent, greaterThan(0.0));
                assertThat(nodeStats.get(nodeId).getIoUsageStats().getIoUtilisationPercent(), greaterThan(0.0));
            }
        }
    }

    public void testNodeRemoval() {
        ResourceUsageCollectorService resourceUsageCollectorService = getInstanceFromNode(ResourceUsageCollectorService.class);
        resourceUsageCollectorService.collectNodeResourceUsageStats(
            "node1",
            System.currentTimeMillis(),
            randomIntBetween(1, 100),
            randomIntBetween(1, 100),
            new IoUsageStats(randomIntBetween(1, 100))
        );
        resourceUsageCollectorService.collectNodeResourceUsageStats(
            "node2",
            System.currentTimeMillis(),
            randomIntBetween(1, 100),
            randomIntBetween(1, 100),
            new IoUsageStats(randomIntBetween(1, 100))
        );

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

        resourceUsageCollectorService.clusterChanged(event);
        final Map<String, NodeResourceUsageStats> nodeStats = resourceUsageCollectorService.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertFalse(nodeStats.containsKey("node2"));
    }
}

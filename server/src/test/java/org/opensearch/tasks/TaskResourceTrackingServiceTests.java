/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.tasks.TransportTasksActionTests;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.WriteableBase64;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.tasks.resourcetracker.ThreadResourceInfo;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.core.tasks.resourcetracker.ResourceStats.CPU;
import static org.opensearch.core.tasks.resourcetracker.ResourceStats.MEMORY;
import static org.opensearch.tasks.TaskResourceTrackingService.TASK_ID;
import static org.opensearch.tasks.TaskResourceTrackingService.TASK_RESOURCE_USAGE;

public class TaskResourceTrackingServiceTests extends OpenSearchTestCase {

    private static final String LOCAL_NODE_ID = "local_node";
    private static final String OLD_COORDINATOR_ID = "old_coord_node";
    private static final String NEW_COORDINATOR_ID = "new_coord_node";

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TaskResourceTrackingService taskResourceTrackingService;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(TransportTasksActionTests.class.getSimpleName(), new AtomicReference<>());
        DiscoveryNode localNode = new DiscoveryNode(
            LOCAL_NODE_ID,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode);
        taskResourceTrackingService = new TaskResourceTrackingService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            clusterService
        );
    }

    @After
    public void terminateThreadPool() {
        clusterService.close();
        terminate(threadPool);
    }

    private void addCoordinatorNodeWithVersion(String nodeId, Version version) {
        DiscoveryNode coordinator = new DiscoveryNode(
            nodeId,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            version
        );
        ClusterState current = clusterService.state();
        DiscoveryNodes updated = DiscoveryNodes.builder(current.nodes()).add(coordinator).build();
        ClusterState newState = ClusterState.builder(current).nodes(updated).build();
        ClusterServiceUtils.setState(clusterService, newState);
    }

    private SearchShardTask searchShardTaskWithParent(String coordinatorNodeId) {
        TaskId parent = new TaskId(coordinatorNodeId, randomLong());
        SearchShardTask task = new SearchShardTask(randomLong(), "test", "test", "task", parent, new HashMap<>());
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        taskResourceTrackingService.startTracking(task);
        task.startThreadResourceTracking(
            Thread.currentThread().threadId(),
            ResourceStatsType.WORKER_STATS,
            new ResourceUsageMetric(CPU, 100),
            new ResourceUsageMetric(MEMORY, 100)
        );
        return task;
    }

    private static String legacyJsonOf(TaskResourceInfo info) {
        return info.toString();
    }

    public void testThreadContextUpdateOnTrackingStart() {
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);

        Task task = new SearchTask(1, "test", "test", () -> "Test", TaskId.EMPTY_TASK_ID, new HashMap<>());

        String key = "KEY";
        String value = "VALUE";

        // Prepare thread context
        threadPool.getThreadContext().putHeader(key, value);
        threadPool.getThreadContext().putTransient(key, value);
        threadPool.getThreadContext().addResponseHeader(key, value);

        ThreadContext.StoredContext storedContext = taskResourceTrackingService.startTracking(task);

        // All headers should be preserved and Task Id should also be included in thread context
        verifyThreadContextFixedHeaders(key, value);
        assertEquals((long) threadPool.getThreadContext().getTransient(TASK_ID), task.getId());

        storedContext.restore();

        // Post restore only task id should be removed from the thread context
        verifyThreadContextFixedHeaders(key, value);
        assertNull(threadPool.getThreadContext().getTransient(TASK_ID));
    }

    public void testStopTrackingHandlesCurrentActiveThread() {
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        Task task = new SearchTask(1, "test", "test", () -> "Test", TaskId.EMPTY_TASK_ID, new HashMap<>());
        ThreadContext.StoredContext storedContext = taskResourceTrackingService.startTracking(task);
        long threadId = Thread.currentThread().threadId();
        taskResourceTrackingService.taskExecutionStartedOnThread(task.getId(), threadId);

        assertTrue(task.getResourceStats().get(threadId).get(0).isActive());
        assertEquals(0, task.getResourceStats().get(threadId).get(0).getResourceUsageInfo().getStatsInfo().get(MEMORY).getTotalValue());

        taskResourceTrackingService.stopTracking(task);

        // Makes sure stop tracking marks the current active thread inactive and refreshes the resource stats before returning.
        assertFalse(task.getResourceStats().get(threadId).get(0).isActive());
        assertTrue(task.getResourceStats().get(threadId).get(0).getResourceUsageInfo().getStatsInfo().get(MEMORY).getTotalValue() > 0);
    }

    /**
     * Test if taskResourceTrackingService properly tracks resource usage when multiple threads work on the same task
     */
    public void testStartingTrackingHandlesMultipleThreadsPerTask() throws InterruptedException {
        ExecutorService executor = threadPool.executor(ThreadPool.Names.GENERIC);
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        Task task = new SearchTask(1, "test", "test", () -> "Test", TaskId.EMPTY_TASK_ID, new HashMap<>());
        taskResourceTrackingService.startTracking(task);
        int numTasks = randomIntBetween(2, 100);
        for (int i = 0; i < numTasks; i++) {
            executor.execute(() -> {
                long threadId = Thread.currentThread().threadId();
                taskResourceTrackingService.taskExecutionStartedOnThread(task.getId(), threadId);
                // The same thread may pick up multiple runnables for the same task id
                assertEquals(1, task.getResourceStats().get(threadId).stream().filter(ThreadResourceInfo::isActive).count());
                taskResourceTrackingService.taskExecutionFinishedOnThread(task.getId(), threadId);
            });
        }
        executor.shutdown();
        while (true) {
            try {
                if (executor.awaitTermination(1, TimeUnit.MINUTES)) break;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Map<Long, List<ThreadResourceInfo>> stats = task.getResourceStats();
        int numExecutions = 0;
        for (Long threadId : stats.keySet()) {
            for (ThreadResourceInfo info : task.getResourceStats().get(threadId)) {
                assertTrue(info.getResourceUsageInfo().getStatsInfo().get(MEMORY).getTotalValue() > 0);
                assertTrue(info.getResourceUsageInfo().getStatsInfo().get(CPU).getTotalValue() > 0);
                assertFalse(info.isActive());
                numExecutions++;
            }

        }
        assertTrue(task.getTotalResourceStats().getCpuTimeInNanos() > 0);
        assertTrue(task.getTotalResourceStats().getMemoryInBytes() > 0);
        // Basic sanity testing that min < average < max < total
        assertTrue(task.getMinResourceStats().getMemoryInBytes() < task.getAverageResourceStats().getMemoryInBytes());
        assertTrue(task.getAverageResourceStats().getMemoryInBytes() < task.getMaxResourceStats().getMemoryInBytes());
        assertTrue(task.getMaxResourceStats().getMemoryInBytes() < task.getTotalResourceStats().getMemoryInBytes());
        // Each execution of a runnable should record an entry in resourceStats even if it's the same thread
        assertEquals(numTasks, numExecutions);
    }

    public void testWriteTaskResourceUsage() {
        SearchShardTask task = new SearchShardTask(1, "test", "test", "task", TaskId.EMPTY_TASK_ID, new HashMap<>());
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        taskResourceTrackingService.startTracking(task);
        task.startThreadResourceTracking(
            Thread.currentThread().threadId(),
            ResourceStatsType.WORKER_STATS,
            new ResourceUsageMetric(CPU, 100),
            new ResourceUsageMetric(MEMORY, 100)
        );
        taskResourceTrackingService.writeTaskResourceUsage(task, "node_1");
        Map<String, List<String>> headers = threadPool.getThreadContext().getResponseHeaders();
        assertEquals(1, headers.size());
        assertTrue(headers.containsKey(TASK_RESOURCE_USAGE));
    }

    public void testGetTaskResourceUsageFromThreadContext() throws Exception {
        TaskResourceInfo expected = new TaskResourceInfo("testAction", 1L, 2L, "nodeId", new TaskResourceUsage(1000L, 2000L));
        String binary = WriteableBase64.encode(expected);
        threadPool.getThreadContext().addResponseHeader(TASK_RESOURCE_USAGE, binary);

        TaskResourceInfo result = taskResourceTrackingService.getTaskResourceUsageFromThreadContext();
        assertNotNull(result);
        assertEquals(expected, result);
    }

    private void verifyThreadContextFixedHeaders(String key, String value) {
        assertEquals(threadPool.getThreadContext().getHeader(key), value);
        assertEquals(threadPool.getThreadContext().getTransient(key), value);
        assertEquals(threadPool.getThreadContext().getResponseHeaders().get(key).get(0), value);
    }

    public void testBinaryHeaderReadFromThreadContext() throws Exception {
        TaskResourceInfo original = new TaskResourceInfo(
            "indices:data/read/search[phase/query]",
            1L,
            2L,
            "nodeA",
            new TaskResourceUsage(1000L, 2000L)
        );

        String binaryHeader = WriteableBase64.encode(original);
        threadPool.getThreadContext().addResponseHeader(TASK_RESOURCE_USAGE, binaryHeader);

        TaskResourceInfo result = taskResourceTrackingService.getTaskResourceUsageFromThreadContext();
        assertNotNull(result);
        assertEquals(original, result);
    }

    public void testCorruptedHeaderReturnsNullGracefully() {
        threadPool.getThreadContext().addResponseHeader(TASK_RESOURCE_USAGE, "not-valid-base64!!!");

        TaskResourceInfo result = taskResourceTrackingService.getTaskResourceUsageFromThreadContext();
        assertNull("Corrupted header should return null gracefully", result);
    }

    public void testEmptyHeaderReturnsNull() {
        threadPool.getThreadContext().addResponseHeader(TASK_RESOURCE_USAGE, "");

        TaskResourceInfo result = taskResourceTrackingService.getTaskResourceUsageFromThreadContext();
        assertNull(result);
    }

    public void testWriteTaskResourceUsageRoundTrips() {
        // EMPTY_TASK_ID means we can't resolve a coordinator, so the producer falls back to JSON;
        // the consumer's JSON path must still round-trip cleanly.
        SearchShardTask task = new SearchShardTask(1, "test", "test", "task", TaskId.EMPTY_TASK_ID, new HashMap<>());
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        taskResourceTrackingService.startTracking(task);
        task.startThreadResourceTracking(
            Thread.currentThread().threadId(),
            ResourceStatsType.WORKER_STATS,
            new ResourceUsageMetric(CPU, 100),
            new ResourceUsageMetric(MEMORY, 100)
        );
        taskResourceTrackingService.writeTaskResourceUsage(task, "node_1");

        TaskResourceInfo result = taskResourceTrackingService.getTaskResourceUsageFromThreadContext();
        assertNotNull(result);
        assertEquals("node_1", result.getNodeId());
    }

    // Rolling-upgrade BC: one test per (data node, coordinator) quadrant. The TASK_RESOURCE_USAGE
    // response header is one-way (data node -> coordinator), so the matrix is exhaustive.
    // Quadrant 4 (old data -> old coord) is the pre-PR baseline; it's exercised by prior releases'
    // tests and not reachable from this branch, so it has no test here.

    public void testBwc_NewDataNode_NewCoordinator() {
        addCoordinatorNodeWithVersion(NEW_COORDINATOR_ID, Version.CURRENT);

        SearchShardTask task = searchShardTaskWithParent(NEW_COORDINATOR_ID);
        taskResourceTrackingService.writeTaskResourceUsage(task, LOCAL_NODE_ID);

        String headerValue = threadPool.getThreadContext().getResponseHeaders().get(TASK_RESOURCE_USAGE).get(0);
        assertFalse("expected binary header, got: " + headerValue, headerValue.startsWith("{"));

        TaskResourceInfo result = taskResourceTrackingService.getTaskResourceUsageFromThreadContext();
        assertNotNull(result);
        assertEquals(LOCAL_NODE_ID, result.getNodeId());
    }

    public void testBwc_NewDataNode_OldCoordinator() {
        // If the data node emits binary here, the old coordinator's IOException-only catch lets
        // XContentParseException escape, the shard response is silently dropped, and the search hangs.
        addCoordinatorNodeWithVersion(OLD_COORDINATOR_ID, Version.CURRENT.minimumCompatibilityVersion());

        SearchShardTask task = searchShardTaskWithParent(OLD_COORDINATOR_ID);
        taskResourceTrackingService.writeTaskResourceUsage(task, LOCAL_NODE_ID);

        String headerValue = threadPool.getThreadContext().getResponseHeaders().get(TASK_RESOURCE_USAGE).get(0);
        assertTrue("expected JSON header for old coordinator, got: " + headerValue, headerValue.startsWith("{"));
    }

    public void testBwc_OldDataNode_NewCoordinator() {
        TaskResourceInfo fromOldNode = new TaskResourceInfo(
            "indices:data/read/search[phase/query]",
            42L,
            7L,
            "old_data_node",
            new TaskResourceUsage(1500L, 2500L)
        );
        threadPool.getThreadContext().addResponseHeader(TASK_RESOURCE_USAGE, legacyJsonOf(fromOldNode));

        TaskResourceInfo result = taskResourceTrackingService.getTaskResourceUsageFromThreadContext();
        assertNotNull(result);
        assertEquals(fromOldNode, result);
    }

    public void testProducerEmitsJsonHeaderWhenCoordinatorMissingFromClusterState() {
        // Coordinator's version is unknown (left the cluster, or state lag) — emit JSON for safety
        // rather than risk a hang if the coordinator turns out to be old.
        SearchShardTask task = searchShardTaskWithParent("ghost_coordinator");
        taskResourceTrackingService.writeTaskResourceUsage(task, LOCAL_NODE_ID);

        String headerValue = threadPool.getThreadContext().getResponseHeaders().get(TASK_RESOURCE_USAGE).get(0);
        assertTrue("expected JSON when coordinator is unknown, got: " + headerValue, headerValue.startsWith("{"));
    }

    public void testProducerEmitsJsonHeaderWhenNoParentTask() {
        // No parent task means there's no coordinator to verify against — emit JSON for safety.
        SearchShardTask task = new SearchShardTask(randomLong(), "test", "test", "task", TaskId.EMPTY_TASK_ID, new HashMap<>());
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        taskResourceTrackingService.startTracking(task);
        task.startThreadResourceTracking(
            Thread.currentThread().threadId(),
            ResourceStatsType.WORKER_STATS,
            new ResourceUsageMetric(CPU, 100),
            new ResourceUsageMetric(MEMORY, 100)
        );
        taskResourceTrackingService.writeTaskResourceUsage(task, LOCAL_NODE_ID);

        String headerValue = threadPool.getThreadContext().getResponseHeaders().get(TASK_RESOURCE_USAGE).get(0);
        assertTrue("expected JSON when no parent task, got: " + headerValue, headerValue.startsWith("{"));
    }

    public void testProducerEmitsJsonHeaderWhenClusterServiceIsNull() {
        // 3-arg constructor: no ClusterService, so the coordinator's version cannot be verified.
        // Producer must default to JSON to remain rolling-upgrade safe.
        TaskResourceTrackingService noClusterService = new TaskResourceTrackingService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        SearchShardTask task = new SearchShardTask(
            randomLong(),
            "test",
            "test",
            "task",
            new TaskId(NEW_COORDINATOR_ID, randomLong()),
            new HashMap<>()
        );
        noClusterService.setTaskResourceTrackingEnabled(true);
        noClusterService.startTracking(task);
        task.startThreadResourceTracking(
            Thread.currentThread().threadId(),
            ResourceStatsType.WORKER_STATS,
            new ResourceUsageMetric(CPU, 100),
            new ResourceUsageMetric(MEMORY, 100)
        );
        noClusterService.writeTaskResourceUsage(task, LOCAL_NODE_ID);

        String headerValue = threadPool.getThreadContext().getResponseHeaders().get(TASK_RESOURCE_USAGE).get(0);
        assertTrue("expected JSON when ClusterService is null, got: " + headerValue, headerValue.startsWith("{"));
    }

    public void testProducerEmitsJsonHeaderWhenBinaryHeaderKillSwitchIsOff() {
        // Operator-controlled kill switch: when disabled, the producer must emit JSON even
        // when the coordinator is on the current version and would otherwise accept binary.
        addCoordinatorNodeWithVersion(NEW_COORDINATOR_ID, Version.CURRENT);
        taskResourceTrackingService.setBinaryResourceUsageHeaderEnabled(false);

        SearchShardTask task = searchShardTaskWithParent(NEW_COORDINATOR_ID);
        taskResourceTrackingService.writeTaskResourceUsage(task, LOCAL_NODE_ID);

        String headerValue = threadPool.getThreadContext().getResponseHeaders().get(TASK_RESOURCE_USAGE).get(0);
        assertTrue("expected JSON when kill switch is off, got: " + headerValue, headerValue.startsWith("{"));
    }

    public void testConsumerStillReadsBinaryWhenKillSwitchIsOff() throws Exception {
        // The kill switch only gates the producer. The consumer must still decode binary so that
        // a coordinator can read headers from data nodes that haven't yet picked up the flip.
        taskResourceTrackingService.setBinaryResourceUsageHeaderEnabled(false);
        TaskResourceInfo expected = new TaskResourceInfo(
            "indices:data/read/search[phase/query]",
            5L,
            6L,
            "data_node",
            new TaskResourceUsage(11L, 22L)
        );
        String binary = WriteableBase64.encode(expected);
        threadPool.getThreadContext().addResponseHeader(TASK_RESOURCE_USAGE, binary);

        TaskResourceInfo result = taskResourceTrackingService.getTaskResourceUsageFromThreadContext();
        assertNotNull(result);
        assertEquals(expected, result);
    }

    public void testConsumerPrefersBinaryOverJsonFallback() throws Exception {
        TaskResourceInfo expected = new TaskResourceInfo(
            "indices:data/read/search[phase/query]",
            99L,
            33L,
            "node_x",
            new TaskResourceUsage(7777L, 8888L)
        );
        String binary = WriteableBase64.encode(expected);
        threadPool.getThreadContext().addResponseHeader(TASK_RESOURCE_USAGE, binary);

        TaskResourceInfo result = taskResourceTrackingService.getTaskResourceUsageFromThreadContext();
        assertNotNull(result);
        assertEquals(expected, result);
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.Version;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.opensearch.test.ClusterServiceUtils.setState;

/**
 * Contains tests for {@link MasterTaskThrottler}
 */
public class MasterTaskThrottlerTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private ClusterService clusterService;
    private DiscoveryNode localNode;
    private DiscoveryNode[] allNodes;
    private MasterThrottlingStats throttlingStats;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportMasterNodeActionTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        localNode = new DiscoveryNode(
            "local_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.V_3_0_0
        );
        allNodes = new DiscoveryNode[] { localNode };
        throttlingStats = new MasterThrottlingStats();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testDefaults() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(
            clusterSettings,
            () -> { return clusterService.getMasterService().getMinNodeVersion(); },
            throttlingStats
        );
        for (String key : MasterTaskThrottler.CONFIGURED_TASK_FOR_THROTTLING) {
            assertNull(throttler.getThrottlingLimit(key));
        }
    }

    public void testValidateSettingsForDifferentVersion() {
        DiscoveryNode masterNode = new DiscoveryNode(
            "local_master_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.V_3_0_0
        );
        DiscoveryNode dataNode = new DiscoveryNode(
            "local_data_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.V_1_0_0
        );
        setState(clusterService, ClusterStateCreationUtils.state(masterNode, masterNode, new DiscoveryNode[] { masterNode, dataNode }));

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(
            clusterSettings,
            () -> { return clusterService.getMasterService().getMinNodeVersion(); },
            throttlingStats
        );

        // set some limit for update snapshot tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder().put("master.throttling.thresholds.put-mapping.value", newLimit).build();

        AtomicBoolean exceptionThrown = new AtomicBoolean();
        try {
            throttler.validateSetting(newSettings);
        } catch (IllegalArgumentException e) {
            exceptionThrown.set(true);
        }
        assertTrue(exceptionThrown.get());
    }

    public void testValidateSettingsForUnknownTask() {
        DiscoveryNode masterNode = new DiscoveryNode(
            "local_master_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.V_3_0_0
        );
        DiscoveryNode dataNode = new DiscoveryNode(
            "local_data_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.V_3_0_0
        );
        setState(clusterService, ClusterStateCreationUtils.state(masterNode, masterNode, new DiscoveryNode[] { masterNode, dataNode }));

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(
            clusterSettings,
            () -> { return clusterService.getMasterService().getMinNodeVersion(); },
            throttlingStats
        );

        // set some limit for update snapshot tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder().put("master.throttling.thresholds.random-task.value", newLimit).build();

        AtomicBoolean exceptionThrown = new AtomicBoolean();
        try {
            throttler.validateSetting(newSettings);
        } catch (IllegalArgumentException e) {
            exceptionThrown.set(true);
        }
        assertTrue(exceptionThrown.get());
    }

    public void testUpdateThrottlingLimitForBasicSanity() {
        DiscoveryNode masterNode = new DiscoveryNode(
            "local_master_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.V_3_0_0
        );
        DiscoveryNode dataNode = new DiscoveryNode(
            "local_data_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.V_3_0_0
        );
        setState(clusterService, ClusterStateCreationUtils.state(masterNode, masterNode, new DiscoveryNode[] { masterNode, dataNode }));

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(
            clusterSettings,
            () -> { return clusterService.getMasterService().getMinNodeVersion(); },
            throttlingStats
        );

        // set some limit for update snapshot tasks
        long newLimit = randomLongBetween(1, 10);

        Settings newSettings = Settings.builder().put("master.throttling.thresholds.put-mapping.value", newLimit).build();
        clusterSettings.applySettings(newSettings);
        assertEquals(newLimit, throttler.getThrottlingLimit("put-mapping").intValue());

        // set update snapshot task limit to default
        newSettings = Settings.builder().put("master.throttling.thresholds.put-mapping.value", -1).build();
        clusterSettings.applySettings(newSettings);
        assertNull(throttler.getThrottlingLimit("put-mapping"));
    }

    public void testValidateSettingForLimit() {
        DiscoveryNode masterNode = new DiscoveryNode(
            "local_master_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.V_3_0_0
        );
        DiscoveryNode dataNode = new DiscoveryNode(
            "local_data_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.V_3_0_0
        );
        setState(clusterService, ClusterStateCreationUtils.state(masterNode, masterNode, new DiscoveryNode[] { masterNode, dataNode }));

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(
            clusterSettings,
            () -> { return clusterService.getMasterService().getMinNodeVersion(); },
            throttlingStats
        );

        Settings newSettings = Settings.builder().put("master.throttling.thresholds.put-mapping.values", -5).build();
        AtomicBoolean exceptionThrown = new AtomicBoolean();
        try {
            throttler.validateSetting(newSettings);
        } catch (IllegalArgumentException e) {
            exceptionThrown.set(true);
        }
        assertTrue(exceptionThrown.getAndSet(false));
    }

    public void testUpdateLimit() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(
            clusterSettings,
            () -> { return clusterService.getMasterService().getMinNodeVersion(); },
            throttlingStats
        );

        throttler.updateLimit("test", 5);
        assertEquals(5L, throttler.getThrottlingLimit("test").intValue());
        throttler.updateLimit("test", -1);
        assertNull(throttler.getThrottlingLimit("test"));
    }

    public void testThrottling() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(
            clusterSettings,
            () -> { return clusterService.getMasterService().getMinNodeVersion(); },
            throttlingStats
        );

        String taskKey = "test";
        throttler.updateLimit(taskKey, 5);

        // adding 3 tasks
        throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, 3));

        // adding 3 more tasks, these tasks should be throttled
        // taskCount in Queue: 3 Threshold: 5
        try {
            throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, 3));
        } catch (Exception e) {
            assertTrue(e instanceof MasterTaskThrottlingException);
        }
        assertEquals(3L, throttlingStats.getThrottlingCount(taskKey));

        // remove one task
        throttler.onProcessed(getMockUpdateTaskList(taskKey, 1));

        // add 3 tasks should pass now.
        // taskCount in Queue: 2 Threshold: 5
        throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, 3));

        // adding one task will throttle
        // taskCount in Queue: 5 Threshold: 5
        try {
            throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, 1));
        } catch (Exception e) {
            assertTrue(e instanceof MasterTaskThrottlingException);
        }
        assertEquals(4L, throttlingStats.getThrottlingCount(taskKey));

        // update limit of threshold 6
        throttler.updateLimit(taskKey, 6);

        // adding one task should pass now
        throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, 1));
    }

    private List<TaskBatcherTests.TestTaskBatcher.UpdateTask> getMockUpdateTaskList(String taskKey, int size) {
        TaskBatcherTests.TestTaskBatcher testTaskBatcher = new TaskBatcherTests.TestTaskBatcher(logger, null);
        List<TaskBatcherTests.TestTaskBatcher.UpdateTask> taskList = new ArrayList<>();

        class MockExecutor
            implements
                TaskExecutorTests.TestExecutor,
                ClusterStateTaskExecutor<TaskBatcherTests.TestTaskBatcher.UpdateTask> {

            @Override
            public ClusterTasksResult<TaskBatcherTests.TestTaskBatcher.UpdateTask> execute(
                ClusterState currentState,
                List<TaskBatcherTests.TestTaskBatcher.UpdateTask> tasks
            ) throws Exception {
                // No Op
                return null;
            }

            @Override
            public boolean runOnlyOnMaster() {
                return true;
            }

            @Override
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {}

            @Override
            public void execute(List tasks) {}

            @Override
            public String getMasterThrottlingKey() {
                return taskKey;
            }

            @Override
            public String describeTasks(List tasks) {
                return taskKey;
            }
        }

        for (int i = 0; i < size; i++) {
            taskList.add(testTaskBatcher.new UpdateTask(Priority.HIGH, taskKey, taskKey, (source, e) -> {
                if (!(e instanceof MasterTaskThrottlingException)) {
                    throw new AssertionError(e);
                }
            }, new MockExecutor()));
        }

        return taskList;
    }
}

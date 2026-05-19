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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.service.ClusterManagerTask.CREATE_INDEX;
import static org.opensearch.cluster.service.ClusterManagerTask.PUT_MAPPING;
import static org.opensearch.cluster.service.ClusterManagerTaskThrottler.THRESHOLD_SETTINGS;
import static org.opensearch.test.ClusterServiceUtils.setState;

/**
 * Contains tests for {@link ClusterManagerTaskThrottler}
 */
public class ClusterManagerTaskThrottlerTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private ClusterService clusterService;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportMasterNodeActionTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
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
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask(PUT_MAPPING, true);
        throttler.registerClusterManagerTask(CREATE_INDEX, true);

        for (String key : throttler.THROTTLING_TASK_KEYS.keySet()) {
            assertEquals(ClusterManagerTask.fromKey(key).getThreshold(), throttler.getThrottlingLimit(key).intValue());
        }
    }

    public void testThrottlingThresholdNotConfigured() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> throttler.registerClusterManagerTask(ClusterManagerTask.fromKey("random-task"), true)
        );
        assertEquals("No cluster manager task found for key: random-task", exception.getMessage());
    }

    public void testValidateSettingsForDifferentVersion() {
        DiscoveryNode clusterManagerNode = getClusterManagerNode(Version.V_2_5_0);
        DiscoveryNode dataNode = getDataNode(Version.V_2_0_0);
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode, dataNode })
        );

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask(PUT_MAPPING, true);

        // set some limit for put-mapping tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", newLimit).build();
        assertThrows(IllegalArgumentException.class, () -> throttler.validateSetting(THRESHOLD_SETTINGS.get(newSettings)));

        // validate for empty setting, it shouldn't throw exception
        Settings emptySettings = Settings.builder().build();
        try {
            throttler.validateSetting(emptySettings);
        } catch (Exception e) {
            // it shouldn't throw exception
            throw new AssertionError(e);
        }
    }

    public void testValidateSettingsForTaskWithoutRetryOnDataNode() {
        DiscoveryNode clusterManagerNode = getClusterManagerNode(Version.V_2_5_0);
        DiscoveryNode dataNode = getDataNode(Version.V_2_5_0);
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode, dataNode })
        );

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask(PUT_MAPPING, false);

        // set some limit for put-mapping tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", newLimit).build();
        assertThrows(IllegalArgumentException.class, () -> throttler.validateSetting(THRESHOLD_SETTINGS.get(newSettings)));
    }

    public void testUpdateSettingsForNullValue() {
        DiscoveryNode clusterManagerNode = getClusterManagerNode(Version.V_2_5_0);
        DiscoveryNode dataNode = getDataNode(Version.V_2_5_0);
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode, dataNode })
        );

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask(PUT_MAPPING, true);
        // check default value
        assertEquals(ClusterManagerTask.PUT_MAPPING.getThreshold(), throttler.getThrottlingLimit(PUT_MAPPING.getKey()).intValue());

        // set some limit for put-mapping tasks
        int newLimit = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", newLimit).build();
        clusterSettings.applySettings(newSettings);
        assertEquals(newLimit, throttler.getThrottlingLimit(PUT_MAPPING.getKey()).intValue());

        // set limit to null
        Settings nullSettings = Settings.builder().build();
        clusterSettings.applySettings(nullSettings);
        assertEquals(ClusterManagerTask.PUT_MAPPING.getThreshold(), throttler.getThrottlingLimit(PUT_MAPPING.getKey()).intValue());
    }

    public void testSettingsOnBootstrap() {
        DiscoveryNode clusterManagerNode = getClusterManagerNode(Version.V_2_5_0);
        DiscoveryNode dataNode = getDataNode(Version.V_2_5_0);
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode, dataNode })
        );

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        int put_mapping_threshold_value = randomIntBetween(1, 10);
        int baseDelay = randomIntBetween(1, 10);
        int maxDelay = randomIntBetween(1, 10);
        Settings initialSettings = Settings.builder()
            .put("cluster_manager.throttling.thresholds.put-mapping.value", put_mapping_threshold_value)
            .put("cluster_manager.throttling.retry.base.delay", baseDelay + "s")
            .put("cluster_manager.throttling.retry.max.delay", maxDelay + "s")
            .build();
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(initialSettings, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask(PUT_MAPPING, true);

        // assert that limit is applied on throttler
        assertEquals(put_mapping_threshold_value, throttler.getThrottlingLimit(PUT_MAPPING.getKey()).intValue());
        // assert that delay setting is applied on throttler
        assertEquals(baseDelay, ClusterManagerTaskThrottler.getBaseDelayForRetry().seconds());
        assertEquals(maxDelay, ClusterManagerTaskThrottler.getMaxDelayForRetry().seconds());
    }

    public void testUpdateRetryDelaySetting() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());

        // verify defaults
        assertEquals(ClusterManagerTaskThrottler.baseDelay, ClusterManagerTaskThrottler.getBaseDelayForRetry());
        assertEquals(ClusterManagerTaskThrottler.maxDelay, ClusterManagerTaskThrottler.getMaxDelayForRetry());

        // verify update base delay
        int baseDelay = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder().put("cluster_manager.throttling.retry.base.delay", baseDelay + "s").build();
        clusterSettings.applySettings(newSettings);
        assertEquals(baseDelay, ClusterManagerTaskThrottler.getBaseDelayForRetry().seconds());

        // verify update max delay
        int maxDelay = randomIntBetween(1, 10);
        newSettings = Settings.builder().put("cluster_manager.throttling.retry.max.delay", maxDelay + "s").build();
        clusterSettings.applySettings(newSettings);
        assertEquals(maxDelay, ClusterManagerTaskThrottler.getMaxDelayForRetry().seconds());
    }

    public void testValidateSettingsForUnknownTask() {
        DiscoveryNode clusterManagerNode = getClusterManagerNode(Version.V_2_5_0);
        DiscoveryNode dataNode = getDataNode(Version.V_2_5_0);
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode, dataNode })
        );

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());

        // set some limit for random tasks
        int newLimit = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.random-task.value", newLimit).build();
        assertThrows(IllegalArgumentException.class, () -> throttler.validateSetting(THRESHOLD_SETTINGS.get(newSettings)));
    }

    public void testUpdateThrottlingLimitForBasicSanity() {
        DiscoveryNode clusterManagerNode = getClusterManagerNode(Version.V_2_5_0);
        DiscoveryNode dataNode = getDataNode(Version.V_2_5_0);
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode, dataNode })
        );

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask(PUT_MAPPING, true);

        // set some limit for put-mapping tasks
        long newLimit = randomLongBetween(1, 10);

        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", newLimit).build();
        clusterSettings.applySettings(newSettings);
        assertEquals(newLimit, throttler.getThrottlingLimit(PUT_MAPPING.getKey()).intValue());

        // set put-mapping task limit to 20
        newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", 20).build();
        clusterSettings.applySettings(newSettings);
        assertEquals(20, throttler.getThrottlingLimit(PUT_MAPPING.getKey()).intValue());
    }

    public void testValidateSettingForLimit() {
        DiscoveryNode clusterManagerNode = getClusterManagerNode(Version.V_2_5_0);
        DiscoveryNode dataNode = getDataNode(Version.V_2_5_0);
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode, dataNode })
        );

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask(PUT_MAPPING, true);

        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", -5).build();
        assertThrows(IllegalArgumentException.class, () -> throttler.validateSetting(THRESHOLD_SETTINGS.get(newSettings)));
    }

    public void testUpdateLimit() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask(PUT_MAPPING, true);

        throttler.updateLimit(PUT_MAPPING.getKey(), 5);
        assertEquals(5L, throttler.getThrottlingLimit(PUT_MAPPING.getKey()).intValue());
        throttler.updateLimit(PUT_MAPPING.getKey(), -1);
        assertNull(throttler.getThrottlingLimit(PUT_MAPPING.getKey()));
    }

    private DiscoveryNode getDataNode(Version version) {
        return new DiscoveryNode(
            "local_data_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            version
        );
    }

    private DiscoveryNode getClusterManagerNode(Version version) {
        return new DiscoveryNode(
            "local_master_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            version
        );
    }

    public void testThrottlingForDisabledThrottlingTask() {
        ClusterManagerThrottlingStats throttlingStats = new ClusterManagerThrottlingStats();
        ClusterManagerTask task = CREATE_INDEX;
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, throttlingStats);
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey = throttler.registerClusterManagerTask(task, false);

        // adding limit directly in thresholds
        throttler.updateLimit(task.getKey(), 5);

        // adding 10 tasks, should pass as throttling is disabled for task
        throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 10));

        // Asserting that there was not any throttling for it
        assertEquals(0L, throttlingStats.getThrottlingCount(task.getKey()));

        // Asserting value in tasksCount map to make sure it gets updated even when throttling is disabled
        assertEquals(Optional.of(10L).get(), throttler.tasksCount.get(task.getKey()));
    }

    public void testThrottlingForInitialStaticSettingAndVersionCheck() {
        ClusterManagerThrottlingStats throttlingStats = new ClusterManagerThrottlingStats();
        DiscoveryNode clusterManagerNode = getClusterManagerNode(Version.V_2_5_0);
        DiscoveryNode dataNode = getDataNode(Version.V_2_4_0);
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode, dataNode })
        );

        // setting threshold in initial settings
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        int put_mapping_threshold_value = randomIntBetween(1, 10);
        Settings initialSettings = Settings.builder()
            .put("cluster_manager.throttling.thresholds.put-mapping.value", put_mapping_threshold_value)
            .build();
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(initialSettings, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, throttlingStats);
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey = throttler.registerClusterManagerTask(PUT_MAPPING, true);

        // verifying adding more tasks than threshold passes
        throttler.onBeginSubmit(getMockUpdateTaskList(PUT_MAPPING.getKey(), throttlingKey, put_mapping_threshold_value + 5));
        assertEquals(0L, throttlingStats.getThrottlingCount(PUT_MAPPING.getKey()));

        // Removing older version node from cluster
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode })
        );

        // adding more tasks, these tasks should be throttled
        // As queue already have more tasks than threshold from previous call.
        assertThrows(
            ClusterManagerThrottlingException.class,
            () -> throttler.onBeginSubmit(getMockUpdateTaskList(PUT_MAPPING.getKey(), throttlingKey, 3))
        );
        assertEquals(3L, throttlingStats.getThrottlingCount(PUT_MAPPING.getKey()));
    }

    public void testThrottling() {
        ClusterManagerThrottlingStats throttlingStats = new ClusterManagerThrottlingStats();
        ClusterManagerTask task = CREATE_INDEX;
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, throttlingStats);
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey = throttler.registerClusterManagerTask(task, true);

        throttler.updateLimit(task.getKey(), 5);

        // adding 3 tasks
        throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 3));

        // adding 3 more tasks, these tasks should be throttled
        // taskCount in Queue: 3 Threshold: 5
        assertThrows(
            ClusterManagerThrottlingException.class,
            () -> throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 3))
        );
        assertEquals(3L, throttlingStats.getThrottlingCount(task.getKey()));

        // remove one task
        throttler.onBeginProcessing(getMockUpdateTaskList(task.getKey(), throttlingKey, 1));

        // add 3 tasks should pass now.
        // taskCount in Queue: 2 Threshold: 5
        throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 3));

        // adding one task will throttle
        // taskCount in Queue: 5 Threshold: 5
        assertThrows(
            ClusterManagerThrottlingException.class,
            () -> throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 1))
        );
        assertEquals(4L, throttlingStats.getThrottlingCount(task.getKey()));

        // update limit of threshold 6
        throttler.updateLimit(task.getKey(), 6);

        // adding one task should pass now
        throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 1));
    }

    public void testThrottlingWithLock() {
        ClusterManagerThrottlingStats throttlingStats = new ClusterManagerThrottlingStats();
        ClusterManagerTask task = CREATE_INDEX;
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, throttlingStats);
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey = throttler.registerClusterManagerTask(task, true);

        throttler.updateLimit(task.getKey(), 5);

        // adding 3 tasks
        throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 3));

        // adding 3 more tasks, these tasks should be throttled
        // taskCount in Queue: 3 Threshold: 5
        assertThrows(
            ClusterManagerThrottlingException.class,
            () -> throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 3))
        );
        assertEquals(3L, throttlingStats.getThrottlingCount(task.getKey()));

        // remove one task
        throttler.onBeginProcessing(getMockUpdateTaskList(task.getKey(), throttlingKey, 1));

        // add 3 tasks should pass now.
        // taskCount in Queue: 2 Threshold: 5
        throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 3));

        final CountDownLatch latch = new CountDownLatch(1);
        Thread threadToLock = null;
        try {
            // Taking lock on tasksCount will not impact throttling behaviour now.
            threadToLock = new Thread(() -> {
                throttler.tasksCount.computeIfPresent(task.getKey(), (key, count) -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return 10L;
                });
            });
            threadToLock.start();

            // adding one task will throttle
            // taskCount in Queue: 5 Threshold: 5
            final ClusterManagerThrottlingException exception = assertThrows(
                ClusterManagerThrottlingException.class,
                () -> throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 1))
            );
            assertEquals("Throttling Exception : Limit exceeded for create-index", exception.getMessage());
            assertEquals(Optional.of(5L).get(), throttler.tasksCount.get(task.getKey()));
            assertEquals(4L, throttlingStats.getThrottlingCount(task.getKey()));
        } finally {
            if (threadToLock != null) {
                latch.countDown();
                // Wait to complete and then assert on new tasksCount that got modified by threadToLock Thread
                try {
                    threadToLock.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        assertEquals(Optional.of(10L).get(), throttler.tasksCount.get(task.getKey()));
    }

    public void testThrottlingWithMultipleOnBeginSubmitsThreadsWithLock() {
        ClusterManagerThrottlingStats throttlingStats = new ClusterManagerThrottlingStats();
        ClusterManagerTask task = CREATE_INDEX;
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getClusterManagerService().getMinNodeVersion();
        }, throttlingStats);
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey = throttler.registerClusterManagerTask(task, true);

        throttler.updateLimit(task.getKey(), 5);

        // adding 3 tasks
        throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 3));

        // adding 3 more tasks, these tasks should be throttled
        // taskCount in Queue: 3 Threshold: 5
        assertThrows(
            ClusterManagerThrottlingException.class,
            () -> throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 3))
        );
        assertEquals(3L, throttlingStats.getThrottlingCount(task.getKey()));

        // remove one task
        throttler.onBeginProcessing(getMockUpdateTaskList(task.getKey(), throttlingKey, 1));

        // add 3 tasks should pass now.
        // taskCount in Queue: 2 Threshold: 5
        throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 3));

        final CountDownLatch latch = new CountDownLatch(1);
        Thread threadToLock = null;
        List<Thread> submittingThreads = new ArrayList<>();

        try {
            // Taking lock on tasksCount will not impact throttling behaviour now.
            threadToLock = new Thread(() -> {
                throttler.tasksCount.computeIfPresent(task.getKey(), (key, count) -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return 10L;
                });
            });
            threadToLock.start();

            final CountDownLatch latch2 = new CountDownLatch(10);
            for (int i = 0; i < 10; i++) {
                Thread submittingThread = new Thread(() -> {
                    // adding one task will throttle
                    // taskCount in Queue: 5 Threshold: 5
                    final ClusterManagerThrottlingException exception = assertThrows(
                        ClusterManagerThrottlingException.class,
                        () -> throttler.onBeginSubmit(getMockUpdateTaskList(task.getKey(), throttlingKey, 1))
                    );
                    assertEquals("Throttling Exception : Limit exceeded for create-index", exception.getMessage());
                    assertEquals(Optional.of(5L).get(), throttler.tasksCount.get(task.getKey()));
                    latch2.countDown();
                });
                submittingThread.start();
                submittingThreads.add(submittingThread);
            }
            try {
                latch2.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            assertEquals(13L, throttlingStats.getThrottlingCount(task.getKey()));
        } finally {
            if (threadToLock != null) {
                latch.countDown();
                try {
                    // Wait to complete and then assert on new tasksCount that got modified by threadToLock Thread
                    threadToLock.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            for (Thread submittingThread : submittingThreads) {
                try {
                    submittingThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        assertEquals(Optional.of(10L).get(), throttler.tasksCount.get(task.getKey()));
    }

    private List<TaskBatcherTests.TestTaskBatcher.UpdateTask> getMockUpdateTaskList(
        String taskKey,
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey,
        int size
    ) {
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
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {}

            @Override
            public void execute(List tasks) {}

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return throttlingKey;
            }

            @Override
            public String describeTasks(List tasks) {
                return taskKey;
            }
        }

        for (int i = 0; i < size; i++) {
            taskList.add(testTaskBatcher.new UpdateTask(Priority.HIGH, taskKey, taskKey, (source, e) -> {
                if (!(e instanceof ClusterManagerThrottlingException)) {
                    throw new AssertionError(e);
                }
            }, new MockExecutor()));
        }
        return taskList;
    }
}

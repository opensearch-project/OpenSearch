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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

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
            return clusterService.getMasterService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask("put-mapping", true);
        throttler.registerClusterManagerTask("create-index", true);
        for (String key : throttler.THROTTLING_TASK_KEYS.keySet()) {
            assertNull(throttler.getThrottlingLimit(key));
        }
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
            return clusterService.getMasterService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask("put-mapping", true);

        // set some limit for update snapshot tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", newLimit).build();
        assertThrows(IllegalArgumentException.class, () -> throttler.validateSetting(newSettings));

        // validate for empty setting, it shouldn't throw exception
        Settings emptySettings = Settings.builder().build();
        try {
            throttler.validateSetting(emptySettings);
        } catch (Exception e) {
            // it shouldn't throw exception
            throw new AssertionError(e);
        }
    }

    public void testValidateSettingsForTaskWihtoutRetryOnDataNode() {
        DiscoveryNode clusterManagerNode = getClusterManagerNode(Version.V_2_5_0);
        DiscoveryNode dataNode = getDataNode(Version.V_2_5_0);
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode, dataNode })
        );

        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getMasterService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask("put-mapping", false);

        // set some limit for update snapshot tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", newLimit).build();
        assertThrows(IllegalArgumentException.class, () -> throttler.validateSetting(newSettings));
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
            return clusterService.getMasterService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask("put-mapping", true);

        // set some limit for put-mapping tasks
        int newLimit = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", newLimit).build();
        clusterSettings.applySettings(newSettings);
        assertEquals(newLimit, throttler.getThrottlingLimit("put-mapping").intValue());

        // set limit to null
        Settings nullSettings = Settings.builder().build();
        clusterSettings.applySettings(nullSettings);
        assertNull(throttler.getThrottlingLimit("put-mapping"));
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
            return clusterService.getMasterService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask("put-mapping", true);

        // assert that limit is applied on throttler
        assertEquals(put_mapping_threshold_value, throttler.getThrottlingLimit("put-mapping").intValue());
        // assert that delay setting is applied on throttler
        assertEquals(baseDelay, ClusterManagerTaskThrottler.getBaseDelayForRetry().seconds());
        assertEquals(maxDelay, ClusterManagerTaskThrottler.getMaxDelayForRetry().seconds());
    }

    public void testUpdateRetryDelaySetting() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getMasterService().getMinNodeVersion();
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
            return clusterService.getMasterService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());

        // set some limit for update snapshot tasks
        int newLimit = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.random-task.value", newLimit).build();
        assertThrows(IllegalArgumentException.class, () -> throttler.validateSetting(newSettings));
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
            return clusterService.getMasterService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask("put-mapping", true);

        // set some limit for update snapshot tasks
        long newLimit = randomLongBetween(1, 10);

        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", newLimit).build();
        clusterSettings.applySettings(newSettings);
        assertEquals(newLimit, throttler.getThrottlingLimit("put-mapping").intValue());

        // set update snapshot task limit to default
        newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", -1).build();
        clusterSettings.applySettings(newSettings);
        assertNull(throttler.getThrottlingLimit("put-mapping"));
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
            return clusterService.getMasterService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask("put-mapping", true);

        Settings newSettings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.values", -5).build();
        assertThrows(IllegalArgumentException.class, () -> throttler.validateSetting(newSettings));
    }

    public void testUpdateLimit() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getMasterService().getMinNodeVersion();
        }, new ClusterManagerThrottlingStats());
        throttler.registerClusterManagerTask("put-mapping", true);

        throttler.updateLimit("test", 5);
        assertEquals(5L, throttler.getThrottlingLimit("test").intValue());
        throttler.updateLimit("test", -1);
        assertNull(throttler.getThrottlingLimit("test"));
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
        String taskKey = "test";
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getMasterService().getMinNodeVersion();
        }, throttlingStats);
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey = throttler.registerClusterManagerTask(taskKey, false);

        // adding limit directly in thresholds
        throttler.updateLimit(taskKey, 5);

        // adding 10 tasks, should pass as throttling is disabled for task
        throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, throttlingKey, 10));

        // Asserting that there was not any throttling for it
        assertEquals(0L, throttlingStats.getThrottlingCount(taskKey));
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
            return clusterService.getMasterService().getMinNodeVersion();
        }, throttlingStats);
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey = throttler.registerClusterManagerTask("put-mapping", true);

        // verifying adding more tasks then threshold passes
        throttler.onBeginSubmit(getMockUpdateTaskList("put-mapping", throttlingKey, put_mapping_threshold_value + 5));
        assertEquals(0L, throttlingStats.getThrottlingCount("put-mapping"));

        // Removing older version node from cluster
        setState(
            clusterService,
            ClusterStateCreationUtils.state(clusterManagerNode, clusterManagerNode, new DiscoveryNode[] { clusterManagerNode })
        );

        // adding more tasks, these tasks should be throttled
        // As queue already have more tasks than threshold from previous call.
        assertThrows(
            ClusterManagerThrottlingException.class,
            () -> throttler.onBeginSubmit(getMockUpdateTaskList("put-mapping", throttlingKey, 3))
        );
        assertEquals(3L, throttlingStats.getThrottlingCount("put-mapping"));
    }

    public void testThrottling() {
        ClusterManagerThrottlingStats throttlingStats = new ClusterManagerThrottlingStats();
        String taskKey = "test";
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterManagerTaskThrottler throttler = new ClusterManagerTaskThrottler(Settings.EMPTY, clusterSettings, () -> {
            return clusterService.getMasterService().getMinNodeVersion();
        }, throttlingStats);
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey = throttler.registerClusterManagerTask(taskKey, true);

        throttler.updateLimit(taskKey, 5);

        // adding 3 tasks
        throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, throttlingKey, 3));

        // adding 3 more tasks, these tasks should be throttled
        // taskCount in Queue: 3 Threshold: 5
        assertThrows(
            ClusterManagerThrottlingException.class,
            () -> throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, throttlingKey, 3))
        );
        assertEquals(3L, throttlingStats.getThrottlingCount(taskKey));

        // remove one task
        throttler.onBeginProcessing(getMockUpdateTaskList(taskKey, throttlingKey, 1));

        // add 3 tasks should pass now.
        // taskCount in Queue: 2 Threshold: 5
        throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, throttlingKey, 3));

        // adding one task will throttle
        // taskCount in Queue: 5 Threshold: 5
        assertThrows(
            ClusterManagerThrottlingException.class,
            () -> throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, throttlingKey, 1))
        );
        assertEquals(4L, throttlingStats.getThrottlingCount(taskKey));

        // update limit of threshold 6
        throttler.updateLimit(taskKey, 6);

        // adding one task should pass now
        throttler.onBeginSubmit(getMockUpdateTaskList(taskKey, throttlingKey, 1));
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
            public boolean runOnlyOnMaster() {
                return true;
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

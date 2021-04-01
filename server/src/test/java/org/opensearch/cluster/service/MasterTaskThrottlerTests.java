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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.snapshots.UpdateIndexShardSnapshotStatusRequest;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
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

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportMasterNodeActionTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        localNode = new DiscoveryNode("local_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), Version.V_7_10_3);
        allNodes = new DiscoveryNode[]{localNode};
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
        ClusterSettings clusterSettings =
                new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());
        assertFalse(throttler.isThrottlingEnabled());
        for(String key : throttler.taskNameToClassMapping.keySet()) {
            assertNull(throttler.getThrottlingLimit(throttler.taskNameToClassMapping.get(key)));
        }
    }

    public void testValidateEnableThrottlingForDifferentVersion() {
        DiscoveryNode masterNode = new DiscoveryNode("local_master_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
                Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), Version.V_7_10_3);
        DiscoveryNode dataNode = new DiscoveryNode("local_data_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE), Version.V_7_1_0);
        setState(clusterService, ClusterStateCreationUtils.state(masterNode, masterNode, new DiscoveryNode[]{masterNode, dataNode}));

        ClusterSettings clusterSettings =
            new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());

        AtomicBoolean exceptionThrown = new AtomicBoolean();
        try {
            throttler.validateEnableSetting(true);
        } catch (IllegalArgumentException e){
            exceptionThrown.set(true);
        }
        assertTrue(exceptionThrown.get());
    }

    public void testValidateEnableThrottlingForHappyCase() {
        DiscoveryNode masterNode = new DiscoveryNode("local_master_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), Version.V_7_10_3);
        DiscoveryNode dataNode = new DiscoveryNode("local_data_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE), Version.V_7_10_3);
        setState(clusterService, ClusterStateCreationUtils.state(masterNode, masterNode, new DiscoveryNode[]{masterNode, dataNode}));

        ClusterSettings clusterSettings =
            new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());

        AtomicBoolean exceptionThrown = new AtomicBoolean();
        try {
            throttler.validateEnableSetting(true);
        } catch (IllegalArgumentException e){
            exceptionThrown.set(true);
        }
        assertFalse(exceptionThrown.get());
    }

    public void testUpdateEnableThrottling() {
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, new DiscoveryNode[]{localNode}));
        ClusterSettings clusterSettings =
                new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());

        Settings newSettings = Settings.builder()
                .put(MasterTaskThrottler.ENABLE_MASTER_THROTTLING.getKey(), true)
                .build();
        clusterSettings.applySettings(newSettings);
        assertTrue(throttler.isThrottlingEnabled());
    }

    public void testFlippingEnableThrottling() {
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, new DiscoveryNode[]{localNode}));

        ClusterSettings clusterSettings =
                new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());

        // set some limit for update snapshot tasks
        int limit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder()
                .put(MasterTaskThrottler.ENABLE_MASTER_THROTTLING.getKey(), true)
                .put("master.throttling.thresholds.update_snapshot.value", limit)
                .build();
        clusterSettings.applySettings(newSettings);
        assertTrue(throttler.isThrottlingEnabled());
        assertEquals(limit, throttler.getThrottlingLimit(UpdateIndexShardSnapshotStatusRequest.class).intValue());

        newSettings = Settings.builder()
                .put(MasterTaskThrottler.ENABLE_MASTER_THROTTLING.getKey(), false)
                .build();
        clusterSettings.applySettings(newSettings);

        newSettings = Settings.builder()
                .put(MasterTaskThrottler.ENABLE_MASTER_THROTTLING.getKey(), true)
                .build();
        clusterSettings.applySettings(newSettings);

        assertEquals(limit, throttler.getThrottlingLimit(UpdateIndexShardSnapshotStatusRequest.class).intValue());
    }

    public void testUpdateThrottlingLimit() {
        ClusterSettings clusterSettings =
                new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());
        throttler.setThrottlingEnabled(true);

        // set some limit for update snapshot tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder()
                .put("master.throttling.thresholds.update_snapshot.value", newLimit)
                .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(newLimit, throttler.getThrottlingLimit(UpdateIndexShardSnapshotStatusRequest.class).intValue());

        // set update snapshot task limit to default
        newSettings = Settings.builder()
                .put("master.throttling.thresholds.update_snapshot.values", -1)
                .build();
        clusterSettings.applySettings(newSettings);
        assertNull(throttler.getThrottlingLimit(UpdateIndexShardSnapshotStatusRequest.class));
    }

    public void testUpdateLimitWithClassPath() {
        ClusterSettings clusterSettings =
                new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());
        throttler.setThrottlingEnabled(true);

        // set some limit for update snapshot tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder()
                .put("master.throttling.thresholds.org/opensearch/cluster/service/MasterTaskThrottlerTests$TestTask.value", newLimit)
                .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(newLimit, throttler.getThrottlingLimit(TestTask.class).intValue());
    }

    public void testValidateSetting() {
        ClusterSettings clusterSettings =
                new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());

        Settings newSettings = Settings.builder()
                .put("master.throttling.thresholds.update_snapshot.values", -5)
                .build();
        AtomicBoolean exceptionThrown = new AtomicBoolean();
        try {
            throttler.validateSetting(newSettings);
        } catch (IllegalArgumentException e){
            exceptionThrown.set(true);
        }
        assertTrue(exceptionThrown.getAndSet(false));

        newSettings = Settings.builder()
                .put("master.throttling.thresholds.randomClassPath.values", 5)
                .build();

        try {
            throttler.validateSetting(newSettings);
        } catch (IllegalArgumentException e){
            exceptionThrown.set(true);
        }
        assertTrue(exceptionThrown.get());
    }

    public void testUpdateLimit() {
        ClusterSettings clusterSettings =
                new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());
        throttler.setThrottlingEnabled(true);

        throttler.updateLimit(TestTask.class, 5);
        assertEquals(5, throttler.getThrottlingLimit(TestTask.class).intValue());
        throttler.updateLimit(TestTask.class, -1);
        assertNull(throttler.getThrottlingLimit(TestTask.class));
    }

    public class TestTask {
        private final int id;

        TestTask(int id) {
            this.id = id;
        }
    }
}

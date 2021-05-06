/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.LegacyESVersion;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
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
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), LegacyESVersion.V_7_10_3);
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
        for(String key : MasterTaskThrottler.CONFIGURED_TASK_FOR_THROTTLING) {
            assertNull(throttler.getThrottlingLimit(key));
        }
    }

    public void testValidateSettingsForDifferentVersion() {
        DiscoveryNode masterNode = new DiscoveryNode("local_master_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
                Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), LegacyESVersion.V_7_10_3);
        DiscoveryNode dataNode = new DiscoveryNode("local_data_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE), LegacyESVersion.V_7_1_0);
        setState(clusterService, ClusterStateCreationUtils.state(masterNode, masterNode, new DiscoveryNode[]{masterNode, dataNode}));

        ClusterSettings clusterSettings =
            new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());

        // set some limit for update snapshot tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder()
            .put("master.throttling.thresholds.update_snapshot.value", newLimit)
            .build();

        AtomicBoolean exceptionThrown = new AtomicBoolean();
        try {
            throttler.validateSetting(newSettings);
        } catch (IllegalArgumentException e){
            exceptionThrown.set(true);
        }
        assertTrue(exceptionThrown.get());
    }

    public void testValidateSettingsForUnknownTask() {
        DiscoveryNode masterNode = new DiscoveryNode("local_master_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), LegacyESVersion.V_7_10_3);
        DiscoveryNode dataNode = new DiscoveryNode("local_data_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE), LegacyESVersion.V_7_10_3);
        setState(clusterService, ClusterStateCreationUtils.state(masterNode, masterNode, new DiscoveryNode[]{masterNode, dataNode}));

        ClusterSettings clusterSettings =
            new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());

        // set some limit for update snapshot tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder()
            .put("master.throttling.thresholds.random-task.value", newLimit)
            .build();

        AtomicBoolean exceptionThrown = new AtomicBoolean();
        try {
            throttler.validateSetting(newSettings);
        } catch (IllegalArgumentException e){
            exceptionThrown.set(true);
        }
        assertTrue(exceptionThrown.get());
    }

    public void testUpdateThrottlingLimitForHappyCase() {
        DiscoveryNode masterNode = new DiscoveryNode("local_master_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), LegacyESVersion.V_7_10_3);
        DiscoveryNode dataNode = new DiscoveryNode("local_data_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE), LegacyESVersion.V_7_10_3);
        setState(clusterService, ClusterStateCreationUtils.state(masterNode, masterNode, new DiscoveryNode[]{masterNode, dataNode}));

        ClusterSettings clusterSettings =
                new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());
        throttler.setThrottlingEnabled(true);

        // set some limit for update snapshot tasks
        int newLimit = randomIntBetween(1, 10);

        Settings newSettings = Settings.builder()
                .put("master.throttling.thresholds.create-index.value", newLimit)
                .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(newLimit, throttler.getThrottlingLimit("create-index").intValue());

        // set update snapshot task limit to default
        newSettings = Settings.builder()
                .put("master.throttling.thresholds.create-index.value", -1)
                .build();
        clusterSettings.applySettings(newSettings);
        assertNull(throttler.getThrottlingLimit("create-index"));
    }

    public void testValidateSettingForLimit() {
        DiscoveryNode masterNode = new DiscoveryNode("local_master_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), LegacyESVersion.V_7_10_3);
        DiscoveryNode dataNode = new DiscoveryNode("local_data_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE), LegacyESVersion.V_7_10_3);
        setState(clusterService, ClusterStateCreationUtils.state(masterNode, masterNode, new DiscoveryNode[]{masterNode, dataNode}));

        ClusterSettings clusterSettings =
                new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());

        Settings newSettings = Settings.builder()
                .put("master.throttling.thresholds.create-index.values", -5)
                .build();
        AtomicBoolean exceptionThrown = new AtomicBoolean();
        try {
            throttler.validateSetting(newSettings);
        } catch (IllegalArgumentException e){
            exceptionThrown.set(true);
        }
        assertTrue(exceptionThrown.getAndSet(false));
    }

    public void testUpdateLimit() {
        ClusterSettings clusterSettings =
                new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterTaskThrottler throttler = new MasterTaskThrottler(clusterSettings, clusterService.getMasterService());
        throttler.setThrottlingEnabled(true);

        throttler.updateLimit("test", 5);
        assertEquals(5, throttler.getThrottlingLimit("test").intValue());
        throttler.updateLimit("test", -1);
        assertNull(throttler.getThrottlingLimit("test"));
    }
}

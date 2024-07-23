/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.support.clustermanager.term.GetTermVersionAction;
import org.opensearch.action.support.clustermanager.term.GetTermVersionResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.node.IoUsageStats;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControllerStats;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.admin.indices.RestGetAliasesAction;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.ratelimitting.admissioncontrol.AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.CLUSTER_ADMIN_CPU_USAGE_LIMIT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AdmissionForClusterManagerIT extends OpenSearchIntegTestCase {

    private static final Logger LOGGER = LogManager.getLogger(AdmissionForClusterManagerIT.class);

    public static final String INDEX_NAME = "test_index";

    private String clusterManagerNodeId;
    private String datanode;
    private ResourceUsageCollectorService cMResourceCollector;

    private static final Settings DISABLE_ADMISSION_CONTROL = Settings.builder()
        .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.DISABLED.getMode())
        .build();

    private static final Settings ENFORCE_ADMISSION_CONTROL = Settings.builder()
        .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500))
        .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED)
        .put(CLUSTER_ADMIN_CPU_USAGE_LIMIT.getKey(), 50)
        .build();

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    @Before
    public void init() {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(
            Settings.builder().put(DISABLE_ADMISSION_CONTROL).build()
        );
        datanode = internalCluster().startDataOnlyNode(Settings.builder().put(DISABLE_ADMISSION_CONTROL).build());

        ensureClusterSizeConsistency();
        ensureGreen();

        // Disable the automatic resource collection
        clusterManagerNodeId = internalCluster().clusterService(clusterManagerNode).localNode().getId();
        cMResourceCollector = internalCluster().getClusterManagerNodeInstance(ResourceUsageCollectorService.class);
        cMResourceCollector.stop();

        // Enable admission control
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(ENFORCE_ADMISSION_CONTROL).execute().actionGet();
        MockTransportService primaryService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            clusterManagerNode
        );

        // Force always fetch from ClusterManager
        ClusterService clusterService = internalCluster().clusterService();
        GetTermVersionResponse oosTerm = new GetTermVersionResponse(
            new ClusterStateTermVersion(
                clusterService.state().getClusterName(),
                clusterService.state().metadata().clusterUUID(),
                clusterService.state().term() - 1,
                clusterService.state().version() - 1
            )
        );
        primaryService.addRequestHandlingBehavior(
            GetTermVersionAction.NAME,
            (handler, request, channel, task) -> channel.sendResponse(oosTerm)
        );
    }

    public void testAdmissionControlEnforced() throws Exception {
        cMResourceCollector.collectNodeResourceUsageStats(clusterManagerNodeId, System.currentTimeMillis(), 97, 99, new IoUsageStats(98));

        // Write API on ClusterManager
        assertAcked(prepareCreate("test").setMapping("field", "type=text").setAliases("{\"alias1\" : {}}"));
        // Read API on ClusterManager

        GetAliasesRequest aliasesRequest = new GetAliasesRequest();
        aliasesRequest.aliases("alias1");
        try {
            dataNodeClient().admin().indices().getAliases(aliasesRequest).actionGet();
            fail("expected failure");
        } catch (Exception e) {
            assertTrue(e instanceof OpenSearchRejectedExecutionException);
            assertTrue(e.getMessage().contains("CPU usage admission controller rejected the request"));
            assertTrue(e.getMessage().contains("[indices:admin/aliases/get]"));
            assertTrue(e.getMessage().contains("action-type [CLUSTER_ADMIN]"));
        }

        client().admin().cluster().prepareUpdateSettings().setTransientSettings(DISABLE_ADMISSION_CONTROL).execute().actionGet();
        GetAliasesResponse getAliasesResponse = dataNodeClient().admin().indices().getAliases(aliasesRequest).actionGet();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));

        AdmissionControlService admissionControlServiceCM = internalCluster().getClusterManagerNodeInstance(AdmissionControlService.class);

        AdmissionControllerStats admissionStats = getAdmissionControlStats(admissionControlServiceCM).get(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER
        );

        assertEquals(admissionStats.rejectionCount.get(AdmissionControlActionType.CLUSTER_ADMIN.getType()).longValue(), 1);
        assertNull(admissionStats.rejectionCount.get(AdmissionControlActionType.SEARCH.getType()));
        assertNull(admissionStats.rejectionCount.get(AdmissionControlActionType.INDEXING.getType()));
    }

    public void testAdmissionControlEnabledOnNoBreach() throws InterruptedException {
        // CPU usage is less than threshold 50%
        cMResourceCollector.collectNodeResourceUsageStats(clusterManagerNodeId, System.currentTimeMillis(), 97, 35, new IoUsageStats(98));

        // Write API on ClusterManager
        assertAcked(prepareCreate("test").setMapping("field", "type=text").setAliases("{\"alias1\" : {}}").execute().actionGet());

        // Read API on ClusterManager
        GetAliasesRequest aliasesRequest = new GetAliasesRequest();
        aliasesRequest.aliases("alias1");
        GetAliasesResponse getAliasesResponse = dataNodeClient().admin().indices().getAliases(aliasesRequest).actionGet();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));
    }

    public void testAdmissionControlMonitorOnBreach() throws InterruptedException {
        admissionControlDisabledOnBreach(
            Settings.builder().put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.MONITOR.getMode()).build()
        );
    }

    public void testAdmissionControlDisabledOnBreach() throws InterruptedException {
        admissionControlDisabledOnBreach(DISABLE_ADMISSION_CONTROL);
    }

    public void admissionControlDisabledOnBreach(Settings admission) throws InterruptedException {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(admission).execute().actionGet();

        cMResourceCollector.collectNodeResourceUsageStats(clusterManagerNodeId, System.currentTimeMillis(), 97, 97, new IoUsageStats(98));

        // Write API on ClusterManager
        assertAcked(prepareCreate("test").setMapping("field", "type=text").setAliases("{\"alias1\" : {}}").execute().actionGet());

        // Read API on ClusterManager
        GetAliasesRequest aliasesRequest = new GetAliasesRequest();
        aliasesRequest.aliases("alias1");
        GetAliasesResponse getAliasesResponse = dataNodeClient().admin().indices().getAliases(aliasesRequest).actionGet();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));

    }

    public void testAdmissionControlResponseStatus() throws Exception {
        cMResourceCollector.collectNodeResourceUsageStats(clusterManagerNodeId, System.currentTimeMillis(), 97, 99, new IoUsageStats(98));

        // Write API on ClusterManager
        assertAcked(prepareCreate("test").setMapping("field", "type=text").setAliases("{\"alias1\" : {}}"));

        // Read API on ClusterManager
        FakeRestRequest aliasesRequest = new FakeRestRequest();
        aliasesRequest.params().put("name", "alias1");
        CountDownLatch waitForResponse = new CountDownLatch(1);
        AtomicReference<RestResponse> aliasResponse = new AtomicReference<>();
        AbstractRestChannel channel = new AbstractRestChannel(aliasesRequest, true) {

            @Override
            public void sendResponse(RestResponse response) {
                aliasResponse.set(response);
                waitForResponse.countDown();
            }
        };

        RestGetAliasesAction restHandler = internalCluster().getInstance(RestGetAliasesAction.class, datanode);
        restHandler.handleRequest(aliasesRequest, channel, internalCluster().getInstance(NodeClient.class, datanode));

        waitForResponse.await();
        assertEquals(RestStatus.TOO_MANY_REQUESTS, aliasResponse.get().status());
    }

    @Override
    public void tearDown() throws Exception {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(DISABLE_ADMISSION_CONTROL).execute().actionGet();
        super.tearDown();
    }

    Map<String, AdmissionControllerStats> getAdmissionControlStats(AdmissionControlService admissionControlService) {
        Map<String, AdmissionControllerStats> acStats = new HashMap<>();
        for (AdmissionControllerStats admissionControllerStats : admissionControlService.stats().getAdmissionControllerStatsList()) {
            acStats.put(admissionControllerStats.getAdmissionControllerName(), admissionControllerStats);
        }
        return acStats;
    }
}

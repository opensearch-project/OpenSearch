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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControllerStats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import static org.opensearch.ratelimitting.admissioncontrol.AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.CLUSTER_INFO_CPU_USAGE_LIMIT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AdmissionForClusterManagerIT extends OpenSearchIntegTestCase {

    private static final Logger LOGGER = LogManager.getLogger(AdmissionForClusterManagerIT.class);

    public static final String INDEX_NAME = "test_index";

    private String clusterManagerNodeId;
    private ResourceUsageCollectorService cMResourceCollector;

    private static final Settings DISABLE_ADMISSION_CONTROL = Settings.builder()
        .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.DISABLED)
        .build();

    private static final Settings ENFORCE_ADMISSION_CONTROL = Settings.builder()
        .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500))
        .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED)
        .put(CLUSTER_INFO_CPU_USAGE_LIMIT.getKey(), 50)
        .build();

    @Before
    public void init() {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(
            Settings.builder().put(DISABLE_ADMISSION_CONTROL).build()
        );
        internalCluster().startDataOnlyNode(Settings.builder().put(DISABLE_ADMISSION_CONTROL).build());

        ensureClusterSizeConsistency();
        ensureGreen();

        // Disable the automatic resource collection
        clusterManagerNodeId = internalCluster().clusterService(clusterManagerNode).localNode().getId();
        cMResourceCollector = internalCluster().getClusterManagerNodeInstance(ResourceUsageCollectorService.class);
        cMResourceCollector.stop();

        // Enable admission control
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(ENFORCE_ADMISSION_CONTROL).execute().actionGet();
    }

    public void testAdmissionControlEnforced() throws InterruptedException {
        cMResourceCollector.collectNodeResourceUsageStats(clusterManagerNodeId, System.currentTimeMillis(), 97, 99);

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
        }

        client().admin().cluster().prepareUpdateSettings().setTransientSettings(DISABLE_ADMISSION_CONTROL).execute().actionGet();

        GetAliasesResponse getAliasesResponse = dataNodeClient().admin().indices().getAliases(aliasesRequest).actionGet();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));

        AdmissionControlService admissionControlServicePrimary = internalCluster().getClusterManagerNodeInstance(
            AdmissionControlService.class
        );
        AdmissionControllerStats admissionStats = admissionControlServicePrimary.stats().getAdmissionControllerStatsList().get(0);
        assertEquals(admissionStats.rejectionCount.get(AdmissionControlActionType.CLUSTER_INFO.getType()).longValue(), 1);
        assertNull(admissionStats.rejectionCount.get(AdmissionControlActionType.SEARCH.getType()));
        assertNull(admissionStats.rejectionCount.get(AdmissionControlActionType.INDEXING.getType()));
    }

    public void testAdmissionControlEnabledOnNoBreach() throws InterruptedException {
        // CPU usage is less than threshold 50%
        cMResourceCollector.collectNodeResourceUsageStats(clusterManagerNodeId, System.currentTimeMillis(), 97, 35);

        // Write API on ClusterManager
        assertAcked(prepareCreate("test").setMapping("field", "type=text").setAliases("{\"alias1\" : {}}").execute().actionGet());

        // Read API on ClusterManager
        GetAliasesRequest aliasesRequest = new GetAliasesRequest();
        aliasesRequest.aliases("alias1");
        GetAliasesResponse getAliasesResponse = dataNodeClient().admin().indices().getAliases(aliasesRequest).actionGet();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));
    }

    public void testAdmissionControlDisabledOnBreach() throws InterruptedException {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(DISABLE_ADMISSION_CONTROL).execute().actionGet();

        cMResourceCollector.collectNodeResourceUsageStats(clusterManagerNodeId, System.currentTimeMillis(), 97, 97);

        // Write API on ClusterManager
        assertAcked(prepareCreate("test").setMapping("field", "type=text").setAliases("{\"alias1\" : {}}").execute().actionGet());

        // Read API on ClusterManager
        GetAliasesRequest aliasesRequest = new GetAliasesRequest();
        aliasesRequest.aliases("alias1");
        GetAliasesResponse getAliasesResponse = dataNodeClient().admin().indices().getAliases(aliasesRequest).actionGet();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));

    }

    @Override
    public void tearDown() throws Exception {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(DISABLE_ADMISSION_CONTROL).execute().actionGet();
        super.tearDown();
    }
}

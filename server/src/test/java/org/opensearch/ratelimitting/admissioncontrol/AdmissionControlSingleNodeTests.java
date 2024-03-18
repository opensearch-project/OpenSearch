/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControllerStats;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.After;

import static org.opensearch.ratelimitting.admissioncontrol.AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.INDEXING_CPU_USAGE_LIMIT;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.SEARCH_CPU_USAGE_LIMIT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

/**
 * Single node integration tests for admission control
 */
public class AdmissionControlSingleNodeTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
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

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500))
            .put(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500))
            .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED)
            .put(SEARCH_CPU_USAGE_LIMIT.getKey(), 0)
            .put(INDEXING_CPU_USAGE_LIMIT.getKey(), 0)
            .build();
    }

    public void testAdmissionControlRejectionEnforcedMode() throws Exception {
        ensureGreen();
        assertBusy(() -> assertEquals(1, getInstanceFromNode(ResourceUsageCollectorService.class).getAllNodeStatistics().size()));
        // Thread.sleep(700);
        client().admin().indices().prepareCreate("index").execute().actionGet();
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex("index").setSource("foo", "bar " + i));
        }
        // Verify that cluster state is updated
        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(new ClusterStateRequest());
        assertThat(future2.isDone(), is(true));

        // verify bulk request hits 429
        BulkResponse res = client().bulk(bulk.request()).actionGet();
        assertEquals(429, res.getItems()[0].getFailure().getStatus().getStatus());
        AdmissionControlService admissionControlService = getInstanceFromNode(AdmissionControlService.class);
        AdmissionControllerStats acStats = admissionControlService.stats().getAdmissionControllerStatsList().get(0);
        assertEquals(1, (long) acStats.getRejectionCount().get(AdmissionControlActionType.INDEXING.getType()));
        client().admin().indices().prepareRefresh("index").get();

        // verify search request hits 429
        SearchRequest searchRequest = new SearchRequest("index");
        try {
            client().search(searchRequest).actionGet();
        } catch (Exception e) {
            assertTrue(((SearchPhaseExecutionException) e).getDetailedMessage().contains("OpenSearchRejectedExecutionException"));
        }
        acStats = admissionControlService.stats().getAdmissionControllerStatsList().get(0);
        assertEquals(1, (long) acStats.getRejectionCount().get(AdmissionControlActionType.SEARCH.getType()));
    }

    public void testAdmissionControlRejectionMonitorOnlyMode() throws Exception {
        assertBusy(() -> assertEquals(1, getInstanceFromNode(ResourceUsageCollectorService.class).getAllNodeStatistics().size()));
        // Verify that cluster state is updated
        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(new ClusterStateRequest());
        assertThat(future2.isDone(), is(true));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(super.nodeSettings())
                .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.MONITOR.getMode())
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex("index").setSource("foo", "bar " + i));
        }
        // verify bulk request success but admission control having rejections stats
        BulkResponse res = client().bulk(bulk.request()).actionGet();
        assertFalse(res.hasFailures());
        AdmissionControlService admissionControlService = getInstanceFromNode(AdmissionControlService.class);
        AdmissionControllerStats acStats = admissionControlService.stats().getAdmissionControllerStatsList().get(0);
        assertEquals(1, (long) acStats.getRejectionCount().get(AdmissionControlActionType.INDEXING.getType()));
        client().admin().indices().prepareRefresh("index").get();

        // verify search request success but admission control having rejections stats
        SearchRequest searchRequest = new SearchRequest("index");
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertEquals(3, searchResponse.getHits().getHits().length);
        acStats = admissionControlService.stats().getAdmissionControllerStatsList().get(0);
        assertEquals(1, (long) acStats.getRejectionCount().get(AdmissionControlActionType.SEARCH.getType()));
    }

    public void testAdmissionControlRejectionDisabledMode() throws Exception {
        assertBusy(() -> assertEquals(1, getInstanceFromNode(ResourceUsageCollectorService.class).getAllNodeStatistics().size()));
        // Verify that cluster state is updated
        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(new ClusterStateRequest());
        assertThat(future2.isDone(), is(true));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder().put(super.nodeSettings()).put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.DISABLED)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex("index").setSource("foo", "bar " + i));
        }
        // verify bulk request success and no rejections
        BulkResponse res = client().bulk(bulk.request()).actionGet();
        assertFalse(res.hasFailures());
        AdmissionControlService admissionControlService = getInstanceFromNode(AdmissionControlService.class);
        AdmissionControllerStats acStats = admissionControlService.stats().getAdmissionControllerStatsList().get(0);
        assertEquals(0, acStats.getRejectionCount().size());
        client().admin().indices().prepareRefresh("index").get();

        // verify search request success and no rejections
        SearchRequest searchRequest = new SearchRequest("index");
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertEquals(3, searchResponse.getHits().getHits().length);
        acStats = admissionControlService.stats().getAdmissionControllerStatsList().get(0);
        assertEquals(0, acStats.getRejectionCount().size());

    }

    public void testAdmissionControlWithinLimits() throws Exception {
        assertBusy(() -> assertEquals(1, getInstanceFromNode(ResourceUsageCollectorService.class).getAllNodeStatistics().size()));
        // Verify that cluster state is updated
        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(new ClusterStateRequest());
        assertThat(future2.isDone(), is(true));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(super.nodeSettings())
                .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED)
                .put(SEARCH_CPU_USAGE_LIMIT.getKey(), 101)
                .put(INDEXING_CPU_USAGE_LIMIT.getKey(), 101)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex("index").setSource("foo", "bar " + i));
        }
        // verify bulk request success and no rejections
        BulkResponse res = client().bulk(bulk.request()).actionGet();
        assertFalse(res.hasFailures());
        AdmissionControlService admissionControlService = getInstanceFromNode(AdmissionControlService.class);
        AdmissionControllerStats acStats = admissionControlService.stats().getAdmissionControllerStatsList().get(0);
        assertEquals(0, acStats.getRejectionCount().size());
        client().admin().indices().prepareRefresh("index").get();

        // verify search request success and no rejections
        SearchRequest searchRequest = new SearchRequest("index");
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertEquals(3, searchResponse.getHits().getHits().length);
        acStats = admissionControlService.stats().getAdmissionControllerStatsList().get(0);
        assertEquals(0, acStats.getRejectionCount().size());
    }
}

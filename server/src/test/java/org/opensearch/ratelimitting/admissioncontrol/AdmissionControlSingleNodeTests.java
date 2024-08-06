/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.apache.lucene.util.Constants;
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
import org.opensearch.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.IoBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControllerStats;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.After;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.ratelimitting.admissioncontrol.AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.INDEXING_CPU_USAGE_LIMIT;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.SEARCH_CPU_USAGE_LIMIT;
import static org.opensearch.ratelimitting.admissioncontrol.settings.IoBasedAdmissionControllerSettings.INDEXING_IO_USAGE_LIMIT;
import static org.opensearch.ratelimitting.admissioncontrol.settings.IoBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.IoBasedAdmissionControllerSettings.SEARCH_IO_USAGE_LIMIT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

/**
 * Single node integration tests for admission control
 */
public class AdmissionControlSingleNodeTests extends OpenSearchSingleNodeTestCase {

    public static final String INDEX_NAME = "test_index";

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @After
    public void cleanup() {
        client().admin().indices().prepareDelete(INDEX_NAME).get();
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
            .put(ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(5000))
            .put(CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
            .put(SEARCH_CPU_USAGE_LIMIT.getKey(), 0)
            .put(INDEXING_CPU_USAGE_LIMIT.getKey(), 0)
            .build();
    }

    public void testAdmissionControlRejectionEnforcedMode() throws Exception {
        ensureGreen();
        assertBusy(() -> assertEquals(1, getInstanceFromNode(ResourceUsageCollectorService.class).getAllNodeStatistics().size()));
        client().admin().indices().prepareCreate(INDEX_NAME).execute().actionGet();
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("foo", "bar " + i));
        }
        // Verify that cluster state is updated
        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(new ClusterStateRequest());
        assertThat(future2.isDone(), is(true));

        // verify bulk request hits 429
        BulkResponse res = client().bulk(bulk.request()).actionGet();
        assertEquals(429, res.getItems()[0].getFailure().getStatus().getStatus());
        AdmissionControlService admissionControlService = getInstanceFromNode(AdmissionControlService.class);
        Map<String, AdmissionControllerStats> acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(
            1,
            (long) acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount()
                .get(AdmissionControlActionType.INDEXING.getType())
        );
        if (Constants.LINUX) {
            assertEquals(
                0,
                (long) acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER)
                    .getRejectionCount()
                    .getOrDefault(AdmissionControlActionType.INDEXING.getType(), 0L)
            );
        } else {
            assertNull(acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER));
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // verify search request hits 429
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        try {
            client().search(searchRequest).actionGet();
        } catch (Exception e) {
            assertTrue(((SearchPhaseExecutionException) e).getDetailedMessage().contains("OpenSearchRejectedExecutionException"));
        }
        acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(
            1,
            (long) acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount()
                .get(AdmissionControlActionType.SEARCH.getType())
        );
        if (Constants.LINUX) {
            assertEquals(
                0,
                (long) acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER)
                    .getRejectionCount()
                    .getOrDefault(AdmissionControlActionType.SEARCH.getType(), 0L)
            );
        } else {
            assertNull(acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER));
        }
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(super.nodeSettings())
                .put(SEARCH_IO_USAGE_LIMIT.getKey(), 0)
                .put(INDEXING_IO_USAGE_LIMIT.getKey(), 0)
                .put(CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.DISABLED.getMode())
                .put(IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("foo", "bar " + i));
        }
        res = client().bulk(bulk.request()).actionGet();
        if (Constants.LINUX) {
            assertEquals(429, res.getItems()[0].getFailure().getStatus().getStatus());
        }
        admissionControlService = getInstanceFromNode(AdmissionControlService.class);
        acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(
            1,
            (long) acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount()
                .get(AdmissionControlActionType.INDEXING.getType())
        );
        if (Constants.LINUX) {
            assertEquals(
                1,
                (long) acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER)
                    .getRejectionCount()
                    .getOrDefault(AdmissionControlActionType.INDEXING.getType(), 0L)
            );
        } else {
            assertNull(acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER));
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // verify search request hits 429
        searchRequest = new SearchRequest(INDEX_NAME);
        try {
            client().search(searchRequest).actionGet();
        } catch (Exception e) {
            assertTrue(((SearchPhaseExecutionException) e).getDetailedMessage().contains("OpenSearchRejectedExecutionException"));
        }
        acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(
            1,
            (long) acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount()
                .get(AdmissionControlActionType.SEARCH.getType())
        );
        if (Constants.LINUX) {
            assertEquals(
                1,
                (long) acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER)
                    .getRejectionCount()
                    .getOrDefault(AdmissionControlActionType.SEARCH.getType(), 0L)
            );
        } else {
            assertNull(acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER));
        }
    }

    public void testAdmissionControlRejectionMonitorOnlyMode() throws Exception {
        assertBusy(() -> assertEquals(1, getInstanceFromNode(ResourceUsageCollectorService.class).getAllNodeStatistics().size()));
        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(new ClusterStateRequest());
        assertThat(future2.isDone(), is(true));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(super.nodeSettings())
                .put(CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.MONITOR.getMode())
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("foo", "bar " + i));
        }
        // verify bulk request success but admission control having rejections stats
        BulkResponse res = client().bulk(bulk.request()).actionGet();
        assertFalse(res.hasFailures());
        AdmissionControlService admissionControlService = getInstanceFromNode(AdmissionControlService.class);
        Map<String, AdmissionControllerStats> acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(
            1,
            (long) acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount()
                .get(AdmissionControlActionType.INDEXING.getType())
        );
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // verify search request success but admission control having rejections stats
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertEquals(3, searchResponse.getHits().getHits().length);
        acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(
            1,
            (long) acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount()
                .get(AdmissionControlActionType.SEARCH.getType())
        );

        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(super.nodeSettings())
                .put(SEARCH_IO_USAGE_LIMIT.getKey(), 0)
                .put(INDEXING_IO_USAGE_LIMIT.getKey(), 0)
                .put(SEARCH_CPU_USAGE_LIMIT.getKey(), 101)
                .put(INDEXING_CPU_USAGE_LIMIT.getKey(), 101)
                .put(CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.DISABLED.getMode())
                .put(IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.MONITOR.getMode())
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("foo", "bar " + i));
        }
        // verify bulk request success but admission control having rejections stats
        res = client().bulk(bulk.request()).actionGet();
        assertFalse(res.hasFailures());
        acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(
            1,
            (long) acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount()
                .get(AdmissionControlActionType.INDEXING.getType())
        );
        if (Constants.LINUX) {
            assertEquals(
                1,
                (long) acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER)
                    .getRejectionCount()
                    .getOrDefault(AdmissionControlActionType.INDEXING.getType(), 0L)
            );
        } else {
            assertNull(acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER));
        }
        searchRequest = new SearchRequest(INDEX_NAME);
        searchResponse = client().search(searchRequest).actionGet();
        assertEquals(3, searchResponse.getHits().getHits().length);
        acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(
            1,
            (long) acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount()
                .get(AdmissionControlActionType.SEARCH.getType())
        );
        if (Constants.LINUX) {
            assertEquals(
                1,
                (long) acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER)
                    .getRejectionCount()
                    .getOrDefault(AdmissionControlActionType.SEARCH.getType(), 0L)
            );
        } else {
            assertNull(acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER));
        }
    }

    public void testAdmissionControlRejectionDisabledMode() throws Exception {
        assertBusy(() -> assertEquals(1, getInstanceFromNode(ResourceUsageCollectorService.class).getAllNodeStatistics().size()));
        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(new ClusterStateRequest());
        assertThat(future2.isDone(), is(true));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(super.nodeSettings())
                .put(CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.DISABLED.getMode())
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("foo", "bar " + i));
        }
        // verify bulk request success and no rejections
        BulkResponse res = client().bulk(bulk.request()).actionGet();
        assertFalse(res.hasFailures());
        AdmissionControlService admissionControlService = getInstanceFromNode(AdmissionControlService.class);
        Map<String, AdmissionControllerStats> acStats = this.getAdmissionControlStats(admissionControlService);

        assertEquals(0, acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER).getRejectionCount().size());
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // verify search request success and no rejections
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertEquals(3, searchResponse.getHits().getHits().length);
        acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(0, acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER).getRejectionCount().size());
        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(super.nodeSettings())
                .put(IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.DISABLED.getMode())
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("foo", "bar " + i));
        }
        // verify bulk request success but admission control having rejections stats
        res = client().bulk(bulk.request()).actionGet();
        assertFalse(res.hasFailures());
        acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(0, acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER).getRejectionCount().size());
        if (Constants.LINUX) {
            assertEquals(0, acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount().size());
        } else {
            assertNull(acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER));
        }

        searchRequest = new SearchRequest(INDEX_NAME);
        searchResponse = client().search(searchRequest).actionGet();
        assertEquals(3, searchResponse.getHits().getHits().length);
        acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(0, acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER).getRejectionCount().size());
        if (Constants.LINUX) {
            assertEquals(0, acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount().size());
        } else {
            assertNull(acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER));
        }
    }

    public void testAdmissionControlWithinLimits() throws Exception {
        assertBusy(() -> assertEquals(1, getInstanceFromNode(ResourceUsageCollectorService.class).getAllNodeStatistics().size()));
        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(new ClusterStateRequest());
        assertThat(future2.isDone(), is(true));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(super.nodeSettings())
                .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
                .put(SEARCH_CPU_USAGE_LIMIT.getKey(), 101)
                .put(INDEXING_CPU_USAGE_LIMIT.getKey(), 101)
                .put(SEARCH_IO_USAGE_LIMIT.getKey(), 101)
                .put(INDEXING_IO_USAGE_LIMIT.getKey(), 101)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("foo", "bar " + i));
        }
        // verify bulk request success and no rejections
        BulkResponse res = client().bulk(bulk.request()).actionGet();
        assertFalse(res.hasFailures());
        AdmissionControlService admissionControlService = getInstanceFromNode(AdmissionControlService.class);
        Map<String, AdmissionControllerStats> acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(0, acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER).getRejectionCount().size());
        if (Constants.LINUX) {
            assertEquals(0, acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount().size());
        } else {
            assertNull(acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER));
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // verify search request success and no rejections
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertEquals(3, searchResponse.getHits().getHits().length);
        acStats = this.getAdmissionControlStats(admissionControlService);
        assertEquals(0, acStats.get(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER).getRejectionCount().size());
        if (Constants.LINUX) {
            assertEquals(0, acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount().size());
        } else {
            assertNull(acStats.get(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER));
        }
    }

    Map<String, AdmissionControllerStats> getAdmissionControlStats(AdmissionControlService admissionControlService) {
        Map<String, AdmissionControllerStats> acStats = new HashMap<>();
        for (AdmissionControllerStats admissionControllerStats : admissionControlService.stats().getAdmissionControllerStatsList()) {
            acStats.put(admissionControllerStats.getAdmissionControllerName(), admissionControllerStats);
        }
        return acStats;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.node.IoUsageStats;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.action.admin.indices.forcemerge.TransportForceMergeAction.FORCE_MERGE_ADMISSION_CONTROL_ENABLED;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.core.rest.RestStatus.TOO_MANY_REQUESTS;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.FORCE_MERGE_CPU_USAGE_LIMIT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * HTTP status of the force merge API when admission control rejects shards. The transport response always carries the
 * per shard failures; this verifies how they are mapped to the HTTP status: 429 only when no shard succeeded, 200 for a
 * partial rejection, 200 for the asynchronous variant that returns a task, and 200 (as before admission control) when
 * no shard was available to merge. Resource pressure is simulated by freezing each node's
 * {@link ResourceUsageCollectorService} and injecting a fixed CPU utilization.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ForceMergeAdmissionControlRestIT extends HttpSmokeTestCase {

    private static final String INDEX = "test-index";
    private static final String UNASSIGNED_INDEX = "unassigned-index";
    private static final double HIGH_CPU = 90;
    private static final double LOW_CPU = 10;
    private static final long FORCE_MERGE_LIMIT = 50;

    public void testAllShardsRejectedReturns429() throws Exception {
        List<String> nodes = startDataNodes(2);
        createIndexWithDocs(2, 1);
        enableForceMergeAdmissionControl();
        nodes.forEach(node -> setCpu(node, HIGH_CPU));

        ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(forceMergeRequest()));

        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(TOO_MANY_REQUESTS.getStatus()));
        XContentTestUtils.JsonMapView body = body(e.getResponse());
        assertThat(body.get("_shards.total"), equalTo(4));
        assertThat(body.get("_shards.successful"), equalTo(0));
        assertThat(body.get("_shards.failed"), equalTo(4));
        // failures with the same cause are grouped in the body, so there may be fewer entries than failed shards
        List<Map<String, Object>> failures = body.get("_shards.failures");
        assertThat(failures.size(), greaterThan(0));
        for (Map<String, Object> failure : failures) {
            assertThat(failure.get("status"), equalTo(TOO_MANY_REQUESTS.name()));
        }
    }

    public void testPartialRejectionReturns200() throws Exception {
        List<String> nodes = startDataNodes(2);
        createIndexWithDocs(4, 1);
        enableForceMergeAdmissionControl();
        String constrainedNode = nodes.get(0);
        setCpu(constrainedNode, HIGH_CPU);
        setCpu(nodes.get(1), LOW_CPU);
        int copiesOnConstrainedNode = shardCopiesOn(nodeId(constrainedNode));
        assertThat(copiesOnConstrainedNode, greaterThan(0));
        assertThat(copiesOnConstrainedNode, lessThan(8));

        Response response = getRestClient().performRequest(forceMergeRequest());

        assertThat(response.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));
        XContentTestUtils.JsonMapView body = body(response);
        assertThat(body.get("_shards.total"), equalTo(8));
        assertThat(body.get("_shards.failed"), equalTo(copiesOnConstrainedNode));
        assertThat(body.get("_shards.successful"), equalTo(8 - copiesOnConstrainedNode));
    }

    public void testAsyncForceMergeUnaffectedByStatusChange() throws Exception {
        List<String> nodes = startDataNodes(2);
        createIndexWithDocs(2, 1);
        enableForceMergeAdmissionControl();
        nodes.forEach(node -> setCpu(node, HIGH_CPU));

        Request request = forceMergeRequest();
        request.addParameter("wait_for_completion", "false");
        Response response = getRestClient().performRequest(request);

        // the task is returned before any shard is attempted, so rejections cannot influence the status
        assertThat(response.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));
        String taskId = body(response).get("task");
        assertThat(taskId, notNullValue());

        // The force merge keeps running in the background and stores its result in the tasks index. Wait for it so the
        // test does not end (and stop the cluster) while that is still in flight; the rejections are in the stored result.
        Request getTask = new Request("GET", "/_tasks/" + taskId);
        getTask.addParameter("wait_for_completion", "true");
        getTask.addParameter("timeout", "30s");
        XContentTestUtils.JsonMapView task = body(getRestClient().performRequest(getTask));
        assertThat(task.get("completed"), equalTo(true));
        assertThat(task.get("response._shards.total"), equalTo(4));
        assertThat(task.get("response._shards.successful"), equalTo(0));
        assertThat(task.get("response._shards.failed"), equalTo(4));
    }

    public void testAllShardsUnassignedReturns200() throws Exception {
        // Unassigned shards are counted in the total but are not failures. Force merge returned 200 for them before
        // admission control and must keep doing so, even with every node over the limit.
        List<String> nodes = startDataNodes(2);
        createUnassignedIndex(2);
        enableForceMergeAdmissionControl();
        nodes.forEach(node -> setCpu(node, HIGH_CPU));

        Response response = getRestClient().performRequest(forceMergeRequest(UNASSIGNED_INDEX));

        assertThat(response.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));
        XContentTestUtils.JsonMapView body = body(response);
        assertThat(body.get("_shards.total"), equalTo(2));
        assertThat(body.get("_shards.successful"), equalTo(0));
        assertThat(body.get("_shards.failed"), equalTo(0));
    }

    private List<String> startDataNodes(int count) {
        List<String> nodes = internalCluster().startNodes(count);
        ensureStableCluster(count);
        return nodes;
    }

    private void createIndexWithDocs(int shards, int replicas) {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
            )
        );
        ensureGreen(INDEX);
        for (int batch = 0; batch < 3; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < 10; i++) {
                bulk.add(client().prepareIndex(INDEX).setSource("field", "value-" + batch + "-" + i));
            }
            assertFalse(bulk.get().hasFailures());
            client().admin().indices().prepareRefresh(INDEX).get();
        }
    }

    /**
     * Creates an index whose shards can never be allocated, because they require a node that does not exist.
     */
    private void createUnassignedIndex(int shards) {
        assertAcked(
            prepareCreate(UNASSIGNED_INDEX).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", "no-such-node")
            ).setWaitForActiveShards(ActiveShardCount.NONE)
        );
        for (ShardRouting shardRouting : clusterService().state().routingTable().allShards(UNASSIGNED_INDEX)) {
            assertTrue(shardRouting.toString(), shardRouting.unassigned());
        }
    }

    private void enableForceMergeAdmissionControl() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        .put(FORCE_MERGE_ADMISSION_CONTROL_ENABLED.getKey(), true)
                        .put(CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
                        .put(FORCE_MERGE_CPU_USAGE_LIMIT.getKey(), FORCE_MERGE_LIMIT)
                )
        );
    }

    /**
     * Freezes the node's resource usage collector and injects the given CPU utilization for the local node.
     */
    private void setCpu(String node, double cpuPercent) {
        ResourceUsageCollectorService collector = internalCluster().getInstance(ResourceUsageCollectorService.class, node);
        collector.stop();
        String nodeId = nodeId(node);
        collector.collectNodeResourceUsageStats(nodeId, System.currentTimeMillis(), 0, cpuPercent, new IoUsageStats(0), 0);
        assertEquals(cpuPercent, collector.getNodeStatistics(nodeId).get().getCpuUtilizationPercent(), 0.0);
    }

    private static Request forceMergeRequest() {
        return forceMergeRequest(INDEX);
    }

    private static Request forceMergeRequest(String indices) {
        Request request = new Request("POST", "/" + indices + "/_forcemerge");
        request.addParameter("max_num_segments", "1");
        return request;
    }

    private static XContentTestUtils.JsonMapView body(Response response) throws IOException {
        return new XContentTestUtils.JsonMapView(
            XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true)
        );
    }

    private String nodeId(String node) {
        return internalCluster().clusterService(node).localNode().getId();
    }

    private int shardCopiesOn(String nodeId) {
        int copies = 0;
        for (ShardRouting shardRouting : clusterService().state().routingTable().allShards(INDEX)) {
            if (nodeId.equals(shardRouting.currentNodeId())) {
                copies++;
            }
        }
        return copies;
    }
}

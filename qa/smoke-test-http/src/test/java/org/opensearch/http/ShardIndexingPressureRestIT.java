/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.http;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.IndexingPressure;
import org.opensearch.index.ShardIndexingPressureSettings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.XContentTestUtils;

import java.util.ArrayList;
import java.util.Map;

import static org.opensearch.core.rest.RestStatus.CREATED;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.core.rest.RestStatus.TOO_MANY_REQUESTS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Test Shard Indexing Pressure Metrics and Statistics
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 2,
    numClientNodes = 0)
public class ShardIndexingPressureRestIT extends HttpSmokeTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "1KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .build();
    }

    @SuppressWarnings("unchecked")
    public void testShardIndexingPressureStats() throws Exception {
        Request createRequest = new Request("PUT", "/index_name");
        createRequest.setJsonEntity("{\"settings\": {\"index\": {\"number_of_shards\": 1, \"number_of_replicas\": 1, " +
            "\"write.wait_for_active_shards\": 2}}}");
        final Response indexCreatedResponse = getRestClient().performRequest(createRequest);
        assertThat(indexCreatedResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        Request successfulIndexingRequest = new Request("POST", "/index_name/_doc/");
        successfulIndexingRequest.setJsonEntity("{\"x\": \"small text\"}");
        final Response indexSuccessFul = getRestClient().performRequest(successfulIndexingRequest);
        assertThat(indexSuccessFul.getStatusLine().getStatusCode(), equalTo(CREATED.getStatus()));

        Request getShardStats1 = new Request("GET", "/_nodes/stats/shard_indexing_pressure?include_all");
        final Response shardStats1 = getRestClient().performRequest(getShardStats1);
        Map<String, Object> shardStatsMap1 = XContentHelper.convertToMap(JsonXContent.jsonXContent, shardStats1.getEntity().getContent(),
            true);
        ArrayList<Object> values1 = new ArrayList<>(((Map<Object, Object>) shardStatsMap1.get("nodes")).values());
        assertThat(values1.size(), equalTo(2));
        XContentTestUtils.JsonMapView node1 = new XContentTestUtils.JsonMapView((Map<String, Object>) values1.get(0));
        ArrayList<Object> shard1IndexingPressureValues = new ArrayList<>(((Map<Object, Object>) node1.get("shard_indexing_pressure.stats"))
            .values());
        assertThat(shard1IndexingPressureValues.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard1 = new XContentTestUtils.JsonMapView((Map<String, Object>) shard1IndexingPressureValues.get(0));
        Integer node1TotalLimitsRejections = node1.get("shard_indexing_pressure.total_rejections_breakup.node_limits");
        Integer shard1CoordinatingBytes = shard1.get("memory.total.coordinating_in_bytes");
        Integer shard1PrimaryBytes = shard1.get("memory.total.primary_in_bytes");
        Integer shard1ReplicaBytes = shard1.get("memory.total.replica_in_bytes");
        Integer shard1CoordinatingRejections = shard1.get("rejection.coordinating.coordinating_rejections");
        Integer shard1PrimaryRejections = shard1.get("rejection.primary.primary_rejections");
        Integer shard1CoordinatingNodeRejections = shard1.get("rejection.coordinating.breakup.node_limits");

        XContentTestUtils.JsonMapView node2 = new XContentTestUtils.JsonMapView((Map<String, Object>) values1.get(1));
        ArrayList<Object> shard2IndexingPressureValues = new ArrayList<>(((Map<Object, Object>) node2.get("shard_indexing_pressure.stats"))
            .values());
        assertThat(shard2IndexingPressureValues.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard2 = new XContentTestUtils.JsonMapView((Map<String, Object>) shard2IndexingPressureValues.get(0));
        Integer node2TotalLimitsRejections = node2.get("shard_indexing_pressure.total_rejections_breakup.node_limits");
        Integer shard2CoordinatingBytes = shard2.get("memory.total.coordinating_in_bytes");
        Integer shard2PrimaryBytes = shard2.get("memory.total.primary_in_bytes");
        Integer shard2ReplicaBytes = shard2.get("memory.total.replica_in_bytes");
        Integer shard2CoordinatingRejections = shard2.get("rejection.coordinating.coordinating_rejections");
        Integer shard2PrimaryRejections = shard2.get("rejection.primary.primary_rejections");
        Integer shard2CoordinatingNodeRejections = shard2.get("rejection.coordinating.breakup.node_limits");


        if (shard1CoordinatingBytes == 0) {
            assertThat(shard2CoordinatingBytes, greaterThan(0));
            assertThat(shard2CoordinatingBytes, lessThan(1024));
        } else {
            assertThat(shard1CoordinatingBytes, greaterThan(0));
            assertThat(shard1CoordinatingBytes, lessThan(1024));
        }

        if (shard1ReplicaBytes == 0) {
            assertThat(shard1PrimaryBytes, greaterThan(0));
            assertThat(shard1PrimaryBytes, lessThan(1024));

            assertThat(shard2ReplicaBytes, greaterThan(0));
            assertThat(shard2ReplicaBytes, lessThan(1024));
        } else {
            assertThat(shard2PrimaryBytes, greaterThan(0));
            assertThat(shard2PrimaryBytes, lessThan(1024));

            assertThat(shard2ReplicaBytes, equalTo(0));
            assertThat(shard1ReplicaBytes, lessThan(1024));
        }

        assertThat(shard1CoordinatingRejections, equalTo(0));
        assertThat(shard1PrimaryRejections, equalTo(0));
        assertThat(shard2CoordinatingRejections, equalTo(0));
        assertThat(shard2PrimaryRejections, equalTo(0));
        assertThat(shard1CoordinatingNodeRejections, equalTo(0));
        assertThat(node1TotalLimitsRejections, equalTo(0));
        assertThat(node2TotalLimitsRejections, equalTo(0));

        Request getNodeStats = new Request("GET", "/_nodes/stats/indexing_pressure");
        final Response nodeStats = getRestClient().performRequest(getNodeStats);
        Map<String, Object> nodeStatsMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, nodeStats.getEntity().getContent(), true);
        values1 = new ArrayList<>(((Map<Object, Object>) nodeStatsMap.get("nodes")).values());
        assertThat(values1.size(), equalTo(2));
        node1 = new XContentTestUtils.JsonMapView((Map<String, Object>) values1.get(0));
        Integer node1CombinedBytes = node1.get("indexing_pressure.memory.total.combined_coordinating_and_primary_in_bytes");
        Integer node1PrimaryBytes = node1.get("indexing_pressure.memory.total.primary_in_bytes");
        Integer node1ReplicaBytes = node1.get("indexing_pressure.memory.total.replica_in_bytes");
        Integer node1CoordinatingRejections = node1.get("indexing_pressure.memory.total.coordinating_rejections");
        Integer node1PrimaryRejections = node1.get("indexing_pressure.memory.total.primary_rejections");
        node2 = new XContentTestUtils.JsonMapView((Map<String, Object>) values1.get(1));
        Integer node2IndexingBytes = node2.get("indexing_pressure.memory.total.combined_coordinating_and_primary_in_bytes");
        Integer node2PrimaryBytes = node2.get("indexing_pressure.memory.total.primary_in_bytes");
        Integer node2ReplicaBytes = node2.get("indexing_pressure.memory.total.replica_in_bytes");
        Integer node2CoordinatingRejections = node2.get("indexing_pressure.memory.total.coordinating_rejections");
        Integer node2PrimaryRejections = node2.get("indexing_pressure.memory.total.primary_rejections");

        if (node1CombinedBytes == 0) {
            assertThat(node2IndexingBytes, greaterThan(0));
            assertThat(node2IndexingBytes, lessThan(1024));
        } else {
            assertThat(node1CombinedBytes, greaterThan(0));
            assertThat(node1CombinedBytes, lessThan(1024));
        }

        if (node1ReplicaBytes == 0) {
            assertThat(node1PrimaryBytes, greaterThan(0));
            assertThat(node1PrimaryBytes, lessThan(1024));

            assertThat(node2ReplicaBytes, greaterThan(0));
            assertThat(node2ReplicaBytes, lessThan(1024));
        } else {
            assertThat(node2PrimaryBytes, greaterThan(0));
            assertThat(node2PrimaryBytes, lessThan(1024));

            assertThat(node2ReplicaBytes, equalTo(0));
            assertThat(node1ReplicaBytes, lessThan(1024));
        }

        assertThat(node1CoordinatingRejections, equalTo(0));
        assertThat(node1PrimaryRejections, equalTo(0));
        assertThat(node2CoordinatingRejections, equalTo(0));
        assertThat(node2PrimaryRejections, equalTo(0));

        Request failedIndexingRequest = new Request("POST", "/index_name/_doc/");
        String largeString = randomAlphaOfLength(10000);
        failedIndexingRequest.setJsonEntity("{\"x\": " + largeString + "}");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(failedIndexingRequest));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(TOO_MANY_REQUESTS.getStatus()));

        Request getShardStats2 = new Request("GET", "/_nodes/stats/shard_indexing_pressure?include_all");
        final Response shardStats2 = getRestClient().performRequest(getShardStats2);
        Map<String, Object> shardStatsMap2 = XContentHelper.convertToMap(JsonXContent.jsonXContent, shardStats2.getEntity().getContent(),
            true);
        ArrayList<Object> values2 = new ArrayList<>(((Map<Object, Object>) shardStatsMap2.get("nodes")).values());
        assertThat(values2.size(), equalTo(2));
        XContentTestUtils.JsonMapView node1AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>) values2.get(0));
        ArrayList<Object> shard1IndexingPressureValuesAfterRejection = new ArrayList<>(((Map<Object, Object>) node1AfterRejection
            .get("shard_indexing_pressure.stats")).values());
        assertThat(shard1IndexingPressureValuesAfterRejection.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard1AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>)
            shard1IndexingPressureValuesAfterRejection.get(0));
        node1TotalLimitsRejections = node1AfterRejection.get("shard_indexing_pressure.total_rejections_breakup.node_limits");
        shard1CoordinatingRejections = shard1AfterRejection.get("rejection.coordinating.coordinating_rejections");
        shard1PrimaryRejections = shard1AfterRejection.get("rejection.primary.primary_rejections");
        shard1CoordinatingNodeRejections = shard1AfterRejection.get("rejection.coordinating.breakup.node_limits");

        XContentTestUtils.JsonMapView node2AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>) values2.get(1));
        ArrayList<Object> shard2IndexingPressureValuesAfterRejection = new ArrayList<>(((Map<Object, Object>) node2AfterRejection
            .get("shard_indexing_pressure.stats")).values());
        assertThat(shard2IndexingPressureValuesAfterRejection.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard2AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>)
            shard2IndexingPressureValuesAfterRejection.get(0));
        node2TotalLimitsRejections = node2AfterRejection.get("shard_indexing_pressure.total_rejections_breakup.node_limits");
        shard2CoordinatingRejections = shard2AfterRejection.get("rejection.coordinating.coordinating_rejections");
        shard2PrimaryRejections = shard2AfterRejection.get("rejection.primary.primary_rejections");
        shard2CoordinatingNodeRejections = shard2AfterRejection.get("rejection.coordinating.breakup.node_limits");

        if (shard1CoordinatingRejections == 0) {
            assertThat(shard2CoordinatingRejections, equalTo(1));
            assertThat(shard2CoordinatingNodeRejections, equalTo(1));
            assertThat(node2TotalLimitsRejections, equalTo(1));
        } else {
            assertThat(shard1CoordinatingRejections, equalTo(1));
            assertThat(shard1CoordinatingNodeRejections, equalTo(1));
            assertThat(node1TotalLimitsRejections, equalTo(1));
        }

        assertThat(shard1PrimaryRejections, equalTo(0));
        assertThat(shard2PrimaryRejections, equalTo(0));

        Request getNodeStats2 = new Request("GET", "/_nodes/stats/indexing_pressure");
        final Response nodeStats2 = getRestClient().performRequest(getNodeStats2);
        Map<String, Object> nodeStatsMap2 = XContentHelper.convertToMap(JsonXContent.jsonXContent, nodeStats2.getEntity().getContent(),
            true);
        values2 = new ArrayList<>(((Map<Object, Object>) nodeStatsMap2.get("nodes")).values());
        assertThat(values2.size(), equalTo(2));
        node1AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>) values2.get(0));
        node1CoordinatingRejections = node1AfterRejection.get("indexing_pressure.memory.total.coordinating_rejections");
        node1PrimaryRejections = node1.get("indexing_pressure.memory.total.primary_rejections");
        node2AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>) values2.get(1));
        node2CoordinatingRejections = node2AfterRejection.get("indexing_pressure.memory.total.coordinating_rejections");
        node2PrimaryRejections = node2AfterRejection.get("indexing_pressure.memory.total.primary_rejections");

        if (node1CoordinatingRejections == 0) {
            assertThat(node2CoordinatingRejections, equalTo(1));
        } else {
            assertThat(node1CoordinatingRejections, equalTo(1));
        }

        assertThat(node1PrimaryRejections, equalTo(0));
        assertThat(node2PrimaryRejections, equalTo(0));

        // Update cluster setting to enable shadow mode
        Request updateSettingRequest = new Request("PUT", "/_cluster/settings");
        updateSettingRequest.setJsonEntity("{\"persistent\": {\"shard_indexing_pressure\": {\"enforced\": \"false\"}}}");
        final Response updateSettingResponse = getRestClient().performRequest(updateSettingRequest);
        assertThat(updateSettingResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        Request shadowIndexingRequest = new Request("POST", "/index_name/_doc/");
        shadowIndexingRequest.setJsonEntity("{\"x\": \"text\"}");
        final Response shadowIndexingResponse = getRestClient().performRequest(shadowIndexingRequest);
        assertThat(shadowIndexingResponse.getStatusLine().getStatusCode(), equalTo(CREATED.getStatus()));

        Request getShardStats3 = new Request("GET", "/_nodes/stats/shard_indexing_pressure?include_all");
        final Response shardStats3 = getRestClient().performRequest(getShardStats3);
        Map<String, Object> shardStatsMap3 = XContentHelper.convertToMap(JsonXContent.jsonXContent, shardStats3.getEntity().getContent(),
            true);
        ArrayList<Object> values3 = new ArrayList<>(((Map<Object, Object>) shardStatsMap3.get("nodes")).values());
        assertThat(values3.size(), equalTo(2));
        XContentTestUtils.JsonMapView node1AfterShadowMode = new XContentTestUtils.JsonMapView((Map<String, Object>)
            values3.get(0));
        ArrayList<Object> shard1IndexingPressureValuesAfterShadowMode = new ArrayList<>(((Map<Object, Object>)
            node1AfterShadowMode.get("shard_indexing_pressure.stats")).values());
        assertThat(shard1IndexingPressureValuesAfterShadowMode.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard1AfterShadowMode = new XContentTestUtils.JsonMapView((Map<String, Object>)
            shard1IndexingPressureValuesAfterShadowMode.get(0));
        node1TotalLimitsRejections = node1AfterShadowMode.get("shard_indexing_pressure.total_rejections_breakup_shadow_mode" +
            ".node_limits");
        shard1CoordinatingRejections = shard1AfterShadowMode.get("rejection.coordinating.coordinating_rejections");
        shard1PrimaryRejections = shard1AfterShadowMode.get("rejection.primary.primary_rejections");
        shard1CoordinatingNodeRejections = shard1AfterShadowMode.get("rejection.coordinating.breakup_shadow_mode.node_limits");

        XContentTestUtils.JsonMapView node2AfterShadowMode = new XContentTestUtils.JsonMapView((Map<String, Object>)
            values3.get(1));
        ArrayList<Object> shard2IndexingPressureValuesAfterShadowMode = new ArrayList<>(((Map<Object, Object>)
            node2AfterShadowMode.get("shard_indexing_pressure.stats")).values());
        assertThat(shard2IndexingPressureValuesAfterShadowMode.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard2AfterShadowMode = new XContentTestUtils.JsonMapView((Map<String, Object>)
            shard2IndexingPressureValuesAfterShadowMode.get(0));
        node2TotalLimitsRejections = node2AfterShadowMode.get("shard_indexing_pressure.total_rejections_breakup_shadow_mode" +
            ".node_limits");
        shard2CoordinatingRejections = shard2AfterShadowMode.get("rejection.coordinating.coordinating_rejections");
        shard2PrimaryRejections = shard2AfterShadowMode.get("rejection.primary.primary_rejections");
        shard2CoordinatingNodeRejections = shard2AfterShadowMode.get("rejection.coordinating.breakup_shadow_mode.node_limits");

        if (shard1CoordinatingRejections == 0) {
            assertThat(shard2CoordinatingRejections, equalTo(1));
            assertThat(shard2CoordinatingNodeRejections, equalTo(1));
            assertThat(node2TotalLimitsRejections, equalTo(1));
        } else {
            assertThat(shard1CoordinatingRejections, equalTo(1));
            assertThat(shard1CoordinatingNodeRejections, equalTo(1));
            assertThat(node1TotalLimitsRejections, equalTo(1));
        }

        assertThat(shard1PrimaryRejections, equalTo(0));
        assertThat(shard2PrimaryRejections, equalTo(0));

        //Reset persistent setting to clear cluster metadata
        updateSettingRequest = new Request("PUT", "/_cluster/settings");
        updateSettingRequest.setJsonEntity("{\"persistent\": {\"shard_indexing_pressure\": {\"enforced\": null}}}");
        getRestClient().performRequest(updateSettingRequest);
    }
}

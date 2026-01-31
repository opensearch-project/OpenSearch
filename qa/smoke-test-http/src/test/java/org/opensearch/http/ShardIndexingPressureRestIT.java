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

import java.io.InputStream;
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
@OpenSearchIntegTestCase.ClusterScope(
    scope = OpenSearchIntegTestCase.Scope.SUITE,
    supportsDedicatedMasters = false,
    numDataNodes = 2,
    numClientNodes = 0
)
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

    /**
     * Response is NOT AutoCloseable. To avoid leaking tracked HTTP channels, drain and close the entity stream.
     * Best-effort: any exception is ignored since this is test cleanup.
     */
    private static void consumeEntity(Response response) {
        if (response == null) return;
        try {
            if (response.getEntity() == null) return;
            try (InputStream is = response.getEntity().getContent()) {
                byte[] buf = new byte[8192];
                while (is.read(buf) != -1) {
                }
            }
        } catch (Exception ignored) {
        }
    }

    @SuppressWarnings("unchecked")
    public void testShardIndexingPressureStats() throws Exception {
        Request createRequest = new Request("PUT", "/index_name");
        createRequest.setJsonEntity(
            "{\"settings\": {\"index\": {\"number_of_shards\": 1, \"number_of_replicas\": 1, \"write.wait_for_active_shards\": 2}}}"
        );
        final Response indexCreatedResponse = getRestClient().performRequest(createRequest);
        try {
            assertThat(indexCreatedResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));
        } finally {
            consumeEntity(indexCreatedResponse);
        }

        Request successfulIndexingRequest = new Request("POST", "/index_name/_doc/");
        successfulIndexingRequest.setJsonEntity("{\"x\": \"small text\"}");
        final Response indexSuccessFul = getRestClient().performRequest(successfulIndexingRequest);
        try {
            assertThat(indexSuccessFul.getStatusLine().getStatusCode(), equalTo(CREATED.getStatus()));
        } finally {
            consumeEntity(indexSuccessFul);
        }

        Request getShardStats1 = new Request("GET", "/_nodes/stats/shard_indexing_pressure?include_all");
        final Response shardStats1 = getRestClient().performRequest(getShardStats1);
        final Map<String, Object> shardStatsMap1;
        try (InputStream is = shardStats1.getEntity().getContent()) {
            shardStatsMap1 = XContentHelper.convertToMap(JsonXContent.jsonXContent, is, true);
        } finally {
            consumeEntity(shardStats1);
        }
        ArrayList<Object> values1 = new ArrayList<>(((Map<Object, Object>) shardStatsMap1.get("nodes")).values());
        assertThat(values1.size(), equalTo(2));
        XContentTestUtils.JsonMapView node1 = new XContentTestUtils.JsonMapView((Map<String, Object>) values1.get(0));
        ArrayList<Object> shard1IndexingPressureValues =
            new ArrayList<>(((Map<Object, Object>) node1.get("shard_indexing_pressure.stats")).values());
        assertThat(shard1IndexingPressureValues.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard1 =
            new XContentTestUtils.JsonMapView((Map<String, Object>) shard1IndexingPressureValues.get(0));
        Integer node1TotalLimitsRejections = node1.get("shard_indexing_pressure.total_rejections_breakup.node_limits");
        Integer shard1CoordinatingBytes = shard1.get("memory.total.coordinating_in_bytes");
        Integer shard1PrimaryBytes = shard1.get("memory.total.primary_in_bytes");
        Integer shard1ReplicaBytes = shard1.get("memory.total.replica_in_bytes");
        Integer shard1CoordinatingRejections = shard1.get("rejection.coordinating.coordinating_rejections");
        Integer shard1PrimaryRejections = shard1.get("rejection.primary.primary_rejections");
        Integer shard1CoordinatingNodeRejections = shard1.get("rejection.coordinating.breakup.node_limits");

        XContentTestUtils.JsonMapView node2 = new XContentTestUtils.JsonMapView((Map<String, Object>) values1.get(1));
        ArrayList<Object> shard2IndexingPressureValues =
            new ArrayList<>(((Map<Object, Object>) node2.get("shard_indexing_pressure.stats")).values());
        assertThat(shard2IndexingPressureValues.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard2 =
            new XContentTestUtils.JsonMapView((Map<String, Object>) shard2IndexingPressureValues.get(0));
        Integer node2TotalLimitsRejections = node2.get("shard_indexing_pressure.total_rejections_breakup.node_limits");
        Integer shard2CoordinatingBytes = shard2.get("memory.total.coordinating_in_bytes");
        Integer shard2PrimaryBytes = shard2.get("memory.total.primary_in_bytes");
        Integer shard2ReplicaBytes = shard2.get("memory.total.replica_in_bytes");
        Integer shard2CoordinatingRejections = shard2.get("rejection.coordinating.coordinating_rejections");
        Integer shard2PrimaryRejections = shard2.get("rejection.primary.primary_rejections");

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
        final Map<String, Object> nodeStatsMap;
        try (InputStream is = nodeStats.getEntity().getContent()) {
            nodeStatsMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, is, true);
        } finally {
            consumeEntity(nodeStats);
        }
        values1 = new ArrayList<>(((Map<Object, Object>) nodeStatsMap.get("nodes")).values());
        assertThat(values1.size(), equalTo(2));
        node1 = new XContentTestUtils.JsonMapView((Map<String, Object>) values1.get(0));
        Integer node1CombinedBytes = node1.get("indexing_pressure.memory.total.combined_coordinating_and_primary_in_bytes");
        Integer node1PrimaryBytes2 = node1.get("indexing_pressure.memory.total.primary_in_bytes");
        Integer node1ReplicaBytes2 = node1.get("indexing_pressure.memory.total.replica_in_bytes");
        Integer node1CoordinatingRejections = node1.get("indexing_pressure.memory.total.coordinating_rejections");
        Integer node1PrimaryRejections2 = node1.get("indexing_pressure.memory.total.primary_rejections");
        node2 = new XContentTestUtils.JsonMapView((Map<String, Object>) values1.get(1));
        Integer node2IndexingBytes = node2.get("indexing_pressure.memory.total.combined_coordinating_and_primary_in_bytes");
        Integer node2PrimaryBytes2 = node2.get("indexing_pressure.memory.total.primary_in_bytes");
        Integer node2ReplicaBytes2 = node2.get("indexing_pressure.memory.total.replica_in_bytes");
        Integer node2CoordinatingRejections2 = node2.get("indexing_pressure.memory.total.coordinating_rejections");
        Integer node2PrimaryRejections2 = node2.get("indexing_pressure.memory.total.primary_rejections");

        if (node1CombinedBytes == 0) {
            assertThat(node2IndexingBytes, greaterThan(0));
            assertThat(node2IndexingBytes, lessThan(1024));
        } else {
            assertThat(node1CombinedBytes, greaterThan(0));
            assertThat(node1CombinedBytes, lessThan(1024));
        }

        if (node1ReplicaBytes2 == 0) {
            assertThat(node1PrimaryBytes2, greaterThan(0));
            assertThat(node1PrimaryBytes2, lessThan(1024));

            assertThat(node2ReplicaBytes2, greaterThan(0));
            assertThat(node2ReplicaBytes2, lessThan(1024));
        } else {
            assertThat(node2PrimaryBytes2, greaterThan(0));
            assertThat(node2PrimaryBytes2, lessThan(1024));

            assertThat(node2ReplicaBytes2, equalTo(0));
            assertThat(node1ReplicaBytes2, lessThan(1024));
        }

        assertThat(node1CoordinatingRejections, equalTo(0));
        assertThat(node1PrimaryRejections2, equalTo(0));
        assertThat(node2CoordinatingRejections2, equalTo(0));
        assertThat(node2PrimaryRejections2, equalTo(0));

        Request failedIndexingRequest = new Request("POST", "/index_name/_doc/");
        String largeString = randomAlphaOfLength(10000);
        failedIndexingRequest.setJsonEntity("{\"x\": \"" + largeString + "\"}");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(failedIndexingRequest));
        try {
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(TOO_MANY_REQUESTS.getStatus()));
        } finally {
            consumeEntity(exception.getResponse());
        }

        Request getShardStats2 = new Request("GET", "/_nodes/stats/shard_indexing_pressure?include_all");
        final Response shardStats2 = getRestClient().performRequest(getShardStats2);
        final Map<String, Object> shardStatsMap2;
        try (InputStream is = shardStats2.getEntity().getContent()) {
            shardStatsMap2 = XContentHelper.convertToMap(JsonXContent.jsonXContent, is, true);
        } finally {
            consumeEntity(shardStats2);
        }
        ArrayList<Object> values2 = new ArrayList<>(((Map<Object, Object>) shardStatsMap2.get("nodes")).values());
        assertThat(values2.size(), equalTo(2));
        XContentTestUtils.JsonMapView node1AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>) values2.get(0));
        ArrayList<Object> shard1IndexingPressureValuesAfterRejection = new ArrayList<>(
            ((Map<Object, Object>) node1AfterRejection.get("shard_indexing_pressure.stats")).values()
        );
        assertThat(shard1IndexingPressureValuesAfterRejection.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard1AfterRejection =
            new XContentTestUtils.JsonMapView((Map<String, Object>) shard1IndexingPressureValuesAfterRejection.get(0));
        node1TotalLimitsRejections = node1AfterRejection.get("shard_indexing_pressure.total_rejections_breakup.node_limits");
        shard1CoordinatingRejections = shard1AfterRejection.get("rejection.coordinating.coordinating_rejections");
        shard1PrimaryRejections = shard1AfterRejection.get("rejection.primary.primary_rejections");
        shard1CoordinatingNodeRejections = shard1AfterRejection.get("rejection.coordinating.breakup.node_limits");

        XContentTestUtils.JsonMapView node2AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>) values2.get(1));
        ArrayList<Object> shard2IndexingPressureValuesAfterRejection = new ArrayList<>(
            ((Map<Object, Object>) node2AfterRejection.get("shard_indexing_pressure.stats")).values()
        );
        assertThat(shard2IndexingPressureValuesAfterRejection.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard2AfterRejection =
            new XContentTestUtils.JsonMapView((Map<String, Object>) shard2IndexingPressureValuesAfterRejection.get(0));
        node2TotalLimitsRejections = node2AfterRejection.get("shard_indexing_pressure.total_rejections_breakup.node_limits");
        Integer shard2CoordinatingRejectionsAfterRej = shard2AfterRejection.get("rejection.coordinating.coordinating_rejections");
        Integer shard2PrimaryRejectionsAfterRej = shard2AfterRejection.get("rejection.primary.primary_rejections");
        Integer shard2CoordinatingNodeRejectionsAfterRej = shard2AfterRejection.get("rejection.coordinating.breakup.node_limits");

        if (shard1CoordinatingRejections == 0) {
            assertThat(shard2CoordinatingRejectionsAfterRej, equalTo(1));
            assertThat(shard2CoordinatingNodeRejectionsAfterRej, equalTo(1));
            assertThat(node2TotalLimitsRejections, equalTo(1));
        } else {
            assertThat(shard1CoordinatingRejections, equalTo(1));
            assertThat(shard1CoordinatingNodeRejections, equalTo(1));
            assertThat(node1TotalLimitsRejections, equalTo(1));
        }

        assertThat(shard1PrimaryRejections, equalTo(0));
        assertThat(shard2PrimaryRejectionsAfterRej, equalTo(0));

        Request getNodeStats2 = new Request("GET", "/_nodes/stats/indexing_pressure");
        final Response nodeStats2 = getRestClient().performRequest(getNodeStats2);
        final Map<String, Object> nodeStatsMap2;
        try (InputStream is = nodeStats2.getEntity().getContent()) {
            nodeStatsMap2 = XContentHelper.convertToMap(JsonXContent.jsonXContent, is, true);
        } finally {
            consumeEntity(nodeStats2);
        }
        values2 = new ArrayList<>(((Map<Object, Object>) nodeStatsMap2.get("nodes")).values());
        assertThat(values2.size(), equalTo(2));
        node1AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>) values2.get(0));
        node1CoordinatingRejections = node1AfterRejection.get("indexing_pressure.memory.total.coordinating_rejections");
        Integer node1PrimaryRejectionsAfter = node1AfterRejection.get("indexing_pressure.memory.total.primary_rejections");
        node2AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>) values2.get(1));
        Integer node2CoordinatingRejectionsAfter = node2AfterRejection.get("indexing_pressure.memory.total.coordinating_rejections");
        Integer node2PrimaryRejectionsAfter = node2AfterRejection.get("indexing_pressure.memory.total.primary_rejections");

        if (node1CoordinatingRejections == 0) {
            assertThat(node2CoordinatingRejectionsAfter, equalTo(1));
        } else {
            assertThat(node1CoordinatingRejections, equalTo(1));
        }

        assertThat(node1PrimaryRejectionsAfter, equalTo(0));
        assertThat(node2PrimaryRejectionsAfter, equalTo(0));

        // Update cluster setting to enable shadow mode
        Request updateSettingRequest = new Request("PUT", "/_cluster/settings");
        updateSettingRequest.setJsonEntity("{\"persistent\": {\"shard_indexing_pressure\": {\"enforced\": \"false\"}}}");
        final Response updateSettingResponse = getRestClient().performRequest(updateSettingRequest);
        try {
            assertThat(updateSettingResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));
        } finally {
            consumeEntity(updateSettingResponse);
        }

        Request shadowIndexingRequest = new Request("POST", "/index_name/_doc/");
        shadowIndexingRequest.setJsonEntity("{\"x\": \"text\"}");
        final Response shadowIndexingResponse = getRestClient().performRequest(shadowIndexingRequest);
        try {
            assertThat(shadowIndexingResponse.getStatusLine().getStatusCode(), equalTo(CREATED.getStatus()));
        } finally {
            consumeEntity(shadowIndexingResponse);
        }

        Request getShardStats3 = new Request("GET", "/_nodes/stats/shard_indexing_pressure?include_all");
        final Response shardStats3 = getRestClient().performRequest(getShardStats3);
        final Map<String, Object> shardStatsMap3;
        try (InputStream is = shardStats3.getEntity().getContent()) {
            shardStatsMap3 = XContentHelper.convertToMap(JsonXContent.jsonXContent, is, true);
        } finally {
            consumeEntity(shardStats3);
        }
        ArrayList<Object> values3 = new ArrayList<>(((Map<Object, Object>) shardStatsMap3.get("nodes")).values());
        assertThat(values3.size(), equalTo(2));
        XContentTestUtils.JsonMapView node1AfterShadowMode = new XContentTestUtils.JsonMapView((Map<String, Object>) values3.get(0));
        ArrayList<Object> shard1IndexingPressureValuesAfterShadowMode = new ArrayList<>(
            ((Map<Object, Object>) node1AfterShadowMode.get("shard_indexing_pressure.stats")).values()
        );
        assertThat(shard1IndexingPressureValuesAfterShadowMode.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard1AfterShadowMode =
            new XContentTestUtils.JsonMapView((Map<String, Object>) shard1IndexingPressureValuesAfterShadowMode.get(0));
        Integer node1TotalLimitsRejectionsShadow = node1AfterShadowMode.get(
            "shard_indexing_pressure.total_rejections_breakup_shadow_mode.node_limits"
        );
        Integer shard1CoordinatingRejectionsShadow = shard1AfterShadowMode.get("rejection.coordinating.coordinating_rejections");
        Integer shard1PrimaryRejectionsShadow = shard1AfterShadowMode.get("rejection.primary.primary_rejections");
        Integer shard1CoordinatingNodeRejectionsShadow = shard1AfterShadowMode.get(
            "rejection.coordinating.breakup_shadow_mode.node_limits"
        );

        XContentTestUtils.JsonMapView node2AfterShadowMode = new XContentTestUtils.JsonMapView((Map<String, Object>) values3.get(1));
        ArrayList<Object> shard2IndexingPressureValuesAfterShadowMode = new ArrayList<>(
            ((Map<Object, Object>) node2AfterShadowMode.get("shard_indexing_pressure.stats")).values()
        );
        assertThat(shard2IndexingPressureValuesAfterShadowMode.size(), equalTo(1));
        XContentTestUtils.JsonMapView shard2AfterShadowMode =
            new XContentTestUtils.JsonMapView((Map<String, Object>) shard2IndexingPressureValuesAfterShadowMode.get(0));
        Integer node2TotalLimitsRejectionsShadow = node2AfterShadowMode.get(
            "shard_indexing_pressure.total_rejections_breakup_shadow_mode.node_limits"
        );
        Integer shard2CoordinatingRejectionsShadow = shard2AfterShadowMode.get("rejection.coordinating.coordinating_rejections");
        Integer shard2PrimaryRejectionsShadow = shard2AfterShadowMode.get("rejection.primary.primary_rejections");
        Integer shard2CoordinatingNodeRejectionsShadow = shard2AfterShadowMode.get(
            "rejection.coordinating.breakup_shadow_mode.node_limits"
        );

        if (shard1CoordinatingRejectionsShadow == 0) {
            assertThat(shard2CoordinatingRejectionsShadow, equalTo(1));
            assertThat(shard2CoordinatingNodeRejectionsShadow, equalTo(1));
            assertThat(node2TotalLimitsRejectionsShadow, equalTo(1));
        } else {
            assertThat(shard1CoordinatingRejectionsShadow, equalTo(1));
            assertThat(shard1CoordinatingNodeRejectionsShadow, equalTo(1));
            assertThat(node1TotalLimitsRejectionsShadow, equalTo(1));
        }

        assertThat(shard1PrimaryRejectionsShadow, equalTo(0));
        assertThat(shard2PrimaryRejectionsShadow, equalTo(0));

        //Reset persistent setting to clear cluster metadata
        updateSettingRequest = new Request("PUT", "/_cluster/settings");
        updateSettingRequest.setJsonEntity("{\"persistent\": {\"shard_indexing_pressure\": {\"enforced\": null}}}");
        final Response resetResponse = getRestClient().performRequest(updateSettingRequest);
        consumeEntity(resetResponse);
    }
}

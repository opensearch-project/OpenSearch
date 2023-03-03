/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Node;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.XContentTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.rest.RestStatus.CREATED;
import static org.opensearch.rest.RestStatus.OK;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 2,
    numClientNodes = 0)
public class SegmentReplicationRestIT extends HttpSmokeTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REPLICATION_TYPE, "true").build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public void testSegmentReplicationStats() throws Exception {
        // create index.
        Request createRequest = new Request("PUT", "/test_index");
        createRequest.setJsonEntity("{\"settings\": {\"index\": {\"number_of_shards\": 1, \"number_of_replicas\": 1, " +
            "\"replication\": {\"type\": \"SEGMENT\"}}}}");
        final Response indexCreatedResponse = getRestClient().performRequest(createRequest);
        assertEquals(indexCreatedResponse.getStatusLine().getStatusCode(), OK.getStatus());
        ensureGreen("test_index");

        //index a doc
        Request successfulIndexingRequest = new Request("POST", "/test_index/_doc/");
        successfulIndexingRequest.setJsonEntity("{\"foo\": \"bar\"}");
        final Response indexSuccessFul = getRestClient().performRequest(successfulIndexingRequest);
        assertEquals(indexSuccessFul.getStatusLine().getStatusCode(), CREATED.getStatus());

        assertBusy(() -> {
            // wait for SR to run.
            for (Node node : getRestClient().getNodes()) {
                assertHitCount(client(node.getName()).prepareSearch("test_index").setSize(0).setPreference("_only_local").get(), 1);
            }
        });
        Request statsRequest = new Request("GET", "/_nodes/stats/segment_replication_stats?pretty");
        final Response response = getRestClient().performRequest(statsRequest);
        logger.info("Node stats response\n{}", EntityUtils.toString(response.getEntity()));
        Map<String, Object> statsMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(),
            true);
        List<Object> nodes = new ArrayList<>(((Map<Object, Object>) statsMap.get("nodes")).values());
        assertEquals(2, nodes.size());
        XContentTestUtils.JsonMapView node1 = new XContentTestUtils.JsonMapView((Map<String, Object>) nodes.get(0));
        final Map<Object, Object> node1_map = node1.get("segment_replication");
        Map<Object, Object> primaryNode_map = node1_map;
        if (node1_map.isEmpty()) {
            XContentTestUtils.JsonMapView node2 = new XContentTestUtils.JsonMapView((Map<String, Object>) nodes.get(1));
            primaryNode_map = node2.get("segment_replication");
        }
        List<Object> primary_values = new ArrayList<>(primaryNode_map
            .values());
        assertEquals(1, primary_values.size());
        XContentTestUtils.JsonMapView shard1 = new XContentTestUtils.JsonMapView((Map<String, Object>) primary_values.get(0));
        Integer node1TotalLimitsRejections = shard1.get("rejected_requests");
        assertNotNull(node1TotalLimitsRejections);
        List<Object> shard1_replicas = shard1.get("replicas");
        assertEquals(1, shard1_replicas.size());
        XContentTestUtils.JsonMapView replica = new XContentTestUtils.JsonMapView((Map<String, Object>) shard1_replicas.get(0));
        Integer checkpoints_behind = replica.get("checkpoints_behind");
        assertEquals(0, checkpoints_behind.intValue());
        assertNotNull(replica.get("node_id"));
        assertNotNull(replica.get("current_replication_time"));
        assertNotNull(replica.get("last_completed_replication_time"));
        assertNotNull(replica.get("bytes_behind"));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrades;

import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClusterStatsIT extends AbstractRollingTestCase {

    private final List<String> nodeStatsMetrics = List.of("os", "process", "jvm", "fs", "plugins", "ingest", "network_types", "discovery_types", "packaging_types");

    private final List<String> indicesStatsMetrics = List.of("shards", "docs", "store", "fielddata", "query_cache", "completion", "segments", "analysis", "mappings");

    public void testClusterStats() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_cluster/stats"));
        validateClusterStatsWithFilterResponse(response, nodeStatsMetrics, indicesStatsMetrics);
        if (AbstractRollingTestCase.UPGRADE_FROM_VERSION.onOrAfter(Version.V_2_18_0) || (
            CLUSTER_TYPE == ClusterType.UPGRADED && Version.CURRENT.onOrAfter(Version.V_2_18_0))) {
            response = client().performRequest(new Request("GET", "/_cluster/stats/os/nodes/_all"));
            validateClusterStatsWithFilterResponse(response, List.of("os"), Collections.emptyList());
            response = client().performRequest(new Request("GET", "/_cluster/stats/indices/mappings/nodes/_all"));
            validateClusterStatsWithFilterResponse(response, Collections.emptyList(), List.of("mappings"));
            response = client().performRequest(new Request("GET", "/_cluster/stats/os,indices/mappings/nodes/_all"));
            validateClusterStatsWithFilterResponse(response, List.of("os"), List.of("mappings"));
        }
    }

    private void validateClusterStatsWithFilterResponse(Response response, List<String> requestedNodesStatsMetrics, List<String> requestedIndicesStatsMetrics) throws IOException {
        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> entity = entityAsMap(response);
        if (requestedNodesStatsMetrics != null && !requestedNodesStatsMetrics.isEmpty()) {
            assertTrue(entity.containsKey("nodes"));
            Map<?, ?> nodesStats = (Map<?, ?>) entity.get("nodes");
            for (String metric : nodeStatsMetrics) {
                if (requestedNodesStatsMetrics.contains(metric)) {
                    assertTrue(nodesStats.containsKey(metric));
                } else {
                    assertFalse(nodesStats.containsKey(metric));
                }
            }
        }

        if (requestedIndicesStatsMetrics != null && !requestedIndicesStatsMetrics.isEmpty()) {
            assertTrue(entity.containsKey("indices"));
            Map<?, ?> indicesStats = (Map<?, ?>) entity.get("indices");
            for (String metric : indicesStatsMetrics) {
                if (requestedIndicesStatsMetrics.contains(metric)) {
                    assertTrue(indicesStats.containsKey(metric));
                } else {
                    assertFalse(indicesStats.containsKey(metric));
                }
            }
        }
    }
}

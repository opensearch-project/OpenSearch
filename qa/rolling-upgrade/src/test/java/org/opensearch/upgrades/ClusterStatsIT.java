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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClusterStatsIT extends AbstractRollingTestCase {

    public void testClusterStats() throws IOException {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            final String indexName = "test-index";
            final int shardCount = 3;
            final int replicaCount = 1;
            Settings settings = Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), shardCount)
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), replicaCount)
                .build();
            createIndex(indexName, settings);
            Request waitForStatus = new Request("GET", "/_cluster/health/" + indexName);
            waitForStatus.addParameter("wait_for_status", "green");
        }

        Response response = client().performRequest(new Request("GET", "/_cluster/stats"));
        validateClusterStatsWithFilterResponse(response, List.of("os", "process", "jvm", "fs", "plugins", "ingest", "network_types", "discovery_types", "packaging_types"),
            List.of("shards", "docs", "store", "fielddata", "query_cache", "completion", "segments", "analysis", "mappings"));
        if (AbstractRollingTestCase.UPGRADE_FROM_VERSION.onOrAfter(Version.V_3_0_0) || (
            CLUSTER_TYPE == ClusterType.UPGRADED && Version.CURRENT.onOrAfter(Version.V_3_0_0))) {
            response = client().performRequest(new Request("GET", "/_cluster/stats/os/nodes/_all"));
            validateClusterStatsWithFilterResponse(response, List.of("os"), Collections.emptyList());
            response = client().performRequest(new Request("GET", "/_cluster/stats/indices/nodes/_all"));
            validateClusterStatsWithFilterResponse(response, Collections.emptyList(), List.of("shards"));
            response = client().performRequest(new Request("GET", "/_cluster/stats/os,indices/nodes/_all"));
            validateClusterStatsWithFilterResponse(response, List.of("os"), List.of("shards"));
        }
    }

    private void validateClusterStatsWithFilterResponse(Response response, List<String> nodesStatsMetrics, List<String> indicesStatsMetrics) throws IOException {
        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> entity = entityAsMap(response);
        if (nodesStatsMetrics != null && !nodesStatsMetrics.isEmpty()) {
            assertTrue(entity.containsKey("nodes"));
            Map<?, ?> nodesStats = ( Map<?, ?>) entity.get("nodes");
            for (String metric : nodesStatsMetrics) {
                assertTrue(nodesStats.containsKey(metric));
            }
        }
        if (indicesStatsMetrics != null && !indicesStatsMetrics.isEmpty()) {
            assertTrue(entity.containsKey("indices"));
            Map<?, ?> indicesStats = ( Map<?, ?>) entity.get("indices");
            for (String metric : indicesStatsMetrics) {
                assertTrue(indicesStats.containsKey(metric));
                if (metric.equals("shards")) {
                    Map<?, ?> shards = (Map<?, ?>) indicesStats.get("shards");
                    assertTrue(shards.containsKey("total"));
                    assertEquals(6, (int) shards.get("total"));
                    assertTrue(shards.containsKey("primaries"));
                    assertEquals(3, (int) shards.get("primaries"));
                }
            }
        }
    }
}

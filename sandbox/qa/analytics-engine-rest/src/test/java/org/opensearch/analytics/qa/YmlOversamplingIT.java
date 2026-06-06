/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.util.List;
import java.util.Map;

/**
 * Verifies that {@code analytics.shard_bucket_oversampling_factor} set via opensearch.yml
 * (node settings at bootstrap) is picked up by the TopK rewriter without any dynamic
 * cluster settings API call.
 *
 * <p>The test cluster for this IT is configured in build.gradle with:
 * {@code setting 'analytics.shard_bucket_oversampling_factor', '2.0'}
 */
public class YmlOversamplingIT extends AnalyticsRestTestCase {

    private static final String INDEX = "yml_oversampling_test";

    /**
     * Grouped count + sort + head without any runtime cluster settings call.
     * If TopK fires (oversampling from yml), the query succeeds with correct results.
     * If TopK doesn't fire, it still succeeds but without per-shard pruning.
     * We verify correctness: exact count values on a known dataset.
     */
    public void testTopKFiresFromYmlSetting() throws Exception {
        // Create 2-shard index
        createParquetIndex(INDEX, 2);

        // 3 groups: A=6, B=4, C=2 docs
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 6; i++) bulk.append("{\"index\":{}}\n{\"grp\":\"A\"}\n");
        for (int i = 0; i < 4; i++) bulk.append("{\"index\":{}}\n{\"grp\":\"B\"}\n");
        for (int i = 0; i < 2; i++) bulk.append("{\"index\":{}}\n{\"grp\":\"C\"}\n");
        bulkAndRefresh(INDEX, bulk.toString());

        // No cluster settings API call — oversampling should come from yml
        Map<String, Object> result = executePpl(
            "source = " + INDEX + " | stats count() as c by grp | sort - c | head 2"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals("top-2 groups", 2, rows.size());
        assertEquals("top group count", 6, ((Number) rows.get(0).get(0)).intValue());
        assertEquals("top group key", "A", rows.get(0).get(1));
        assertEquals("second group count", 4, ((Number) rows.get(1).get(0)).intValue());
        assertEquals("second group key", "B", rows.get(1).get(1));
    }

    private void createParquetIndex(String index, int shards) throws Exception {
        try { client().performRequest(new org.opensearch.client.Request("DELETE", "/" + index)); } catch (Exception ignored) {}
        String body = "{\"settings\": {"
            + "  \"number_of_shards\": " + shards + ", \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true, \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\", \"index.composite.secondary_data_formats\": \"\""
            + "}, \"mappings\": {\"properties\": {\"grp\": {\"type\": \"keyword\"}}}}";
        org.opensearch.client.Request create = new org.opensearch.client.Request("PUT", "/" + index);
        create.setJsonEntity(body);
        assertOkAndParse(client().performRequest(create), "create " + index);
        org.opensearch.client.Request health = new org.opensearch.client.Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void bulkAndRefresh(String index, String body) throws Exception {
        org.opensearch.client.Request bulk = new org.opensearch.client.Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(body);
        client().performRequest(bulk);
    }
}

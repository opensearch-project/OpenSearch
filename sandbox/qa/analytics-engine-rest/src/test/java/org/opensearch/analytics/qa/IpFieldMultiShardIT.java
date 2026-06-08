/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Multi-shard {@code ip}-field coverage (Mustang bug tracker Bug 3): a query over an index with an
 * {@code ip}-type field can fail with a Substrait {@code Binary} vs {@code BinaryView} schema mismatch.
 * {@link FieldTypeCoverageIT#testIp()} covers {@code ip} at 1 shard; this exercises the 2-shard reduce
 * path. Run unmuted — if the defect is present it fails here, otherwise it passes.
 */
public class IpFieldMultiShardIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("ip_multishard", "ip_multishard");

    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned == false) {
            DatasetProvisioner.provision(client(), DATASET, 2);
            provisioned = true;
        }
    }

    /** count() by status_code over the 10 docs (status_code = 200 + id%3): 200->4, 201->3, 202->3. */
    @SuppressWarnings("unchecked")
    public void testAggregationOnIndexWithIpFieldAtTwoShards() throws IOException {
        Map<String, Object> resp = executePpl("source=" + DATASET.indexName + " | stats count() by status_code");
        List<List<Object>> rows = (List<List<Object>>) resp.get("datarows");
        assertNotNull("expected datarows for count-by-status over an ip-bearing index", rows);

        Map<Integer, Integer> counts = new HashMap<>();
        for (List<Object> row : rows) {
            counts.put(((Number) row.get(1)).intValue(), ((Number) row.get(0)).intValue());
        }
        Map<Integer, Integer> expected = Map.of(200, 4, 201, 3, 202, 3);
        assertEquals("count() by status_code", expected, counts);
    }
}

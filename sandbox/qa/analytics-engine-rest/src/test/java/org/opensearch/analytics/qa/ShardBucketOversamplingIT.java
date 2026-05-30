/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.List;
import java.util.Map;

/**
 * IT for per-shard bucket oversampling using ClickBench dataset.
 * Exercises real TopK query shapes (stats+sort+head) on multi-shard index.
 */
public class ShardBucketOversamplingIT extends AnalyticsRestTestCase {

    private static volatile boolean provisioned = false;
    private static final String INDEX = "parquet_hits";

    private void ensureProvisioned() throws Exception {
        if (!provisioned) {
            DatasetProvisioner.provision(client(), ClickBenchTestHelper.DATASET, 2);
            Request req = new Request("PUT", "/_cluster/settings");
            req.setJsonEntity("{\"persistent\":{\"analytics.shard_bucket_oversampling_factor\": 2.0}}");
            client().performRequest(req);
            provisioned = true;
        }
    }

    /** Q13 shape: count by keyword, sort desc, head 10. */
    public void testCountByGroup_sortDesc_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats count() as c by RegionID | sort - c | head 10"
        );
        assertRowCount(result, 10);
    }

    /** Q10 shape: sum + count + avg + dc by group, sort, head. */
    public void testMultiAgg_sortByCount_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats sum(AdvEngineID), count() as c, avg(ResolutionWidth) by RegionID | sort - c | head 10"
        );
        assertRowCount(result, 10);
    }

    /** Sum by group, sort desc. */
    public void testSumByGroup_sortDesc_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats sum(ResolutionWidth) as s by RegionID | sort - s | head 10"
        );
        assertRowCount(result, 10);
    }

    /** Avg by group, sort desc. */
    public void testAvgByGroup_sortDesc_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats avg(ResolutionWidth) as a by RegionID | sort - a | head 10"
        );
        assertRowCount(result, 10);
    }

    /** Min/Max by group. */
    public void testMinMaxByGroup() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats min(ResolutionWidth) as mi, max(ResolutionWidth) as ma by RegionID | sort - ma | head 10"
        );
        assertRowCount(result, 10);
    }

    /** dc by group, sort desc. */
    public void testDcByGroup_sortDesc_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats dc(UserID) as u by RegionID | sort - u | head 10"
        );
        assertRowCount(result, 10);
    }

    /** No oversampling (factor=0): query still works. */
    public void testFactorZero_queryWorks() throws Exception {
        ensureProvisioned();
        // Temporarily set factor to 0
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.shard_bucket_oversampling_factor\": 0.0}}");
        client().performRequest(req);

        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats count() as c by RegionID | sort - c | head 10"
        );
        assertRowCount(result, 10);

        // Restore
        Request restore = new Request("PUT", "/_cluster/settings");
        restore.setJsonEntity("{\"persistent\":{\"analytics.shard_bucket_oversampling_factor\": 2.0}}");
        client().performRequest(restore);
    }

    @SuppressWarnings("unchecked")
    private void assertRowCount(Map<String, Object> result, int expected) {
        List<?> rows = (List<?>) result.get("rows");
        assertNotNull("response must have rows, got: " + result.keySet(), rows);
        assertEquals(expected, rows.size());
    }

    private Map<String, Object> executePPL(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + ppl + "\"}");
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }
}

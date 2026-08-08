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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.Map;

/**
 * Integration test for the {@code analytics.query.max_plan_depth} dynamic cluster setting.
 * Verifies that queries succeeding at the default depth are rejected when the limit is lowered.
 */
public class MaxPlanDepthSettingIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testQuerySucceedsAtDefaultDepthThenRejectedWhenLowered() throws Exception {
        ensureDataProvisioned();
        // A simple aggregation query produces a plan with depth > 1
        String query = "source=" + DATASET.indexName + " | stats avg(num0) by str0";

        // Should succeed at the default depth (15)
        Request ok = new Request("POST", "/_analytics/ppl");
        ok.setJsonEntity("{\"query\": \"" + query + "\"}");
        Response response = client().performRequest(ok);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Lower the max plan depth to 1 — any non-trivial query will be rejected
        setMaxPlanDepth(1);
        try {
            Request rejected = new Request("POST", "/_analytics/ppl");
            rejected.setJsonEntity("{\"query\": \"" + query + "\"}");
            try {
                client().performRequest(rejected);
                fail("Expected 400 for query exceeding max_plan_depth=1");
            } catch (ResponseException e) {
                assertEquals("should be 400", 400, e.getResponse().getStatusLine().getStatusCode());
                String body = new String(e.getResponse().getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                assertTrue("error mentions depth: " + body, body.contains("maximum depth"));
            }
        } finally {
            // Restore default so other tests are not affected
            resetMaxPlanDepth();
        }
    }

    private void setMaxPlanDepth(int depth) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"transient\": {\"analytics.query.max_plan_depth\": " + depth + "}}");
        client().performRequest(req);
    }

    private void resetMaxPlanDepth() throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"transient\": {\"analytics.query.max_plan_depth\": null}}");
        client().performRequest(req);
    }
}

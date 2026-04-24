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

import java.util.Map;

/**
 * ClickBench integration test for PPL queries through the DataFusion backend.
 * <p>
 * Query path: {@code POST /_analytics/ppl} → test-ppl-frontend → analytics-engine → Calcite → Substrait → DataFusion
 * <p>
 * ClickBench data is provisioned into a parquet-format index via {@link ClickBenchTestFixture}.
 * Requires the test-ppl-frontend plugin to be installed.
 */
public class PplClickBenchIT extends DataFusionRestTestCase {

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws Exception {
        if (dataProvisioned == false) {
            ClickBenchTestFixture.provisionIndex(client());
            dataProvisioned = true;
        }
    }

    /**
     * Verify that a simple PPL query against the parquet-backed ClickBench index
     * returns a valid response via the unified PPL path.
     */
    public void testSimplePplQuery() throws Exception {
        ensureDataProvisioned();

        String pplQuery = "source = " + ClickBenchTestFixture.INDEX_NAME;
        logger.info("=== PPL simple query: {} ===", pplQuery);

        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(pplQuery) + "\"}");
        Response response = client().performRequest(request);

        Map<String, Object> responseMap = assertOkAndParse(response, "PPL simple query");
        logger.info("PPL simple query response: {}", responseMap);

        assertFalse("PPL response should not be empty", responseMap.isEmpty());
    }

    // TODO: Add ClickBench PPL aggregation queries (Q1-Q43) once the scheduler
    // transport thread assertion is resolved in the analytics engine.
}

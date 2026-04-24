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
 * ClickBench integration test for DSL queries through the DataFusion backend.
 * <p>
 * Query path: {@code POST /{index}/_search} → dsl-query-executor → Calcite → Substrait → DataFusion
 * <p>
 * ClickBench data is provisioned into a parquet-format index via {@link ClickBenchTestFixture}.
 * To add more queries, add test methods that load from {@code clickbench/dsl/q{N}.json}.
 */
public class DslClickBenchIT extends DataFusionRestTestCase {

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws Exception {
        if (dataProvisioned == false) {
            ClickBenchTestFixture.provisionIndex(client());
            dataProvisioned = true;
        }
    }

    /**
     * Verify that a simple search against the parquet-backed ClickBench index
     * returns a valid response via the DSL query path.
     */
    public void testSimpleSearch() throws Exception {
        ensureDataProvisioned();

        Request request = new Request("POST", "/" + ClickBenchTestFixture.INDEX_NAME + "/_search");
        request.setJsonEntity("{\"size\": 0, \"track_total_hits\": true}");
        Response response = client().performRequest(request);

        Map<String, Object> responseMap = assertOkAndParse(response, "DSL simple search");
        logger.info("DSL simple search response: {}", responseMap);

        @SuppressWarnings("unchecked")
        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        assertNotNull("Response should contain 'hits'", hits);
    }

    // TODO: Add ClickBench aggregation queries (Q1-Q43) once the scheduler
    // transport thread assertion is resolved in the analytics engine.
}

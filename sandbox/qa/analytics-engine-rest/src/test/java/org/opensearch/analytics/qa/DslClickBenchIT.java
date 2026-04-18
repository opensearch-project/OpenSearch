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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ClickBench integration test for DSL queries through the DataFusion backend.
 * <p>
 * Query path: {@code POST /{index}/_search} → dsl-query-executor → Calcite → Substrait → DataFusion
 * <p>
 * To add more queries, add their numbers to {@link #QUERY_NUMBERS}. Each query
 * is loaded from {@code clickbench/dsl/q{N}.json} and executed against the
 * ClickBench dataset.
 */
public class DslClickBenchIT extends DataFusionRestTestCase {

    /**
     * Query numbers to execute. Add entries here to run additional ClickBench queries.
     * Each number N maps to the resource file {@code clickbench/dsl/q{N}.json}.
     */
    private static final Set<Integer> QUERY_NUMBERS = Set.of(1);

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws Exception {
        if (dataProvisioned == false) {
            ClickBenchTestFixture.provisionIndex(client());
            dataProvisioned = true;
        }
    }

    public void testClickBenchDslQueries() throws Exception {
        ensureDataProvisioned();

        List<String> failures = new ArrayList<>();

        for (int queryNum : QUERY_NUMBERS) {
            String queryId = "Q" + queryNum;
            try {
                String dslBody = ClickBenchTestFixture.loadDslQuery(queryNum);
                logger.info("=== DSL {}: ===\n{}", queryId, dslBody);

                Request request = new Request("POST", "/" + ClickBenchTestFixture.INDEX_NAME + "/_search");
                request.setJsonEntity(dslBody);
                Response response = client().performRequest(request);

                Map<String, Object> responseMap = assertOkAndParse(response, "DSL " + queryId);
                logger.info("DSL {} response: {}", queryId, responseMap);

                // Verify response structure: hits.total.value
                @SuppressWarnings("unchecked")
                Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
                assertNotNull("DSL " + queryId + ": response should contain 'hits'", hits);

                @SuppressWarnings("unchecked")
                Map<String, Object> total = (Map<String, Object>) hits.get("total");
                assertNotNull("DSL " + queryId + ": response should contain 'hits.total'", total);

                int totalHits = ((Number) total.get("value")).intValue();
                logger.info("DSL {} result: total hits = {}", queryId, totalHits);
                assertEquals(
                    "DSL " + queryId + ": expected " + ClickBenchTestFixture.EXPECTED_DOC_COUNT + " documents",
                    ClickBenchTestFixture.EXPECTED_DOC_COUNT,
                    totalHits
                );
            } catch (Exception e) {
                String msg = "DSL " + queryId + " failed: " + e.getMessage();
                logger.error(msg, e);
                failures.add(msg);
            }
        }

        if (failures.isEmpty() == false) {
            fail("DSL query failures:\n" + String.join("\n", failures));
        }
    }
}

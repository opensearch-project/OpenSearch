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
 * ClickBench integration test for PPL queries through the DataFusion backend.
 * <p>
 * Query path: {@code POST /_plugins/_ppl} → SQL plugin → analytics-engine → Calcite → Substrait → DataFusion
 * <p>
 * To add more queries, add their numbers to {@link #QUERY_NUMBERS}. Each query
 * is loaded from {@code clickbench/ppl/q{N}.ppl} and executed against the
 * ClickBench dataset.
 * <p>
 * Requires the SQL plugin (opensearch-sql-plugin) to be installed.
 */
public class PplClickBenchIT extends DataFusionRestTestCase {

    /**
     * Query numbers to execute. Add entries here to run additional ClickBench queries.
     * Each number N maps to the resource file {@code clickbench/ppl/q{N}.ppl}.
     */
    private static final Set<Integer> QUERY_NUMBERS = Set.of(1);

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws Exception {
        if (dataProvisioned == false) {
            ClickBenchTestFixture.provisionIndex(client());
            dataProvisioned = true;
        }
    }

    public void testClickBenchPplQueries() throws Exception {
        ensureDataProvisioned();

        List<String> failures = new ArrayList<>();

        for (int queryNum : QUERY_NUMBERS) {
            String queryId = "Q" + queryNum;
            try {
                String pplQuery = ClickBenchTestFixture.loadPplQuery(queryNum);
                pplQuery = pplQuery.replace("clickbench", ClickBenchTestFixture.INDEX_NAME);
                logger.info("=== PPL {}: ===\n{}", queryId, pplQuery);

                Request request = new Request("POST", "/_plugins/_ppl");
                request.setJsonEntity("{\"query\": \"" + escapeJson(pplQuery) + "\"}");
                Response response = client().performRequest(request);

                Map<String, Object> responseMap = assertOkAndParse(response, "PPL " + queryId);
                logger.info("PPL {} response: {}", queryId, responseMap);

                assertFalse("PPL " + queryId + ": response should not be empty", responseMap.isEmpty());
            } catch (Exception e) {
                String msg = "PPL " + queryId + " failed: " + e.getMessage();
                logger.error(msg, e);
                failures.add(msg);
            }
        }

        if (failures.isEmpty() == false) {
            fail("PPL query failures:\n" + String.join("\n", failures));
        }
    }
}

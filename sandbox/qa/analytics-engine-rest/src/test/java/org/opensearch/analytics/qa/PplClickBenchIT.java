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
import java.util.Set;

/**
 * ClickBench PPL integration test. Runs PPL queries against a parquet-backed ClickBench index.
 * <p>
 * Query path: {@code POST /_analytics/ppl} → test-ppl-frontend → analytics-engine → Calcite → Substrait → DataFusion
 * <p>
 * Currently restricted to Q1 to keep CI green. Auto-discovery of all 43 ClickBench queries is
 * temporarily disabled because several queries exercise unsupported translators/planner rules
 * and the broader DSL run destabilizes the shared test cluster. Re-enable auto-discovery once
 * the analytics-engine adds support for those paths.
 */
public class PplClickBenchIT extends AnalyticsRestTestCase {

    /**
     * ClickBench PPL query numbers to run. Auto-discovery finds all q{N}.ppl files under
     * resources/datasets/clickbench/ppl/. Individual queries can be excluded via
     * {@link #SKIP_QUERIES} when a feature is genuinely missing rather than broken.
     */
    // Queries skipped:
    //  - Q29: Substrait's default aggregate catalog has no min/max binding for string
    //    types. Isthmus fails with "Unable to find binding for call MIN($1)" when PPL
    //    emits `min(Referer)` on a VARCHAR column. DataFusion can execute min(Utf8)
    //    natively — the gap is purely in the Substrait serialization layer. A fix
    //    would register additional min/max impls covering string types in the loaded
    //    SimpleExtension.ExtensionCollection at plugin init.
    private static final Set<Integer> SKIP_QUERIES = Set.of(29);

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws Exception {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), ClickBenchTestHelper.DATASET);
            dataProvisioned = true;
        }
    }

    public void testClickBenchPplQueries() throws Exception {
        ensureDataProvisioned();

        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(ClickBenchTestHelper.DATASET, "ppl")
            .stream()
            .filter(n -> SKIP_QUERIES.contains(n) == false)
            .toList();
        assertFalse("No PPL queries discovered", queryNumbers.isEmpty());
        logger.info("Running {} PPL queries (of {} discovered): {}", queryNumbers.size(), queryNumbers.size(), queryNumbers);

        List<String> failures = DatasetQueryRunner.runQueries(
            client(),
            ClickBenchTestHelper.DATASET,
            "ppl",
            "ppl",
            queryNumbers,
            (client, dataset, queryBody) -> {
                String ppl = queryBody.trim().replace("clickbench", dataset.indexName);
                Request request = new Request("POST", "/_analytics/ppl");
                request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
                Response response = client.performRequest(request);
                return assertOkAndParse(response, "PPL query");
            }
        );

        if (failures.isEmpty() == false) {
            fail("PPL query failures (" + failures.size() + " of " + queryNumbers.size() + "):\n" + String.join("\n", failures));
        }
    }
}

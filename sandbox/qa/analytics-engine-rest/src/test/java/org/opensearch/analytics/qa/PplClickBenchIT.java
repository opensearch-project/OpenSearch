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
    //  - Missing feature: Q19 (extract(minute from …)), Q40 (case() else + head N from M),
    //    Q43 (date_format() + head N from M).
    //  - Substrait emit can't find a MIN binding for VARCHAR inputs (isthmus library):
    //    Q29 (min(Referer) where Referer is text). Needs a min(string) binding in
    //    the aggregate function catalog or an equivalent adapter.
    //  - Multi-shard exchange can't serialize TIMESTAMP (LocalDateTime): Q7, Q24-Q27,
    //    Q37-Q42.
    //  - WHERE + GROUP-BY + aggregate on multi-shard triggers Arrow "project index 0
    //    out of bounds, max field 0": Q11, Q12, Q13, Q14, Q15, Q22, Q23, Q31, Q32;
    //    plus Q20 (WHERE + fields, no aggregate, still routed through multi-shard path).
    // DEBUG: temporarily un-skip the multi-shard-only failures to see if they
    // pass on single-shard (where the split rule doesn't fire and no exchange
    // traffic / no native-side aggregate reduce is exercised).
    // Queries skipped — all known PPL frontend / Substrait gaps, unrelated to the
    // distributed aggregate execution path:
    //  - Q19: extract(minute from …) not supported by the PPL frontend.
    //  - Q29: Substrait can't bind MIN on VARCHAR inputs (isthmus library limitation).
    //    Requires a min(string) binding in the aggregate function catalog.
    //  - Q40: case() else + head N from M — PPL frontend gap.
    //  - Q43: date_format() + head N from M — PPL frontend gap.
    private static final Set<Integer> SKIP_QUERIES = Set.of(19, 29, 40, 43);

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

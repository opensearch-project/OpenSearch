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
    // Queries skipped:
    //  - Missing feature: Q19 (extract(minute from …)), Q40 (case() else + head N from M),
    //    Q43 (date_format() + head N from M).
    //  - Substrait emit can't find a MIN binding for VARCHAR inputs (isthmus library):
    //    Q29 (min(Referer) where Referer is text). Needs a min(string) binding in
    //    the aggregate function catalog or an equivalent adapter.
    //  - Multi-shard: aggregates / projections on TIMESTAMP/DATE fields hit a DataFusion
    //    'Panic: primitive array' in streamNext — shard-side partial emits a timestamp
    //    encoding that the coordinator's Arrow kernel doesn't accept. Covered by the
    //    upcoming force_output_schema / per-aggregate state-coercion wrapper on the Rust
    //    side (§14.5 of the design doc). Queries: Q7, Q24-Q27, Q37-Q42.
    private static final Set<Integer> SKIP_QUERIES = Set.of(
        7, 19, 24, 25, 26, 27, 29, 37, 38, 39, 40, 41, 42, 43
    );

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

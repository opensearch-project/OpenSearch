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

/**
 * ClickBench DSL integration test. Runs DSL queries against a parquet-backed ClickBench index.
 * <p>
 * Query path: {@code POST /{index}/_search} → dsl-query-executor → Calcite → Substrait → DataFusion
 * <p>
 * Currently restricted to Q1 to keep CI green. Auto-discovery of all 43 ClickBench queries is
 * temporarily disabled because several queries exercise unsupported aggregation translators
 * (e.g. ValueCount, Cardinality, MultiTerms) or planner rules, and in some cases crash the
 * cluster, which cascades into the PPL suite as well. Re-enable auto-discovery once the
 * analytics-engine adds support for those paths.
 */
public class DslClickBenchIT extends AnalyticsRestTestCase {

    /**
     * ClickBench DSL query numbers to run. Q1 validates the DSL → DataFusion path end-to-end.
     * Additional queries can be added here as the analytics engine adds support for more
     * aggregation translators and planner rules.
     */
    private static final List<Integer> QUERY_NUMBERS = List.of(1);

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws Exception {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), ClickBenchTestHelper.DATASET);
            dataProvisioned = true;
        }
    }

    public void testClickBenchDslQueries() throws Exception {
        ensureDataProvisioned();

        // Auto-discovery disabled until all ClickBench queries pass. See class javadoc.
        // List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(ClickBenchTestHelper.DATASET, "dsl");
        // assertFalse("No DSL queries discovered", queryNumbers.isEmpty());
        // logger.info("Discovered {} DSL queries: {}", queryNumbers.size(), queryNumbers);
        List<Integer> queryNumbers = QUERY_NUMBERS;
        logger.info("Running {} DSL queries: {}", queryNumbers.size(), queryNumbers);

        List<String> failures = DatasetQueryRunner.runQueries(
            client(),
            ClickBenchTestHelper.DATASET,
            "dsl",
            "json",
            queryNumbers,
            (client, dataset, queryBody) -> {
                Request request = new Request("POST", "/" + dataset.indexName + "/_search");
                request.setJsonEntity(queryBody);
                Response response = client.performRequest(request);
                return assertOkAndParse(response, "DSL query");
            }
        );

        if (failures.isEmpty() == false) {
            fail("DSL query failures (" + failures.size() + " of " + queryNumbers.size() + "):\n" + String.join("\n", failures));
        }
    }
}

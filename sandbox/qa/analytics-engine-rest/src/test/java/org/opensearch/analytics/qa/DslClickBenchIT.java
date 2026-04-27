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
 * Currently runs a subset of queries (see {@link #QUERY_NUMBERS}). To run all 43 ClickBench
 * queries, use {@link DatasetQueryRunner#discoverQueryNumbers(Dataset, String)}. Some queries
 * hit analytics-engine planner/translator limitations and are excluded until resolved.
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

        logger.info("Running {} DSL queries: {}", QUERY_NUMBERS.size(), QUERY_NUMBERS);

        List<String> failures = DatasetQueryRunner.runQueries(
            client(),
            ClickBenchTestHelper.DATASET,
            "dsl",
            "json",
            QUERY_NUMBERS,
            (client, dataset, queryBody) -> {
                Request request = new Request("POST", "/" + dataset.indexName + "/_search");
                request.setJsonEntity(queryBody);
                Response response = client.performRequest(request);
                return assertOkAndParse(response, "DSL query");
            }
        );

        if (failures.isEmpty() == false) {
            fail("DSL query failures:\n" + String.join("\n", failures));
        }
    }
}

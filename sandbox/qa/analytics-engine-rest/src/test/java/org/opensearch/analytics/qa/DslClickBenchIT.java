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
 * ClickBench DSL integration test. Auto-discovers and runs all DSL queries from
 * {@code datasets/clickbench/dsl/} against a parquet-backed ClickBench index.
 * <p>
 * Query path: {@code POST /{index}/_search} → dsl-query-executor → Calcite → Substrait → DataFusion
 * <p>
 * Failures are collected without fail-fast so all discovered queries are attempted.
 */
public class DslClickBenchIT extends AnalyticsRestTestCase {

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws Exception {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), ClickBenchTestHelper.DATASET);
            dataProvisioned = true;
        }
    }

    public void testClickBenchDslQueries() throws Exception {
        ensureDataProvisioned();

        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(ClickBenchTestHelper.DATASET, "dsl");
        assertFalse("No DSL queries discovered", queryNumbers.isEmpty());
        logger.info("Discovered {} DSL queries: {}", queryNumbers.size(), queryNumbers);

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

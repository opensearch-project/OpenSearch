/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PPL integration test for the
 * <a href="https://github.com/opensearch-project/opensearch-benchmark-workloads/blob/main/big5/operations/ppl.json">big5 benchmark workload</a>.
 */
public class PplBig5IT extends AnalyticsRestTestCase {

    private static final ExpectedResponseStrategy STRATEGY = ExpectedResponseStrategy.PASS_ON_MISSING;

    /** Query numbers to skip until the underlying gap is closed. Empty: all 46 pass today. */
    private static final Set<Integer> SKIP_QUERIES = Set.of();

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), Big5TestHelper.DATASET);
            dataProvisioned = true;
        }
    }

    public void testBig5PplQueries() throws Exception {
        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(Big5TestHelper.DATASET, "ppl")
            .stream()
            .filter(n -> SKIP_QUERIES.contains(n) == false)
            .toList();
        assertFalse("No PPL queries discovered", queryNumbers.isEmpty());
        logger.info("Running {} big5 PPL queries (of {} discovered): {}", queryNumbers.size(), queryNumbers.size(), queryNumbers);

        List<String> failures = DatasetQueryRunner.runQueries(
            client(),
            Big5TestHelper.DATASET,
            "ppl",
            "ppl",
            queryNumbers,
            (client, dataset, queryBody) -> {
                String ppl = queryBody.trim().replace("big5", dataset.indexName);
                Request request = new Request("POST", "/_plugins/_ppl");
                request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
                Response response = client.performRequest(request);
                return assertOkAndParse(response, "PPL query");
            },
            STRATEGY
        );

        if (failures.isEmpty() == false) {
            fail("Big5 PPL query failures (" + failures.size() + " of " + queryNumbers.size() + "):\n" + String.join("\n", failures));
        }
    }
}

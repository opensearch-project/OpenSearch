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
 * Complex Redesigned (multi-index) PPL integration test. Runs PPL queries against complex_redesigned data.
 */
public class MultiSourceJoinsPplIT extends AnalyticsRestTestCase {

    private static final ExpectedResponseStrategy STRATEGY = ExpectedResponseStrategy.PASS_ON_MISSING;
    private static final Set<Integer> SKIP_QUERIES = Set.of();
    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws Exception {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), MultiSourceJoinsTestHelper.DATASET);
            dataProvisioned = true;
        }
    }

    public void testMultiSourceJoinsPplQueries() throws Exception {
        ensureDataProvisioned();

        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(MultiSourceJoinsTestHelper.DATASET, "ppl")
            .stream()
            .filter(n -> SKIP_QUERIES.contains(n) == false)
            .toList();
        assertFalse("No PPL queries discovered", queryNumbers.isEmpty());
        logger.info("Running {} Complex Redesigned (multi-index) PPL queries: {}", queryNumbers.size(), queryNumbers);

        List<String> failures = DatasetQueryRunner.runQueries(
            client(),
            MultiSourceJoinsTestHelper.DATASET,
            "ppl",
            "ppl",
            queryNumbers,
            (client, dataset, queryBody) -> {
                String ppl = queryBody.trim();
                Request request = new Request("POST", "/_analytics/ppl");
                request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
                Response response = client.performRequest(request);
                return assertOkAndParse(response, "PPL query");
            },
            STRATEGY
        );

        if (failures.isEmpty() == false) {
            fail("Complex Redesigned (multi-index) PPL query failures (" + failures.size() + " of " + queryNumbers.size() + "):\n" + String.join("\n", failures));
        }
    }
}

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
import java.util.Set;

/**
 * Base class for PPL integration tests. Provides common test execution logic.
 */
public abstract class BasePplIT extends AnalyticsRestTestCase {

    private static final ExpectedResponseStrategy DEFAULT_STRATEGY = ExpectedResponseStrategy.PASS_ON_MISSING;
    private boolean dataProvisioned = false;

    protected abstract Dataset getDataset();

    protected ExpectedResponseStrategy getStrategy() {
        return DEFAULT_STRATEGY;
    }

    protected Set<Integer> getSkipQueries() {
        return Set.of();
    }

    @Override
    protected void onBeforeQuery() throws IOException {
        if (!dataProvisioned) {
            DatasetProvisioner.provision(client(), getDataset());
            dataProvisioned = true;
        }
    }

    protected void runPplQueries() throws Exception {
        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(getDataset(), "ppl")
            .stream()
            .filter(n -> !getSkipQueries().contains(n))
            .toList();
        assertFalse("No PPL queries discovered", queryNumbers.isEmpty());
        logger.info("Running {} {} PPL queries: {}", queryNumbers.size(), getDataset().name, queryNumbers);

        List<String> failures = DatasetQueryRunner.runQueries(
            client(),
            getDataset(),
            "ppl",
            "ppl",
            queryNumbers,
            (client, dataset, queryBody) -> {
                String ppl = queryBody.trim();
                Request request = new Request("POST", "/_plugins/_ppl");
                request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
                Response response = client.performRequest(request);
                return assertOkAndParse(response, "PPL query");
            },
            getStrategy()
        );

        if (!failures.isEmpty()) {
            fail(getDataset().name + " PPL query failures (" + failures.size() + " of " + queryNumbers.size() + "):\n" + String.join("\n", failures));
        }
    }
}

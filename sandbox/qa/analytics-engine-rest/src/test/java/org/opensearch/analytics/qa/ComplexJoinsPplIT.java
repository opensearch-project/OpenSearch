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
 * Complex Joins PPL integration test (multi-index). Tests join operations across multiple indexes.
 * Uses existing indexes from other datasets: security_logs, app_monitor, kubernetes_logs,
 * monitor_tracking, performance_metrics, voice_verification.
 */
public class ComplexJoinsPplIT extends AnalyticsRestTestCase {

    private static final ExpectedResponseStrategy STRATEGY = ExpectedResponseStrategy.PASS_ON_MISSING;
    private static final Set<Integer> SKIP_QUERIES = Set.of();
    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws Exception {
        if (dataProvisioned == false) {
            // Provision all required indexes
            DatasetProvisioner.provision(client(), SecurityLogsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), MultiSourceJoinsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), KubernetesLogsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), PerformanceMetricsTestHelper.DATASET);
            dataProvisioned = true;
        }
    }

    public void testComplexJoinsPplQueries() throws Exception {
        ensureDataProvisioned();

        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(ComplexJoinsTestHelper.DATASET, "ppl")
            .stream()
            .filter(n -> SKIP_QUERIES.contains(n) == false)
            .toList();
        assertFalse("No PPL queries discovered", queryNumbers.isEmpty());
        logger.info("Running {} Complex Joins PPL queries: {}", queryNumbers.size(), queryNumbers);

        List<String> failures = DatasetQueryRunner.runQueries(
            client(),
            ComplexJoinsTestHelper.DATASET,
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
            fail("Complex Joins PPL query failures (" + failures.size() + " of " + queryNumbers.size() + "):\n" + String.join("\n", failures));
        }
    }
}

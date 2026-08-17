/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Integration test for the filter predicate count guard.
 * Validates that queries exceeding the configured limit are rejected with HTTP 400.
 */
public class FilterPredicateGuardIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /**
     * A query with more leaf predicates than the configured limit is rejected.
     * Default limit is 200; we lower it to 5 via cluster settings and send a
     * query with 10 OR-ed predicates.
     */
    public void testExcessivePredicateCountRejected() throws Exception {
        ensureDataProvisioned();

        // Lower the limit to 5 predicates
        updateClusterSetting("analytics.query.max_filter_predicate_count", "5");
        try {
            // Build: source=calcs | where num0=1 OR num0=2 OR ... OR num0=10
            String predicates = IntStream.rangeClosed(1, 10)
                .mapToObj(i -> "num0=" + i)
                .collect(Collectors.joining(" OR "));
            String ppl = "source=" + DATASET.indexName + " | where " + predicates + " | fields num0";

            ResponseException e = expectThrows(ResponseException.class, () -> executePpl(ppl));
            int status = e.getResponse().getStatusLine().getStatusCode();
            assertEquals("Expected HTTP 400 for excessive predicate count", 400, status);
            String body = org.apache.hc.core5.http.io.entity.EntityUtils.toString(e.getResponse().getEntity());
            assertTrue(
                "Error message should mention predicate count, got: " + body,
                body.contains("predicates") && body.contains("maximum allowed")
            );
        } finally {
            // Reset to default
            updateClusterSetting("analytics.query.max_filter_predicate_count", null);
        }
    }

    /**
     * A query within the predicate count limit succeeds.
     */
    public void testAcceptablePredicateCountSucceeds() throws Exception {
        ensureDataProvisioned();

        // Default limit is 200 — a query with 3 predicates should pass fine
        String ppl = "source=" + DATASET.indexName + " | where num0=1 OR num0=2 OR num0=3 | fields num0";
        executePpl(ppl); // should not throw
    }

    /**
     * Setting the limit to 0 disables the guard entirely.
     */
    public void testDisabledGuardAllowsAnything() throws Exception {
        ensureDataProvisioned();

        // Disable the guard
        updateClusterSetting("analytics.query.max_filter_predicate_count", "0");
        try {
            // 15 predicates — would fail with a limit of 5, but 0 means unlimited
            String predicates = IntStream.rangeClosed(1, 15)
                .mapToObj(i -> "num0=" + i)
                .collect(Collectors.joining(" OR "));
            String ppl = "source=" + DATASET.indexName + " | where " + predicates + " | fields num0";
            executePpl(ppl); // should not throw
        } finally {
            updateClusterSetting("analytics.query.max_filter_predicate_count", null);
        }
    }

    private void updateClusterSetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        if (value != null) {
            request.setJsonEntity("{\"transient\": {\"" + key + "\": " + value + "}}");
        } else {
            request.setJsonEntity("{\"transient\": {\"" + key + "\": null}}");
        }
        client().performRequest(request);
    }
}

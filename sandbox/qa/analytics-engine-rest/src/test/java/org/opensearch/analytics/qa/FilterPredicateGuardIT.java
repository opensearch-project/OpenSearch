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
 * Integration test for the filter predicate count guard
 * ({@code analytics.query.max_filter_predicate_count}).
 *
 * <p>Covers the two shapes that behave differently against the guard:
 * <ul>
 *   <li><b>Distinct-field fan-out</b> ({@code a=1 OR b=2 OR c=3 ...}) — cannot be folded, stays as
 *       N separate leaf predicates, and IS caught by the guard.</li>
 *   <li><b>Same-field fan-out</b> ({@code x=1 OR x=2 OR x=3 ...}) — folded by Calcite into a single
 *       {@code SEARCH(x, Sarg[...])} before the guard runs, counts as one predicate, and is NOT
 *       caught regardless of the value count.</li>
 * </ul>
 */
public class FilterPredicateGuardIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static boolean dataProvisioned = false;

    // calcs has num0..num4 and int0..int3 — nine distinct numeric fields. Using each field at most
    // once guarantees no two comparisons can SARG-fold together (folding requires the same field).
    private static final String[] DISTINCT_FIELDS = { "num0", "num1", "num2", "num3", "num4", "int0", "int1", "int2" };

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /**
     * Distinct-field fan-out is NOT foldable and IS caught by the guard.
     *
     * <p>Eight predicates, each on a <em>different</em> field ({@code num0=0 OR num1=1 OR num2=2
     * ...}), cannot collapse into a SARG — a Sarg is a range-set over a single field, so folding
     * only combines comparisons that share a field. All eight survive to the marking phase as
     * separate leaves. With the limit lowered to 5, the guard rejects with 400.
     */
    public void testDistinctFieldPredicatesExceedingLimitRejected() throws Exception {
        ensureDataProvisioned();

        updateClusterSetting("analytics.query.max_filter_predicate_count", "5");
        try {
            // 8 predicates, one per distinct field → 8 non-foldable leaves (> limit of 5).
            String predicates = IntStream.range(0, DISTINCT_FIELDS.length)
                .mapToObj(i -> DISTINCT_FIELDS[i] + "=" + i)
                .collect(Collectors.joining(" OR "));
            String ppl = "source=" + DATASET.indexName + " | where " + predicates + " | fields num0";

            ResponseException e = expectThrows(ResponseException.class, () -> executePpl(ppl));
            int status = e.getResponse().getStatusLine().getStatusCode();
            assertEquals("Expected HTTP 400 for excessive distinct-field predicate count", 400, status);
            String body = org.apache.hc.core5.http.io.entity.EntityUtils.toString(e.getResponse().getEntity());
            assertTrue(
                "Error message should mention predicate count, got: " + body,
                body.contains("predicates") && body.contains("maximum allowed")
            );
        } finally {
            updateClusterSetting("analytics.query.max_filter_predicate_count", null);
        }
    }

    /**
     * Same-field fan-out IS folded and is NOT caught by the guard.
     *
     * <p>This is the key contrast to {@link #testDistinctFieldPredicatesExceedingLimitRejected}.
     * Fifty comparisons all against {@code num0} ({@code num0=0 OR num0=1 OR ... OR num0=49}) are
     * folded by Calcite's {@code ReduceExpressionsRule} into a single {@code SEARCH(num0, Sarg[..])}
     * predicate during the {@code reduceExpressions} phase, which runs <em>before</em> the marking
     * phase where the guard counts leaves. So even with the limit lowered to 5 — far below the 50
     * literal values — the folded condition counts as one predicate and the query succeeds. This
     * confirms the guard targets genuine per-predicate cost (distinct-field fan-out), not cheap
     * SARG-foldable value lists.
     */
    public void testSameFieldFoldedPredicatesNotCaught() throws Exception {
        ensureDataProvisioned();

        updateClusterSetting("analytics.query.max_filter_predicate_count", "5");
        try {
            // 50 same-field equalities → folded to ONE SEARCH(num0, Sarg[...]) before the guard runs.
            String predicates = IntStream.range(0, 50)
                .mapToObj(i -> "num0=" + i)
                .collect(Collectors.joining(" OR "));
            String ppl = "source=" + DATASET.indexName + " | where " + predicates + " | fields num0";
            executePpl(ppl); // should NOT throw despite 50 literals and a limit of 5
        } finally {
            updateClusterSetting("analytics.query.max_filter_predicate_count", null);
        }
    }

    /**
     * A distinct-field query within the limit succeeds.
     */
    public void testAcceptablePredicateCountSucceeds() throws Exception {
        ensureDataProvisioned();

        // 3 distinct-field predicates, well under the default limit.
        String ppl = "source=" + DATASET.indexName + " | where num0=1 OR num1=2 OR num2=3 | fields num0";
        executePpl(ppl); // should not throw
    }

    /**
     * Setting the limit to 0 disables the guard entirely.
     */
    public void testDisabledGuardAllowsAnything() throws Exception {
        ensureDataProvisioned();

        updateClusterSetting("analytics.query.max_filter_predicate_count", "0");
        try {
            // 8 distinct-field predicates — would be caught at a limit of 5, but 0 means unlimited.
            String predicates = IntStream.range(0, DISTINCT_FIELDS.length)
                .mapToObj(i -> DISTINCT_FIELDS[i] + "=" + i)
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

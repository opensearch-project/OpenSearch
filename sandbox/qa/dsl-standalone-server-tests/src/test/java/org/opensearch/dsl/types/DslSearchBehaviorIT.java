/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.types;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

/**
 * Behavioral integration tests for the DSL search path that cannot be expressed as golden-file
 * comparisons. Per-query-type result correctness (including {@code term}) is already covered by
 * {@link DslQueryTypesIT}, which sweeps every query type and validates each response against a
 * recorded golden. This class holds only checks that require <b>repeated execution with randomized
 * parameters</b> and assert an invariant — something a single-shot golden snapshot cannot capture.
 *
 * <p>Dataset: {@code datasets/people/} (5 docs), provisioned into a dual-format index
 * (primary=parquet, secondary=lucene).
 *
 * <pre>
 *   ./gradlew :sandbox:qa:dsl-standalone-server-tests:restTest \
 *     --tests "org.opensearch.dsl.types.DslSearchBehaviorIT" -PrestCluster=localhost:9200
 * </pre>
 */
public class DslSearchBehaviorIT extends OpenSearchRestTestCase {

    private static final Dataset PEOPLE = new Dataset("people", "dsl_people");
    private static boolean provisioned = false;

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void provisionOnce() throws IOException {
        if (provisioned == false) {
            DatasetProvisioner.provision(client(), PEOPLE);
            provisioned = true;
        }
    }

    /**
     * Runs {@code match_all} repeatedly with randomized {@code preference} values and asserts the hit
     * count is stably the full document count (5) every time — i.e. {@code preference} routing never
     * changes the result set. This repeat-with-random-parameter invariant cannot be expressed as a
     * single-shot golden snapshot, which is why it lives here rather than in {@link DslQueryTypesIT}.
     * The dataset is bulk-ingested with auto-generated document ids via {@link DatasetProvisioner}.
     */
    public void testMatchAllStableCountWithRandomPreference() throws IOException {
        int iters = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < iters; i++) {
            String preference = randomAlphaOfLengthBetween(1, 4);
            // preference must not start with '_' (reserved for known types e.g. _shards, _primary)
            while (preference.startsWith("_")) {
                preference = randomAlphaOfLengthBetween(1, 4);
            }
            Map<String, Object> response = search("{\"query\":{\"match_all\":{}}}", preference);
            assertEquals("match_all count must be stable across preferences", 5, totalHits(response));
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** Run a search with an optional {@code preference} query param. */
    private Map<String, Object> search(String body, String preference) throws IOException {
        Request request = new Request("POST", "/" + PEOPLE.indexName + "/_search");
        request.setJsonEntity(body);
        if (preference != null) {
            request.addParameter("preference", preference);
        }
        Response response = client().performRequest(request);
        assertEquals("expected HTTP 200 for: " + body, 200, response.getStatusLine().getStatusCode());
        return entityAsMap(response);
    }

    @SuppressWarnings("unchecked")
    private static int totalHits(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        Map<String, Object> total = (Map<String, Object>) hits.get("total");
        return ((Number) total.get("value")).intValue();
    }
}

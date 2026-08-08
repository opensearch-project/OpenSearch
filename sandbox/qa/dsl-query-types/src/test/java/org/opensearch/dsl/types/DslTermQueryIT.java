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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Focused integration test for the DSL {@code term} query against the live sandbox server, running
 * over a parquet/composite index (primary=parquet, secondary=lucene) via the standard search path.
 *
 * <p>Unlike {@link DslQueryTypesIT} — which sweeps every query type and validates each response
 * against a golden file — this test mirrors the depth of the {@code server/.../search/query/*IT}
 * tests: it asserts <b>real result correctness</b> (hit count, {@code _source} values, matched
 * documents) for a single query type, in code rather than from a golden.
 *
 * <p>Dataset: {@code datasets/people/} (5 docs — alice/bob/carol/dave/eve; 3 in seattle, 2 in
 * portland), provisioned into a dual-format index (primary=parquet, secondary=lucene).
 *
 * <p>Assertions covered for the {@code term} query: exact hit count, exact {@code _source} field
 * values, and result ordering. Not asserted: {@code _score} / relevance (the analytics engine
 * computes no relevance score — {@code max_score}/{@code _score} are null on this backend).
 *
 * <pre>
 *   ./gradlew :sandbox:qa:dsl-query-types:restTest \
 *     --tests "org.opensearch.dsl.types.DslTermQueryIT" -PrestCluster=localhost:9200
 * </pre>
 */
public class DslTermQueryIT extends OpenSearchRestTestCase {

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

    /** term on a keyword field matching 3 docs: assert exact count and the matching names. */
    public void testTermMatchesExpectedDocuments() throws IOException {
        Map<String, Object> response = search("{\"query\":{\"term\":{\"city\":\"seattle\"}}}");

        assertEquals("hit count for term(city=seattle)", 3, totalHits(response));
        assertEquals("names matching city=seattle", List.of("alice", "carol", "eve"), sortedNames(response));
    }

    /** term matching a different value set: assert count and names. */
    public void testTermMatchesOtherValue() throws IOException {
        Map<String, Object> response = search("{\"query\":{\"term\":{\"city\":\"portland\"}}}");

        assertEquals("hit count for term(city=portland)", 2, totalHits(response));
        assertEquals("names matching city=portland", List.of("bob", "dave"), sortedNames(response));
    }

    /** term matching exactly one doc: assert the full _source values of that doc. */
    public void testTermSingleDocumentSourceValues() throws IOException {
        Map<String, Object> response = search("{\"query\":{\"term\":{\"name\":\"alice\"}}}");

        assertEquals("hit count for term(name=alice)", 1, totalHits(response));
        Map<String, Object> src = firstSource(response);
        assertEquals("alice", src.get("name"));
        assertEquals("seattle", src.get("city"));
        assertEquals(30, ((Number) src.get("age")).intValue());
        assertEquals(95.5, ((Number) src.get("score")).doubleValue(), 1e-9);
        assertEquals("This is seattle city", src.get("description"));
    }

    /** term matching no docs: assert an empty result (0 hits), not an error. */
    public void testTermMatchesNothing() throws IOException {
        Map<String, Object> response = search("{\"query\":{\"term\":{\"city\":\"denver\"}}}");

        assertEquals("hit count for term(city=denver)", 0, totalHits(response));
        assertTrue("hits should be empty for a non-matching term", hits(response).isEmpty());
    }

    /**
     * REST equivalent of {@code SimpleSearchIT.testSearchRandomPreference}: run {@code match_all}
     * repeatedly with randomized {@code preference} values and assert the hit count is stably the
     * full document count (5) every time. Unlike the transport-client original — which uses
     * {@code indexRandom(...).setId(...)} — the parquet/composite index sets
     * {@code index.append_only.enabled} (custom ids rejected), so the dataset is bulk-ingested with
     * auto-generated ids via {@link DatasetProvisioner}.
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

    private Map<String, Object> search(String body) throws IOException {
        Request request = new Request("POST", "/" + PEOPLE.indexName + "/_search");
        request.setJsonEntity(body);
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

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> hits(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        return (List<Map<String, Object>>) hits.get("hits");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> firstSource(Map<String, Object> response) {
        return (Map<String, Object>) hits(response).get(0).get("_source");
    }

    /** Names from every hit's _source, sorted — order-independent set comparison for filter tests. */
    @SuppressWarnings("unchecked")
    private static List<String> sortedNames(Map<String, Object> response) {
        List<String> names = new ArrayList<>();
        for (Map<String, Object> hit : hits(response)) {
            Map<String, Object> src = (Map<String, Object>) hit.get("_source");
            names.add((String) src.get("name"));
        }
        return names.stream().sorted().collect(Collectors.toList());
    }
}

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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for {@code POST /_plugins/_analytics_backend_datafusion/cache/_clear}.
 *
 * <p>Tests verify:
 * <ul>
 *   <li>Clear all: no params — returns 200, {@code acknowledged=true}, non-empty {@code cleared_nodes}</li>
 *   <li>Selective clear: {@code ?footer=true}, {@code ?column=true}, {@code ?offset=true} — each returns 200</li>
 *   <li>Combined params: {@code ?column=true&offset=true} — returns 200</li>
 *   <li>Unknown param: ignored (backward compat) — still returns 200</li>
 * </ul>
 *
 * <p>The cache contents themselves are not observable via REST (stats endpoint is PR 1),
 * so these tests verify only that the action completes successfully and returns a
 * well-formed response.
 */
public class DataFusionCacheClearIT extends AnalyticsRestTestCase {

    private static final String CLEAR_ENDPOINT = "/_plugins/_analytics_backend_datafusion/cache/_clear";

    // ── clear all ────────────────────────────────────────────────────────────

    public void testClearAllCachesReturns200WithAcknowledged() throws IOException {
        Map<String, Object> body = clearCache("");
        assertEquals(true, body.get("acknowledged"));
        assertClearedNodes(body);
    }

    // ── selective: single param ───────────────────────────────────────────────

    public void testClearFooterCacheOnly() throws IOException {
        Map<String, Object> body = clearCache("?footer=true");
        assertEquals(true, body.get("acknowledged"));
        assertClearedNodes(body);
    }

    public void testClearColumnIndexCacheOnly() throws IOException {
        Map<String, Object> body = clearCache("?column=true");
        assertEquals(true, body.get("acknowledged"));
        assertClearedNodes(body);
    }

    public void testClearOffsetIndexCacheOnly() throws IOException {
        Map<String, Object> body = clearCache("?offset=true");
        assertEquals(true, body.get("acknowledged"));
        assertClearedNodes(body);
    }

    // ── selective: combined params ────────────────────────────────────────────

    public void testClearColumnAndOffsetTogether() throws IOException {
        Map<String, Object> body = clearCache("?column=true&offset=true");
        assertEquals(true, body.get("acknowledged"));
        assertClearedNodes(body);
    }

    public void testClearAllThreeExplicitly() throws IOException {
        Map<String, Object> body = clearCache("?footer=true&column=true&offset=true");
        assertEquals(true, body.get("acknowledged"));
        assertClearedNodes(body);
    }

    // ── idempotency ───────────────────────────────────────────────────────────

    public void testClearIsIdempotent() throws IOException {
        // Second clear of an already-empty cache must still return 200.
        clearCache("");
        Map<String, Object> body = clearCache("");
        assertEquals(true, body.get("acknowledged"));
        assertClearedNodes(body);
    }

    // ── response shape ────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    public void testResponseIncludesNodesAndClusterName() throws IOException {
        Request request = new Request("POST", CLEAR_ENDPOINT);
        Response response = client().performRequest(request);
        Map<String, Object> body = assertOkAndParse(response, "cache/_clear");

        // NodesResponseRestListener wraps with _nodes + cluster_name
        assertNotNull("_nodes header present", body.get("_nodes"));
        assertNotNull("cluster_name present", body.get("cluster_name"));

        Map<String, Object> nodesHeader = (Map<String, Object>) body.get("_nodes");
        int total = ((Number) nodesHeader.get("total")).intValue();
        int successful = ((Number) nodesHeader.get("successful")).intValue();
        assertTrue("at least one node total", total >= 1);
        assertTrue("all nodes successful", successful == total);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void assertClearedNodes(Map<String, Object> body) {
        List<Object> clearedNodes = (List<Object>) body.get("cleared_nodes");
        assertNotNull("cleared_nodes present", clearedNodes);
        assertFalse("at least one node cleared", clearedNodes.isEmpty());
    }

    private Map<String, Object> clearCache(String queryParams) throws IOException {
        Request request = new Request("POST", CLEAR_ENDPOINT + queryParams);
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "cache/_clear" + queryParams);
    }
}

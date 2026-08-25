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
 * Integration test for the can_match section of the profile=true response on SHARD_FRAGMENT stages.
 *
 * <p>can_match only runs when the query has range filters or a bounded-field sort AND the fan-out
 * clears the {@code worthPreFiltering} threshold. A {@code sort | head N} drops that threshold to 1,
 * and we additionally force {@code analytics.query.pre_filter_shard_size=1} so the probe reliably
 * fires on this small multi-shard index rather than depending on a production-scale fan-out.
 */
public class CanMatchProfileIT extends AnalyticsRestTestCase {

    private static final String INDEX = "canmatch_profile_test";
    private static final String PRE_FILTER_SETTING = "analytics.query.pre_filter_shard_size";
    private static boolean provisioned = false;

    private void ensureProvisioned() throws IOException {
        if (!provisioned) {
            createIndex();
            indexData();
            provisioned = true;
        }
    }

    /**
     * A {@code sort ts | head N} query over a 2-shard index triggers the can_match probe (and the
     * top-N gate). The SHARD_FRAGMENT stage's profile must carry a can_match block with sensible,
     * internally-consistent counts.
     */
    @SuppressWarnings("unchecked")
    public void testCanMatchProfileOnSortQuery() throws IOException {
        ensureProvisioned();
        applySetting(PRE_FILTER_SETTING, "1");
        try {
            Map<String, Object> result = executeWithProfile("source = " + INDEX + " | sort ts | fields host | head 3");

            Map<String, Object> profile = (Map<String, Object>) result.get("profile");
            assertNotNull("profile present", profile);
            List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
            assertNotNull("stages present", stages);

            Map<String, Object> canMatch = findCanMatch(stages);
            assertNotNull("a SHARD_FRAGMENT stage must carry a can_match block. stages: " + stages, canMatch);

            // Latency is always recorded (>= 0). total_shards must be the 2 shards we created.
            assertNotNull("can_match_ms present", canMatch.get("can_match_ms"));
            int total = ((Number) canMatch.get("total_shards")).intValue();
            assertEquals("total_shards should equal the index shard count", 2, total);

            int pruned = ((Number) canMatch.get("shards_pruned_by_filter")).intValue();
            int skipped = ((Number) canMatch.get("shards_skipped_by_topn")).intValue();
            int dispatched = ((Number) canMatch.get("shards_dispatched")).intValue();
            assertNotNull("topn_gate_armed present", canMatch.get("topn_gate_armed"));

            // Internal consistency: dispatched = total - pruned - skipped, and none negative.
            assertTrue("pruned >= 0", pruned >= 0);
            assertTrue("skipped >= 0", skipped >= 0);
            assertEquals("dispatched = total - pruned - skipped. can_match: " + canMatch, total - pruned - skipped, dispatched);
            assertTrue("counts must not exceed total. can_match: " + canMatch, pruned + skipped <= total);
        } finally {
            resetSetting(PRE_FILTER_SETTING);
        }
    }

    /**
     * A query with neither range filters nor a bounded sort must NOT run can_match — so no
     * SHARD_FRAGMENT stage should carry a can_match block. Guards against always-on overhead.
     */
    @SuppressWarnings("unchecked")
    public void testNoCanMatchWhenNoFilterOrSort() throws IOException {
        ensureProvisioned();
        // Deliberately leave pre_filter_shard_size at its default and issue a bare projection.
        Map<String, Object> result = executeWithProfile("source = " + INDEX + " | fields host");

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        assertNull("no can_match block expected for a filter-less, sort-less query", findCanMatch(stages));
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Map<String, Object> findCanMatch(List<Map<String, Object>> stages) {
        for (Map<String, Object> stage : stages) {
            if ("SHARD_FRAGMENT".equals(stage.get("execution_type"))) {
                Object cm = stage.get("can_match");
                if (cm != null) {
                    return (Map<String, Object>) cm;
                }
            }
        }
        return null;
    }

    private void createIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"ts\":   { \"type\": \"long\" },"
            + "    \"host\": { \"type\": \"keyword\" }"
            + "  }"
            + "}"
            + "}";

        Request req = new Request("PUT", "/" + INDEX);
        req.setJsonEntity(body);
        client().performRequest(req);
    }

    private void indexData() throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"ts\":").append(1_000_000 + i * 1000).append(",\"host\":\"host_").append(i).append("\"}\n");
        }
        Request req = new Request("POST", "/" + INDEX + "/_bulk");
        req.addParameter("refresh", "true");
        req.setJsonEntity(bulk.toString());
        Response resp = client().performRequest(req);
        assertEquals(200, resp.getStatusLine().getStatusCode());
    }

    private Map<String, Object> executeWithProfile(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\", \"profile\": true}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PROFILE: " + ppl);
    }

    private void applySetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\": {\"" + key + "\": " + value + "}}");
        client().performRequest(request);
    }

    private void resetSetting(String key) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\": {\"" + key + "\": null}}");
        client().performRequest(request);
    }
}

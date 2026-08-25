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
import java.util.Locale;
import java.util.Map;

/**
 * Integration tests for the {@code can_match} block of the {@code profile=true} response on
 * SHARD_FRAGMENT stages.
 *
 * <p>These are the <b>observability</b> counterpart to the behavioral can-match ITs in the
 * coordinator module ({@code CanMatchPruningIT}, {@code SortEarlyTerminationIT}). Those assert that
 * pruning/skipping <i>happened</i> by counting fragment dispatches over the internal transport;
 * they cannot assert the profile because {@code QueryProfile} is deliberately not serialized over
 * the wire (see {@code PPLResponse}) — it only exists on the coordinator-local / REST response
 * path. So the profile can only be validated through the REST endpoint, which is what these tests
 * do. They reuse the same flagship fixture — one single-shard parquet index per day, addressed as a
 * PPL comma-list ({@code source = a,b,c}) so the query routes through the distributed analytics
 * engine where can-match runs (a {@code logs-*} wildcard would bypass it) and day ↔ shard is 1:1,
 * making prune/skip counts deterministic.
 *
 * <p>{@code analytics.query.pre_filter_shard_size=1} forces the probe on for the filter-only cases,
 * which are below the production fan-out threshold; the sort cases drop the threshold to 1 on their
 * own.
 */
public class CanMatchProfileIT extends AnalyticsRestTestCase {

    private static final String DAY_INDEX_PREFIX = "canmatch_profile_2026_07_";
    private static final int FIRST_DAY = 8;
    private static final int TOTAL_DAYS = 5;              // 2026-07-08 .. 2026-07-12, one shard each
    private static final int LAST_DAY = FIRST_DAY + TOTAL_DAYS - 1;
    private static final int DOCS_PER_DAY = 4;
    private static final String PRE_FILTER_SETTING = "analytics.query.pre_filter_shard_size";

    private static String indicesCsv;    // comma-list of the daily indices, provisioned once
    private static boolean provisioned = false;

    private String ensureProvisioned() throws IOException {
        if (!provisioned) {
            indicesCsv = createDailyIndices();
            provisioned = true;
        }
        return indicesCsv;
    }

    // ─── Filter-pruning scenarios (mirror CanMatchPruningIT) ────────────────────────

    /** Trailing lower-bound window: older days prune, recent days survive → some pruned, some dispatched. */
    @SuppressWarnings("unchecked")
    public void testProfileReportsFilterPruning() throws IOException {
        String indices = ensureProvisioned();
        applySetting(PRE_FILTER_SETTING, "1");
        try {
            // Keep only days 11 and 12 (>= 2026-07-11); days 08-10 prune.
            Map<String, Object> canMatch = canMatchBlockFor(
                "source = " + indices + " | where `@timestamp` >= '2026-07-11 00:00:00' | fields host"
            );
            assertNotNull("can_match block expected for a filtered multi-shard query", canMatch);
            assertEquals("total_shards = one per day", TOTAL_DAYS, intField(canMatch, "total_shards"));
            assertEquals("days 08-10 prune", 3, intField(canMatch, "shards_pruned_by_filter"));
            assertEquals("no sort → no top-N skips", 0, intField(canMatch, "shards_skipped_by_topn"));
            assertEquals("topn_gate_armed false without a sort", Boolean.FALSE, canMatch.get("topn_gate_armed"));
            assertConsistent(canMatch);
        } finally {
            resetSetting(PRE_FILTER_SETTING);
        }
    }

    /** Window covering every day: nothing disjoint → nothing pruned, all dispatched. */
    @SuppressWarnings("unchecked")
    public void testProfileReportsNoPruneOnCoveringWindow() throws IOException {
        String indices = ensureProvisioned();
        applySetting(PRE_FILTER_SETTING, "1");
        try {
            Map<String, Object> canMatch = canMatchBlockFor(
                "source = " + indices + " | where `@timestamp` >= '2026-07-01 00:00:00' and `@timestamp` <= '2026-07-31 23:59:59' | fields host"
            );
            assertNotNull("can_match block expected", canMatch);
            assertEquals(TOTAL_DAYS, intField(canMatch, "total_shards"));
            assertEquals("covering window prunes nothing", 0, intField(canMatch, "shards_pruned_by_filter"));
            assertEquals("all shards dispatched", TOTAL_DAYS, intField(canMatch, "shards_dispatched"));
            assertConsistent(canMatch);
        } finally {
            resetSetting(PRE_FILTER_SETTING);
        }
    }

    /** Window disjoint from every day: all prune except the one force-kept for a valid empty result. */
    @SuppressWarnings("unchecked")
    public void testProfilePrunesToSingleShard() throws IOException {
        String indices = ensureProvisioned();
        applySetting(PRE_FILTER_SETTING, "1");
        try {
            Map<String, Object> canMatch = canMatchBlockFor(
                "source = " + indices + " | where `@timestamp` >= '2026-09-01 00:00:00' | fields host"
            );
            assertNotNull("can_match block expected", canMatch);
            assertEquals(TOTAL_DAYS, intField(canMatch, "total_shards"));
            assertEquals("all-but-one pruned (one force-kept)", TOTAL_DAYS - 1, intField(canMatch, "shards_pruned_by_filter"));
            assertEquals("exactly one shard dispatched", 1, intField(canMatch, "shards_dispatched"));
            assertConsistent(canMatch);
        } finally {
            resetSetting(PRE_FILTER_SETTING);
        }
    }

    /** No filter and no sort → the probe is skipped entirely, so no can_match block appears. */
    @SuppressWarnings("unchecked")
    public void testProfileNoCanMatchWithoutFilterOrSort() throws IOException {
        String indices = ensureProvisioned();
        // Leave pre_filter_shard_size at default; a bare projection has nothing to probe.
        Map<String, Object> result = executeWithProfile("source = " + indices + " | fields host");
        List<Map<String, Object>> stages = stagesOf(result);
        assertNull("no can_match block for a filter-less, sort-less query", findCanMatch(stages));
    }

    // ─── Sort top-N gate scenarios (mirror SortEarlyTerminationIT) ──────────────────

    /** DESC sort over disjoint daily ranges: the gate arms; any skips are reported consistently. */
    @SuppressWarnings("unchecked")
    public void testProfileReportsTopNGateArmed() throws IOException {
        String indices = ensureProvisioned();
        // sort drops the threshold to 1 on its own; no setting needed.
        Map<String, Object> canMatch = canMatchBlockFor(
            "source = " + indices + " | sort - `@timestamp` | fields host | head " + DOCS_PER_DAY
        );
        assertNotNull("can_match block expected for a bounded sort", canMatch);
        assertEquals(TOTAL_DAYS, intField(canMatch, "total_shards"));
        // The gate arming is deterministic (one day's worth of rows fills the top-N heap). Whether
        // any shard is actually SKIPPED, however, is timing-dependent: elimination only fires on
        // shards still queued when the gate arms, and the per-node dispatch window (which the
        // coordinator-level SortEarlyTerminationIT pins to 1 for determinism) can't be controlled
        // over REST. So assert the deterministic facts — gate armed + consistent counts — and only
        // that skips never exceed what's eliminable, not that at least one occurred.
        assertEquals("top-N over one day's worth of rows arms the gate", Boolean.TRUE, canMatch.get("topn_gate_armed"));
        assertTrue("skipped shards must be within bounds. block: " + canMatch, intField(canMatch, "shards_skipped_by_topn") <= TOTAL_DAYS - 1);
        assertConsistent(canMatch);
    }

    /** Sort with a large limit (>= all data): the gate never fills, so nothing is skipped. */
    @SuppressWarnings("unchecked")
    public void testProfileTopNNoSkipWhenLimitExceedsData() throws IOException {
        String indices = ensureProvisioned();
        int everything = TOTAL_DAYS * DOCS_PER_DAY + 10;   // more than all rows across all days
        Map<String, Object> canMatch = canMatchBlockFor(
            "source = " + indices + " | sort - `@timestamp` | fields host | head " + everything
        );
        assertNotNull("can_match block expected for a bounded sort", canMatch);
        assertEquals(TOTAL_DAYS, intField(canMatch, "total_shards"));
        assertEquals("limit exceeds all data → nothing can be eliminated", 0, intField(canMatch, "shards_skipped_by_topn"));
        assertEquals("every shard dispatched", TOTAL_DAYS, intField(canMatch, "shards_dispatched"));
        assertConsistent(canMatch);
    }

    // ─── Helpers ────────────────────────────────────────────────────────────────────

    /** Runs a profile=true query and returns the first SHARD_FRAGMENT stage's can_match block (or null). */
    private Map<String, Object> canMatchBlockFor(String ppl) throws IOException {
        return findCanMatch(stagesOf(executeWithProfile(ppl)));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> stagesOf(Map<String, Object> result) {
        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        assertNotNull("stages present", stages);
        return stages;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> findCanMatch(List<Map<String, Object>> stages) {
        for (Map<String, Object> stage : stages) {
            if ("SHARD_FRAGMENT".equals(stage.get("execution_type")) && stage.get("can_match") != null) {
                return (Map<String, Object>) stage.get("can_match");
            }
        }
        return null;
    }

    private int intField(Map<String, Object> block, String key) {
        Object v = block.get(key);
        assertNotNull(key + " present in can_match block: " + block, v);
        return ((Number) v).intValue();
    }

    /** dispatched = total - pruned - skipped, no negatives, counts within bounds. */
    private void assertConsistent(Map<String, Object> canMatch) {
        assertNotNull("can_match_ms present", canMatch.get("can_match_ms"));
        int total = intField(canMatch, "total_shards");
        int pruned = intField(canMatch, "shards_pruned_by_filter");
        int skipped = intField(canMatch, "shards_skipped_by_topn");
        int dispatched = intField(canMatch, "shards_dispatched");
        assertTrue("pruned >= 0", pruned >= 0);
        assertTrue("skipped >= 0", skipped >= 0);
        assertTrue("pruned + skipped <= total. block: " + canMatch, pruned + skipped <= total);
        assertEquals("dispatched = total - pruned - skipped. block: " + canMatch, total - pruned - skipped, dispatched);
    }

    /**
     * Creates {@value #TOTAL_DAYS} single-shard daily parquet indices, one day of docs each, and
     * returns the comma-separated index list for a multi-index PPL {@code source =} clause.
     */
    private String createDailyIndices() throws IOException {
        StringBuilder csv = new StringBuilder();
        for (int day = FIRST_DAY; day <= LAST_DAY; day++) {
            String index = String.format(Locale.ROOT, "%s%02d", DAY_INDEX_PREFIX, day);
            try {
                client().performRequest(new Request("DELETE", "/" + index));
            } catch (Exception ignored) {}

            String body = "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": 1,"
                + "  \"number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\","
                + "  \"index.composite.secondary_data_formats\": \"lucene\""
                + "},"
                + "\"mappings\": { \"properties\": {"
                + "  \"@timestamp\": { \"type\": \"date\" },"
                + "  \"host\": { \"type\": \"keyword\" }"
                + "} }"
                + "}";
            Request create = new Request("PUT", "/" + index);
            create.setJsonEntity(body);
            client().performRequest(create);

            StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < DOCS_PER_DAY; i++) {
                bulk.append("{\"index\":{}}\n");
                bulk.append(
                    String.format(
                        Locale.ROOT,
                        "{\"@timestamp\":\"2026-07-%02dT%02d:00:00Z\",\"host\":\"host-%d-%d\"}\n",
                        day,
                        i,
                        day,
                        i
                    )
                );
            }
            Request indexBulk = new Request("POST", "/" + index + "/_bulk");
            indexBulk.addParameter("refresh", "true");
            indexBulk.setJsonEntity(bulk.toString());
            Response resp = client().performRequest(indexBulk);
            assertEquals(200, resp.getStatusLine().getStatusCode());

            if (day > FIRST_DAY) {
                csv.append(',');
            }
            csv.append(index);
        }
        return csv.toString();
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

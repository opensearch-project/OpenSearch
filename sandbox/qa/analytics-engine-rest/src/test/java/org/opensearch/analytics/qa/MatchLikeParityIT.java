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
import java.util.Random;

/**
 * Randomized parity test between the delegated {@code match(...)} query (lowered to Lucene
 * via filter-delegation) and the native {@code LIKE '%target%'} query (executed directly
 * on parquet). The two paths walk independent code, so they must return the same row count
 * on a carefully-constructed corpus.
 *
 * <p><b>Why this exists.</b> Counts drifting between {@code match} and {@code LIKE} after
 * refresh/flush/merge cycles was the signal that surfaced the segment ordinal ↔ writer
 * generation misalignment in the filter-delegation path. This test randomizes ingest batch
 * sizes and the refresh/flush/merge schedule so the specific interleaving that caused the
 * drift keeps getting re-rolled on every run. A fixed-input regression test can only guard
 * against the one interleaving someone remembered.
 *
 * <p><b>Corpus design.</b> Every document's {@code message} is one of two families:
 * <ul>
 *   <li><i>Matching</i>: the target token appears as a whole word, surrounded by spaces
 *       (e.g. {@code "bravo alpha charlie"}). Both the Lucene analyzer (standard tokenizer
 *       lowercases + whitespace-splits) and parquet {@code LIKE '%alpha%'} (substring on the
 *       raw value) select these.</li>
 *   <li><i>Non-matching</i>: exclusively uses tokens that don't contain the target word as
 *       a substring (e.g. {@code "bravo charlie delta"}). Neither {@code match} nor
 *       {@code LIKE '%alpha%'} selects these.</li>
 * </ul>
 * The two families are disjoint and exhaustive, so ground-truth is simply the count of
 * matching docs ingested so far.
 *
 * <p><b>What each iteration exercises.</b>
 * <ol>
 *   <li>Random-sized batch of matching + non-matching docs via {@code _bulk}.</li>
 *   <li>Always refresh (so docs become searchable this iteration).</li>
 *   <li>Randomly flush (forces parquet write).</li>
 *   <li>Randomly force-merge to a random target segment count
 *       (exercises the merge-output writer-generation stamping and segment realignment).</li>
 *   <li>Run both queries and assert each equals the running ground-truth and equals each other.</li>
 * </ol>
 *
 * <p><b>Reproducibility.</b> Uses {@link #randomLong()} from
 * {@code OpenSearchTestCase}, so the seed is printed on failure. Re-run with
 * {@code ./gradlew :...:integTest -Dtests.seed=HEX} to reproduce.
 */
public class MatchLikeParityIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "match_like_parity";
    private static final String TARGET = "alpha";

    /**
     * Templates where the target token appears as a whole word. {@code match(message, 'alpha')}
     * and {@code message LIKE '%alpha%'} both select these.
     */
    private static final String[] MATCHING_TEMPLATES = new String[] {
        "bravo alpha charlie",
        "delta alpha echo foxtrot",
        "alpha golf hotel india",
        "juliet kilo alpha lima",
        "mike november alpha oscar papa",
        "alpha quebec romeo",
        "sierra tango alpha uniform victor" };

    /**
     * Templates that contain none of {@code alpha} as a substring. {@code match} and
     * {@code LIKE '%alpha%'} both reject these. Deliberately avoids words like
     * {@code alphabet}, {@code alphas}, etc. that would break the substring invariant.
     */
    private static final String[] NON_MATCHING_TEMPLATES = new String[] {
        "bravo charlie delta",
        "echo foxtrot golf",
        "hotel india juliet kilo",
        "lima mike november oscar",
        "papa quebec romeo sierra",
        "tango uniform victor whiskey",
        "xray yankee zulu" };

    public void testMatchVsLikeParityAcrossRefreshFlushMerge() throws Exception {
        long seed = randomLong();
        Random random = new Random(seed);
        logger.info("MatchLikeParityIT seed={} (re-run with -Dtests.seed={})", seed, Long.toHexString(seed));

        createCompositeIndex();

        long cumulativeMatching = 0L;
        long cumulativeNonMatching = 0L;

        int iterations = 4 + random.nextInt(9); // 4..12 inclusive
        logger.info("MatchLikeParityIT iterations={}", iterations);

        for (int iter = 0; iter < iterations; iter++) {
            int batchMatching = 50 + random.nextInt(951);      // 50..1000
            int batchNonMatching = 50 + random.nextInt(951);   // 50..1000

            String phase = indexBatch(batchMatching, batchNonMatching, random);
            cumulativeMatching += batchMatching;
            cumulativeNonMatching += batchNonMatching;

            logger.info(
                "iter={} ingest matching={} non_matching={} cumulative_matching={} cumulative_total={} phase={}",
                iter,
                batchMatching,
                batchNonMatching,
                cumulativeMatching,
                cumulativeMatching + cumulativeNonMatching,
                phase
            );

            // Always refresh so the newly-ingested docs become searchable in this iteration.
            refresh();

            // Randomly flush to force parquet writes. Biased toward true so most iterations
            // produce on-disk parquet segments.
            if (random.nextInt(5) < 4) {
                flush();
            }

            // Randomly force-merge. Not all iterations — some leave the segment graph
            // multi-segment so the next query can exercise the multi-leaf path.
            if (random.nextInt(3) == 0) {
                int target = 1 + random.nextInt(3); // 1..3 segments
                forceMerge(target);
                // Refresh again to pick up the merged segment view.
                refresh();
            }

            assertMatchAndLikeAgreeOnCount(iter, cumulativeMatching);
        }

        // Final force-merge-to-one and re-query. Historically the all-in-one-segment path
        // hid the alignment bug because leaf index 0 happens to be the only option; this
        // step documents that the earlier assertions are meaningful precisely because they
        // ran before the single-leaf shortcut.
        forceMerge(1);
        refresh();
        assertMatchAndLikeAgreeOnCount(iterations, cumulativeMatching);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private void createCompositeIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (Exception ignored) {
            // index may not exist yet
        }

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"message\": { \"type\": \"text\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX_NAME);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /**
     * Bulk-indexes {@code matching + nonMatching} docs interleaved in a random order.
     * Intentionally does <em>not</em> pass {@code refresh=true} — refresh is driven by
     * the test's random schedule so each iteration's docs land in their own segment.
     *
     * @return a short tag describing the bulk size, for log lines.
     */
    private String indexBatch(int matching, int nonMatching, Random random) throws IOException {
        int total = matching + nonMatching;
        int[] order = new int[total];
        for (int i = 0; i < matching; i++) order[i] = 1;
        // Shuffle (Fisher–Yates) so matching/non-matching docs don't cluster by position.
        for (int i = total - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int tmp = order[i];
            order[i] = order[j];
            order[j] = tmp;
        }

        StringBuilder bulk = new StringBuilder(total * 64);
        for (int o : order) {
            String message;
            if (o == 1) {
                message = MATCHING_TEMPLATES[random.nextInt(MATCHING_TEMPLATES.length)];
            } else {
                message = NON_MATCHING_TEMPLATES[random.nextInt(NON_MATCHING_TEMPLATES.length)];
            }
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"message\":\"").append(escapeJson(message)).append("\"}\n");
        }

        Request req = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        req.setJsonEntity(bulk.toString());
        Response resp = client().performRequest(req);
        Map<String, Object> parsed = assertOkAndParse(resp, "bulk index");
        Boolean errors = (Boolean) parsed.get("errors");
        assertFalse("bulk response has errors=" + parsed, Boolean.TRUE.equals(errors));
        return String.format(Locale.ROOT, "bulk(matching=%d, non=%d)", matching, nonMatching);
    }

    private void refresh() throws IOException {
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_refresh"));
    }

    private void flush() throws IOException {
        Request req = new Request("POST", "/" + INDEX_NAME + "/_flush");
        req.addParameter("force", "true");
        client().performRequest(req);
    }

    private void forceMerge(int maxNumSegments) throws IOException {
        Request req = new Request("POST", "/" + INDEX_NAME + "/_forcemerge");
        req.addParameter("max_num_segments", String.valueOf(maxNumSegments));
        client().performRequest(req);
    }

    /**
     * The core assertion: both queries must return {@code expectedMatching}, and they must
     * agree with each other. We assert both separately (not just equality to each other)
     * so a failure message points at which side drifted.
     */
    private void assertMatchAndLikeAgreeOnCount(int iter, long expectedMatching) throws IOException {
        long matchCount = queryScalarCount("source = " + INDEX_NAME + " | where match(message, '" + TARGET + "') | stats count()");
        long likeCount = queryScalarCount("source = " + INDEX_NAME + " | where message LIKE '%" + TARGET + "%' | stats count()");

        String ctx = "iter=" + iter + " expected_matching=" + expectedMatching + " match=" + matchCount + " like=" + likeCount;
        assertEquals("match(message,'" + TARGET + "') count diverged from ground truth (" + ctx + ")", expectedMatching, matchCount);
        assertEquals("message LIKE '%" + TARGET + "%' count diverged from ground truth (" + ctx + ")", expectedMatching, likeCount);
        assertEquals("match and LIKE counts must agree (" + ctx + ")", matchCount, likeCount);
    }

    /**
     * Run a PPL query expected to produce a single scalar count (one row, one column).
     */
    private long queryScalarCount(String ppl) throws IOException {
        Request req = new Request("POST", "/_analytics/ppl");
        req.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Map<String, Object> parsed = assertOkAndParse(client().performRequest(req), "PPL: " + ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) parsed.get("rows");
        assertNotNull("PPL response missing `rows` for: " + ppl, rows);
        assertEquals("PPL scalar count should return exactly 1 row for: " + ppl, 1, rows.size());
        assertEquals("PPL scalar count should return exactly 1 column for: " + ppl, 1, rows.get(0).size());
        Object v = rows.get(0).get(0);
        assertNotNull("PPL scalar count value must not be null for: " + ppl, v);
        return ((Number) v).longValue();
    }
}

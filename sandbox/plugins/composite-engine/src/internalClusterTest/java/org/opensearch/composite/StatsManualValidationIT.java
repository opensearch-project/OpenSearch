/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Manual-validation IT — drives the full stats API surface end-to-end and prints
 * the actual JSON responses to stdout for human verification. Logs a structured
 * report covering: all old per-shard/per-index/per-node counters, the new
 * routing sub-object, rayon/tokio native runtime stats, and filter behavior.
 *
 * <p>Not part of the standard test suite; runs only when explicitly requested:
 * <pre>
 *   ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 *       --tests "org.opensearch.composite.StatsManualValidationIT" \
 *       -Dtests.verbose=true --info
 * </pre>
 *
 * Each println uses a stable header so the output is easy to grep through.
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class StatsManualValidationIT extends BaseStatsIT {

    @SuppressWarnings("unchecked")
    public void testFullManualValidation() throws Exception {
        String idx = "manual-idx";
        createCompositeIndex(idx, true);

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 0 — empty index
        // ─────────────────────────────────────────────────────────────────────────
        printSection("STAGE 0: Empty index — all counters should be 0");
        printResponse("GET /_plugins/parquet/" + idx + "/_stats", parquetIndexStats(idx));
        printResponse("GET /_plugins/lucene/" + idx + "/_stats", luceneIndexStats(idx));

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 1 — index 100 docs (no refresh)
        // ─────────────────────────────────────────────────────────────────────────
        printSection("STAGE 1: Indexed 100 docs (no refresh) — docs_indexed_total=100, refresh_total=0");
        indexDocs(idx, 100, 0);
        Map<String, Object> p1 = parquetIndexStats(idx);
        Map<String, Object> l1 = luceneIndexStats(idx);
        printResponse("parquet", p1);
        printResponse("lucene", l1);
        printAssertion(
            "parquet.indexing.docs_indexed_total == 100",
            getCounter(p1, "indices." + idx + ".indexing.docs_indexed_total") == 100L
        );
        printAssertion(
            "lucene.indexing.docs_indexed_total == 100",
            getCounter(l1, "indices." + idx + ".indexing.docs_indexed_total") == 100L
        );
        printAssertion("lucene.refresh.refresh_total == 0", getCounter(l1, "indices." + idx + ".refresh.refresh_total") == 0L);

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 2 — refresh
        // ─────────────────────────────────────────────────────────────────────────
        printSection("STAGE 2: Refresh — refresh_total=1, native_finalize_total>=1");
        refreshIndex(idx);
        Map<String, Object> p2 = parquetIndexStats(idx);
        Map<String, Object> l2 = luceneIndexStats(idx);
        printResponse("parquet", p2);
        printResponse("lucene", l2);
        printAssertion(
            "parquet.native_write.native_write_total >= 1",
            getCounter(p2, "indices." + idx + ".native_write.native_write_total") >= 1L
        );
        printAssertion(
            "parquet.native_write.native_finalize_total >= 1",
            getCounter(p2, "indices." + idx + ".native_write.native_finalize_total") >= 1L
        );
        printAssertion("lucene.refresh.refresh_total >= 1", getCounter(l2, "indices." + idx + ".refresh.refresh_total") >= 1L);

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 3 — flush
        // ─────────────────────────────────────────────────────────────────────────
        printSection("STAGE 3: Flush — flush_total>=1, commit_total>=1");
        flushIndex(idx);
        Map<String, Object> l3 = luceneIndexStats(idx);
        printResponse("lucene", l3);
        printAssertion("lucene.flush.flush_total >= 1", getCounter(l3, "indices." + idx + ".flush.flush_total") >= 1L);
        printAssertion("lucene.commit.commit_total >= 1", getCounter(l3, "indices." + idx + ".commit.commit_total") >= 1L);

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 4 — index more in 4 batches with refreshes (5 segments total)
        // ─────────────────────────────────────────────────────────────────────────
        printSection("STAGE 4: 4 more batches × 40 docs = 160 more (total=260)");
        for (int batch = 0; batch < 4; batch++) {
            indexDocs(idx, 40, 100 + batch * 40);
            refreshIndex(idx);
        }
        Map<String, Object> p4 = parquetIndexStats(idx);
        Map<String, Object> l4 = luceneIndexStats(idx);
        printResponse("parquet", p4);
        printResponse("lucene", l4);
        printAssertion(
            "parquet.indexing.docs_indexed_total == 260",
            getCounter(p4, "indices." + idx + ".indexing.docs_indexed_total") == 260L
        );
        printAssertion(
            "lucene.indexing.docs_indexed_total == 260",
            getCounter(l4, "indices." + idx + ".indexing.docs_indexed_total") == 260L
        );
        printAssertion(
            "cross-format consistency: parquet == lucene",
            getCounter(p4, "indices." + idx + ".indexing.docs_indexed_total") == getCounter(
                l4,
                "indices." + idx + ".indexing.docs_indexed_total"
            )
        );

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 5 — force_merge to 1 (triggers rayon/tokio paths)
        // ─────────────────────────────────────────────────────────────────────────
        printSection("STAGE 5: force_merge to 1 segment — rayon/tokio counters should engage");
        forceMerge(idx, 1);
        // Parquet merge is async; wait for counters to populate
        assertBusy(() -> {
            Map<String, Object> r = parquetIndexStats(idx);
            assertCounterAtLeast("parquet.merge.merge_total >= 1", r, "indices." + idx + ".merge.merge_total", 1L);
        }, 30, TimeUnit.SECONDS);
        Map<String, Object> p5 = parquetIndexStats(idx);
        Map<String, Object> l5 = luceneIndexStats(idx);
        printResponse("parquet (post-merge)", p5);
        printResponse("lucene (post-merge)", l5);
        printAssertion("parquet.merge.merge_total >= 1", getCounter(p5, "indices." + idx + ".merge.merge_total") >= 1L);
        printAssertion(
            "parquet.merge.merge_input_files_total > 1",
            getCounter(p5, "indices." + idx + ".merge.merge_input_files_total") > 1L
        );
        printAssertion(
            "parquet.merge.merge_output_rows_total == 260",
            getCounter(p5, "indices." + idx + ".merge.merge_output_rows_total") == 260L
        );
        printAssertion("parquet.merge.merge_failures == 0", getCounter(p5, "indices." + idx + ".merge.merge_failures") == 0L);
        // Note: lucene.merge.merge_total stays 0 in composite-engine flow because the
        // lucene secondary uses flush+addIndexes (not standalone IndexWriter.maybeMerge).
        // The lucene-side force-merge work is captured in flush.flush_force_merge_time_millis.
        printAssertion(
            "lucene.flush.flush_force_merge_time_millis > 0 (force-merge ran via flush path)",
            getCounter(l5, "indices." + idx + ".flush.flush_force_merge_time_millis") > 0L
        );
        printAssertion("lucene.merge.merge_failures == 0", getCounter(l5, "indices." + idx + ".merge.merge_failures") == 0L);

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 6 — node-stats: native_runtime block must appear
        // ─────────────────────────────────────────────────────────────────────────
        printSection("STAGE 6: /_nodes/_stats — native_runtime (rayon + tokio) must populate");
        Map<String, Object> nodeStats = parquetNodeStats("");
        printResponse("GET /_plugins/parquet/_nodes/_stats", nodeStats);
        Map<String, Object> nodes = (Map<String, Object>) nodeStats.get("nodes");
        Map<String, Object> firstNode = (Map<String, Object>) nodes.values().iterator().next();
        printAssertion("native_runtime block present at node level", firstNode.containsKey("native_runtime"));
        Map<String, Object> nativeRuntime = (Map<String, Object>) firstNode.get("native_runtime");
        Map<String, Object> rayon = (Map<String, Object>) nativeRuntime.get("rayon");
        Map<String, Object> tokio = (Map<String, Object>) nativeRuntime.get("tokio");
        printAssertion("rayon.configured_threads > 0", ((Number) rayon.get("configured_threads")).longValue() > 0);
        printAssertion("rayon.merge_tasks_submitted >= 1", ((Number) rayon.get("merge_tasks_submitted")).longValue() >= 1);
        printAssertion("rayon.merge_tasks_completed >= 1", ((Number) rayon.get("merge_tasks_completed")).longValue() >= 1);
        printAssertion("rayon.merge_tasks_panicked == 0", ((Number) rayon.get("merge_tasks_panicked")).longValue() == 0);
        printAssertion("tokio.num_workers > 0", ((Number) tokio.get("num_workers")).longValue() > 0);
        printAssertion("tokio.spawned_tasks_total > 0", ((Number) tokio.get("spawned_tasks_total")).longValue() > 0);

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 7 — index-level response must NOT have native_runtime
        // ─────────────────────────────────────────────────────────────────────────
        printSection("STAGE 7: Index-level /{idx}/_stats must NOT contain native_runtime");
        Map<String, Object> indices = (Map<String, Object>) p5.get("indices");
        Map<String, Object> indexBlock = (Map<String, Object>) indices.get(idx);
        printAssertion("index-level response does NOT have native_runtime", !indexBlock.containsKey("native_runtime"));

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 8 — shard-level routing sub-object
        // ─────────────────────────────────────────────────────────────────────────
        printSection("STAGE 8: ?level=shards — verify routing sub-object");
        Map<String, Object> shardLevel = parquetIndexStats(idx, "level", "shards");
        printResponse("parquet ?level=shards", shardLevel);
        Map<String, Object> shards = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) shardLevel.get("indices")).get(
            idx
        )).get("shards");
        Map<String, Object> shard0 = (Map<String, Object>) ((java.util.List<?>) shards.get("0")).get(0);
        Map<String, Object> routing = (Map<String, Object>) shard0.get("routing");
        printAssertion("shards.0[0] has routing sub-object", routing != null);
        printAssertion("routing.primary == true", Boolean.TRUE.equals(routing.get("primary")));
        printAssertion("routing.node is non-null", routing.get("node") != null);
        printAssertion("routing.state is non-null", routing.get("state") != null);

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 9 — filter params
        // ─────────────────────────────────────────────────────────────────────────
        printSection("STAGE 9: ?ignore_unavailable=true with mixed missing/available indices");
        try {
            Map<String, Object> ignoreResp = parquetIndexStats(idx + ",no-such-idx", "ignore_unavailable", "true");
            Map<String, Object> ignoreIdx = (Map<String, Object>) ignoreResp.get("indices");
            printAssertion("ignore_unavailable=true returns 200 with available index", ignoreIdx.containsKey(idx));
            printAssertion("ignore_unavailable=true does NOT include missing index", !ignoreIdx.containsKey("no-such-idx"));
        } catch (Exception e) {
            printAssertion("ignore_unavailable=true did NOT throw", false);
            logger.error("ignore_unavailable threw", e);
        }

        // ─────────────────────────────────────────────────────────────────────────
        // Stage 10 — final summary
        // ─────────────────────────────────────────────────────────────────────────
        printSection("MANUAL VALIDATION COMPLETE — all assertions verified");
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Output helpers — write to stdout via System.out so they're visible in test logs
    // ─────────────────────────────────────────────────────────────────────────────

    private static void printSection(String title) {
        System.out.println();
        System.out.println("════════════════════════════════════════════════════════════════════════");
        System.out.println("  " + title);
        System.out.println("════════════════════════════════════════════════════════════════════════");
    }

    private static void printResponse(String label, Map<String, Object> body) {
        System.out.println();
        System.out.println("    >>> " + label);
        try {
            String json = JsonXContent.contentBuilder().map(body == null ? new LinkedHashMap<>() : body).toString();
            for (String line : json.split("\n")) {
                System.out.println("    " + line);
            }
        } catch (IOException e) {
            System.out.println("    [error rendering body: " + e.getMessage() + "]");
        }
    }

    private static void printAssertion(String label, boolean ok) {
        System.out.println("    " + (ok ? "✅" : "❌") + "  " + label);
        if (!ok) {
            throw new AssertionError("assertion failed: " + label);
        }
    }
}

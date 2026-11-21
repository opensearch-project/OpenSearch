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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;

/**
 * End-to-end query-then-fetch (late materialization) on single-shard indices, checked against an
 * in-memory oracle and against a multi-shard copy of the same data.
 *
 * <p>Three copies of one dataset are provisioned:
 * <ul>
 *   <li>{@code 1shard}: unsorted, one shard. QTF fires with the shard fragment as the query phase.</li>
 *   <li>{@code 1shard_sorted}: {@code index.sort = ts desc}, one shard. QTF is skipped when the sort
 *       is served by the index order and no filter reads another column.</li>
 *   <li>{@code 2shard}: unsorted, two shards. The pre-existing multi-shard QTF path.</li>
 * </ul>
 * Docs are bulk-loaded in shuffled order across several refreshes so each shard has multiple
 * segments and on-disk order differs from {@code ts} order; a fetch that resolved a row id to the
 * wrong document would return the wrong {@code msg}/{@code severity} for a {@code ts}.
 */
public class LateMaterializationSingleShardIT extends AnalyticsRestTestCase {

    private static final String PREFIX = "qtf_1shard_e2e";
    private static final String ONE_SHARD = PREFIX + "_1shard";
    private static final String ONE_SHARD_SORTED = PREFIX + "_1shard_sorted";
    private static final String TWO_SHARD = PREFIX + "_2shard";
    private static final List<String> ALL_INDICES = List.of(ONE_SHARD, ONE_SHARD_SORTED, TWO_SHARD);

    private static final int NUM_DOCS = 300;
    private static final int BATCHES = 4;

    private static boolean provisioned = false;
    private static List<Doc> docs;

    /** One indexed document; every field but {@code ts} is fetch-only in the queries below. */
    private record Doc(long ts, int grp, long val, String severity, String msg) {
        Object field(String name) {
            return switch (name) {
                case "ts" -> ts;
                case "grp" -> grp;
                case "val" -> val;
                case "severity" -> severity;
                case "msg" -> msg;
                default -> throw new IllegalArgumentException(name);
            };
        }
    }

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) return;
        docs = generateDocs();
        createIndex(ONE_SHARD, 1, false);
        createIndex(ONE_SHARD_SORTED, 1, true);
        createIndex(TWO_SHARD, 2, false);
        for (String index : ALL_INDICES) {
            indexDocs(index);
        }
        provisioned = true;
    }

    // ---- correctness: every index returns exactly what the oracle says ----

    public void testSortDescHead() throws IOException {
        assertAllMatchOracle(
            "sort - ts | fields ts, msg, val | head 10",
            d -> true,
            Comparator.comparingLong(Doc::ts).reversed(),
            0,
            10,
            List.of("ts", "msg", "val")
        );
    }

    public void testSortAscHead() throws IOException {
        assertAllMatchOracle(
            "sort ts | fields ts, severity, grp | head 13",
            d -> true,
            Comparator.comparingLong(Doc::ts),
            0,
            13,
            List.of("ts", "severity", "grp")
        );
    }

    public void testFilterOffSortKey() throws IOException {
        // grp is read by the filter, so the sorted single-shard index takes QTF too.
        assertAllMatchOracle(
            "where grp = 3 | sort - ts | fields ts, severity, msg | head 5",
            d -> d.grp() == 3,
            Comparator.comparingLong(Doc::ts).reversed(),
            0,
            5,
            List.of("ts", "severity", "msg")
        );
    }

    public void testFullTextFilterMultiKeySort() throws IOException {
        // val has ties; ts breaks them so the expected order is total.
        assertAllMatchOracle(
            "where match(msg, 'alpha') | sort - val, ts | fields val, ts, msg, severity | head 7",
            d -> d.msg().startsWith("alpha"),
            Comparator.comparingLong(Doc::val).reversed().thenComparingLong(Doc::ts),
            0,
            7,
            List.of("val", "ts", "msg", "severity")
        );
    }

    public void testHeadWithOffset() throws IOException {
        assertAllMatchOracle(
            "sort ts | fields ts, msg | head 5 from 10",
            d -> true,
            Comparator.comparingLong(Doc::ts),
            10,
            5,
            List.of("ts", "msg")
        );
    }

    public void testHeadLargerThanMatchingRows() throws IOException {
        assertAllMatchOracle(
            "where grp = 7 | sort - ts | fields ts, msg | head 1000",
            d -> d.grp() == 7,
            Comparator.comparingLong(Doc::ts).reversed(),
            0,
            1000,
            List.of("ts", "msg")
        );
    }

    public void testNoMatchingRows() throws IOException {
        // K = 0 on the single-shard path: the LM stage gets an empty query phase and must still complete.
        for (String index : ALL_INDICES) {
            List<List<Object>> rows = rows(executePpl("source = " + index + " | where grp = 999 | sort - ts | fields ts, msg | head 5"));
            assertEquals(index + ": no rows expected", 0, rows.size());
        }
    }

    // ---- plan: which indices run the LATE_MATERIALIZATION stage ----

    public void testUnsortedSingleShardRunsLateMaterialization() throws IOException {
        Map<String, Object> lm = lateMaterializationStage(ONE_SHARD, "sort - ts | fields ts, msg, val | head 10");
        assertNotNull("single-shard unsorted index must run the LATE_MATERIALIZATION stage", lm);
        assertTaskCount(lm, 1);
    }

    public void testSortedSingleShardSkipsLateMaterializationWhenIndexOrderServesSort() throws IOException {
        // index.sort = ts desc; ORDER BY ts DESC and ORDER BY ts ASC (reverse scan) are both served.
        assertNull(lateMaterializationStage(ONE_SHARD_SORTED, "sort - ts | fields ts, msg, val | head 10"));
        assertNull(lateMaterializationStage(ONE_SHARD_SORTED, "sort ts | fields ts, msg, val | head 10"));
    }

    public void testSortedSingleShardRunsLateMaterializationWithFilterOffSortKey() throws IOException {
        Map<String, Object> lm = lateMaterializationStage(ONE_SHARD_SORTED, "where grp = 3 | sort - ts | fields ts, msg | head 5");
        assertNotNull("a filter on a non-sort column must keep QTF on a sorted single shard", lm);
        assertTaskCount(lm, 1);
    }

    public void testSortedSingleShardRunsLateMaterializationWhenSortKeyIsNotIndexSort() throws IOException {
        assertNotNull(lateMaterializationStage(ONE_SHARD_SORTED, "sort val | fields val, msg | head 5"));
    }

    public void testMultiShardRunsLateMaterializationRegardlessOfSort() throws IOException {
        assertNotNull(lateMaterializationStage(TWO_SHARD, "sort - ts | fields ts, msg, val | head 10"));
    }

    // ---- helpers ----

    private void assertAllMatchOracle(
        String pipeline,
        Predicate<Doc> filter,
        Comparator<Doc> order,
        int offset,
        int limit,
        List<String> fields
    ) throws IOException {
        List<List<String>> expected = docs.stream()
            .filter(filter)
            .sorted(order)
            .skip(offset)
            .limit(limit)
            .map(d -> fields.stream().map(f -> String.valueOf(d.field(f))).toList())
            .toList();
        for (String index : ALL_INDICES) {
            Map<String, Object> result = executePpl("source = " + index + " | " + pipeline);
            List<String> columns = extractColumnNames(result);
            assertEquals(index + ": columns", fields, columns);
            List<List<String>> actual = rows(result).stream().map(r -> r.stream().map(LateMaterializationSingleShardIT::norm).toList()).toList();
            assertEquals(index + ": rows for `" + pipeline + "`", expected, actual);
        }
    }

    private static String norm(Object v) {
        // JSON numbers may parse as Integer, Long or Double depending on magnitude; the dataset is integral.
        if (v instanceof Number n) return String.valueOf(n.longValue());
        return String.valueOf(v);
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> rows(Map<String, Object> result) {
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("datarows present: " + result, rows);
        return rows;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> lateMaterializationStage(String index, String pipeline) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson("source = " + index + " | " + pipeline) + "\", \"profile\": true}");
        Map<String, Object> result = assertOkAndParse(client().performRequest(request), "PROFILE: " + pipeline);
        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        for (Map<String, Object> stage : (List<Map<String, Object>>) profile.get("stages")) {
            if ("LATE_MATERIALIZATION".equals(stage.get("execution_type"))) {
                Object state = stage.get("state");
                assertTrue("LM stage must not fail: " + stage, "SUCCEEDED".equals(state) || "CANCELLED".equals(state));
                return stage;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static void assertTaskCount(Map<String, Object> lmStage, int expected) {
        List<Map<String, Object>> tasks = (List<Map<String, Object>>) lmStage.get("tasks");
        assertNotNull("LM stage tasks: " + lmStage, tasks);
        assertEquals("LM fetch tasks (one per participating shard): " + lmStage, expected, tasks.size());
    }

    private static List<Doc> generateDocs() {
        Random random = new Random(42);
        List<Doc> out = new ArrayList<>(NUM_DOCS);
        String[] severities = { "INFO", "WARN", "ERROR", "DEBUG" };
        for (int i = 0; i < NUM_DOCS; i++) {
            long ts = 1_700_000_000_000L + i * 1_000L;
            int grp = i % 10;
            long val = random.nextInt(40);
            String severity = severities[random.nextInt(severities.length)];
            String msg = (i % 3 == 0 ? "alpha" : "beta") + " event " + i + " payload " + Long.toHexString(random.nextLong());
            out.add(new Doc(ts, grp, val, severity, msg));
        }
        return out;
    }

    private void createIndex(String index, int shards, boolean sortedByTsDesc) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        String sort = sortedByTsDesc ? ", \"index.sort.field\": \"ts\", \"index.sort.order\": \"desc\"" : "";
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + shards + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + sort
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"ts\": { \"type\": \"long\" },"
            + "    \"grp\": { \"type\": \"integer\" },"
            + "    \"val\": { \"type\": \"long\" },"
            + "    \"severity\": { \"type\": \"keyword\" },"
            + "    \"msg\": { \"type\": \"text\" }"
            + "  }"
            + "}"
            + "}";
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(body);
        assertOkAndParse(client().performRequest(create), "Create index " + index);
    }

    private void indexDocs(String index) throws IOException {
        List<Doc> shuffled = new ArrayList<>(docs);
        Collections.shuffle(shuffled, new Random(7));
        int perBatch = (shuffled.size() + BATCHES - 1) / BATCHES;
        for (int b = 0; b < BATCHES; b++) {
            StringBuilder bulk = new StringBuilder();
            for (Doc d : shuffled.subList(b * perBatch, Math.min(shuffled.size(), (b + 1) * perBatch))) {
                bulk.append("{\"index\":{\"_index\":\"").append(index).append("\"}}\n");
                bulk.append("{\"ts\":").append(d.ts())
                    .append(",\"grp\":").append(d.grp())
                    .append(",\"val\":").append(d.val())
                    .append(",\"severity\":\"").append(d.severity())
                    .append("\",\"msg\":\"").append(d.msg())
                    .append("\"}\n");
            }
            Request req = new Request("POST", "/_bulk");
            req.addParameter("refresh", "true");
            req.setJsonEntity(bulk.toString());
            Response resp = client().performRequest(req);
            Map<String, Object> parsed = assertOkAndParse(resp, "Bulk " + index);
            assertEquals("bulk errors for " + index + ": " + parsed, Boolean.FALSE, parsed.get("errors"));
            // Flush per batch so every shard ends up with several segments (row ids span segments).
            client().performRequest(new Request("POST", "/" + index + "/_flush?force=true"));
        }
    }
}

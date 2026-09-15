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
import java.util.List;
import java.util.Map;

/**
 * End-to-end correctness of PPL {@code sort <col> | head N} — the shape whose lowering has been
 * wrong twice, in two independent places.
 *
 * <p>Every dataset here has distinct ascending ids, so {@code sort id | head N} has exactly ONE
 * correct answer: ids {@code 0..N-1} in ascending order. Tests assert that exact prefix rather than
 * mere run-to-run stability — a stable-but-wrong answer is still a bug.
 *
 * <h2>Regression 1 — QTF root stage returned zero rows</h2>
 * An <em>unprojected</em> {@code source = idx | sort <col> | head N} on a multi-shard index returned
 * 0 rows (HTTP 200, no error). That shape makes the QTF / late-materialization rewriter fire — it
 * fires only when a projected column is absent from the sort key, and only when there is an
 * ExchangeReducer to anchor on, hence multi-shard only — and leaves
 * {@code OpenSearchLateMaterialization} as the plan ROOT. {@code Stitcher.finish} then closed its
 * {@code parentSink}, which for a root stage is the {@code RowProducingSink} holding the answer:
 * {@code close()} freed and cleared every batch before {@code QueryExecution} read it. Adding
 * {@code | fields …} anywhere makes the skip predicate decline, which is why the pre-existing ITs
 * ({@code SortPushdownIT}, {@code LateMaterializationDateNanosIT}) never caught it.
 *
 * <h2>Regression 2 — Sort dropped under a Fetch</h2>
 * {@code join | sort L.id | head 100} returned a different 100 rows on every run — even on a single
 * coordinator-centric path, so this was never an MPP defect.
 * {@code DataFusionFragmentConvertor.replaceInput} rewired {@code Fetch.input = newInput} and
 * dropped the {@code Sort} beneath it (a Calcite {@code LogicalSort} carrying both a collation and a
 * fetch lowers to {@code Fetch(Sort(input))}), so {@code head N} returned N arrival-order rows and
 * which rows arrived first varied with shard scheduling. Fixed by #21912; kept here so it stays
 * fixed.
 */
public class PplSortHeadCorrectnessIT extends AnalyticsRestTestCase {

    private static final String LEFT_INDEX = "sh_corr_left";
    private static final String RIGHT_INDEX = "sh_corr_right";
    private static final int SHARDS = 5;
    private static final int ROW_COUNT = 5_000;

    /** Shard counts swept by the QTF test: 1 proves the no-exchange path, >1 exercises QTF. */
    private static final List<Integer> SHARD_COUNTS = List.of(1, 2, 3, 5);

    private static final String TWO_COL_MAPPING = "{\"id\": {\"type\": \"integer\"}, \"amount\": {\"type\": \"integer\"}}";

    private static boolean dataProvisioned = false;

    private static final String JOIN = "source = " + LEFT_INDEX + " | inner join left=L right=R on L.id = R.id " + RIGHT_INDEX;

    @Override
    public void tearDown() throws Exception {
        resetSetting("analytics.mpp.enabled");
        resetSetting("analytics.mpp.distribute.min_rows");
        resetSetting("analytics.mpp.broadcast.probe_estimate");
        super.tearDown();
    }

    // ─── regression 1: QTF / late-materialization root stage ──────────────────

    /**
     * The bug: unprojected {@code sort id | head N} returned 0 rows on any index with more than one
     * shard. Sweeps shard count × fetch size — the answer must be the exact prefix in every cell.
     */
    public void testUnprojectedSortHead_isExactPrefixAtEveryShardCount() throws IOException {
        setMpp(false);
        for (int shards : SHARD_COUNTS) {
            String index = "sh_corr_qtf_s" + shards;
            createParquetIndex(index, shards, TWO_COL_MAPPING);
            bulkAndRefresh(index, bulkBodyFor(500));
            for (int fetch : List.of(1, 4, 100)) {
                String ppl = "source = " + index + " | sort id | head " + fetch;
                assertEquals(
                    "shards=" + shards + " fetch=" + fetch + ": `" + ppl + "` must return ids 0.." + (fetch - 1),
                    expectedPrefix(fetch),
                    idsOf(executePplRows(ppl))
                );
            }
        }
    }

    /**
     * Descending mirror plus the two {@code fields} placements. These already passed before the fix
     * (an explicit projection makes the QTF skip predicate decline, or reverses the prefix), so they
     * pin the neighbours of the failing shape and would catch a fix that broke them.
     */
    public void testSortHeadNeighbourShapes_multiShard() throws IOException {
        setMpp(false);
        String index = "sh_corr_neighbours";
        createParquetIndex(index, SHARDS, TWO_COL_MAPPING);
        bulkAndRefresh(index, bulkBodyFor(500));

        assertEquals(
            "fields before sort: QTF declines, still the exact prefix",
            expectedPrefix(10),
            idsOf(executePplRows("source = " + index + " | fields id | sort id | head 10"))
        );
        assertEquals(
            "fields after head: projection pushdown narrows the scan, still the exact prefix",
            expectedPrefix(10),
            idsOf(executePplRows("source = " + index + " | sort id | head 10 | fields id"))
        );
        assertEquals(
            "descending: the exact suffix, reversed",
            expectedSuffixDescending(500, 10),
            idsOf(executePplRows("source = " + index + " | sort - id | head 10"))
        );
        assertEquals(
            "head larger than the dataset returns every row, fully sorted",
            expectedPrefix(500),
            idsOf(executePplRows("source = " + index + " | sort id | head 1000"))
        );
    }

    // ─── regression 2: join | sort | head ─────────────────────────────────────

    /** The same coord-centric query must give the same exact prefix on every run. */
    public void testJoinSortHead_coordCentricIsExactPrefixEveryRun() throws IOException {
        ensureJoinDataProvisioned();
        setMpp(false);
        List<List<Integer>> runs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            runs.add(idsOf(executePplRows(JOIN + " | sort L.id | head 100")));
        }
        assertEquals("run0 == run1 (determinism)", runs.get(0), runs.get(1));
        assertEquals("run1 == run2 (determinism)", runs.get(1), runs.get(2));
        assertEquals("coord-centric top-100 must be ids 0..99 ascending", expectedPrefix(100), runs.get(0));
    }

    /** {@code sort} with no {@code head} must agree on both paths and be fully ordered. */
    public void testJoinSortWithoutHead_coordAndShuffleAgree() throws IOException {
        ensureJoinDataProvisioned();
        setMpp(false);
        List<Integer> coord = idsOf(executePplRows(JOIN + " | sort L.id"));
        setMpp(true);
        List<Integer> shuffle = idsOf(executePplRows(JOIN + " | sort L.id"));
        assertEquals("coord returns all rows", ROW_COUNT, coord.size());
        assertEquals("coord list == shuffle list", coord, shuffle);
        assertEquals("fully sorted 0..4999", expectedPrefix(ROW_COUNT), coord);
    }

    /** {@code head N} where N == total is a no-op fetch and must not perturb the order. */
    public void testJoinSortHeadEqualToTotal_preservesOrderOnBothPaths() throws IOException {
        ensureJoinDataProvisioned();
        setMpp(false);
        List<Integer> coord = idsOf(executePplRows(JOIN + " | sort L.id | head " + ROW_COUNT));
        setMpp(true);
        List<Integer> shuffle = idsOf(executePplRows(JOIN + " | sort L.id | head " + ROW_COUNT));
        assertEquals("coord list == shuffle list", coord, shuffle);
        assertEquals("coord fully sorted 0..4999", expectedPrefix(ROW_COUNT), coord);
    }

    /** The original cross-path failure — top-100 must match on coord-centric and hash-shuffle. */
    public void testJoinSortHead_coordAndShuffleAgree() throws IOException {
        ensureJoinDataProvisioned();
        setMpp(false);
        List<Integer> coord = idsOf(executePplRows(JOIN + " | sort L.id | head 100"));
        setMpp(true);
        List<Integer> shuffle = idsOf(executePplRows(JOIN + " | sort L.id | head 100"));
        assertEquals("coord list == shuffle list", coord, shuffle);
        assertEquals("top-100 must be ids 0..99 ascending", expectedPrefix(100), coord);
    }

    // ─── helpers ──────────────────────────────────────────────────────────────

    private static List<Integer> expectedPrefix(int n) {
        List<Integer> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(i);
        return out;
    }

    /** Top {@code n} of {@code total} ascending ids, taken descending: {@code total-1 … total-n}. */
    private static List<Integer> expectedSuffixDescending(int total, int n) {
        List<Integer> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(total - 1 - i);
        return out;
    }

    /** Projects the {@code id} column (the sort key) out of a PPL result. */
    private static List<Integer> idsOf(PplResult result) {
        int column = result.idColumn();
        List<Integer> ids = new ArrayList<>(result.rows().size());
        for (List<Object> row : result.rows()) {
            ids.add(((Number) row.get(column)).intValue());
        }
        return ids;
    }

    /** Bulk body of {@code n} docs with distinct ascending ids. */
    private static String bulkBodyFor(int n) {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < n; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"id\":").append(i).append(",\"amount\":").append((i + 1) * 10).append("}\n");
        }
        return bulk.toString();
    }

    /**
     * MPP gate plus the two overrides the join steps need: the IT dataset sits below the production
     * distribute floor, and a 2-node cluster makes broadcast cheap enough to beat hash-shuffle.
     */
    private void setMpp(boolean enabled) throws IOException {
        applySetting("analytics.mpp.enabled", String.valueOf(enabled));
        applySetting("analytics.mpp.distribute.min_rows", "1");
        applySetting("analytics.mpp.broadcast.probe_estimate", "20");
    }

    private void ensureJoinDataProvisioned() throws IOException {
        if (dataProvisioned) return;
        createParquetIndex(LEFT_INDEX, SHARDS, TWO_COL_MAPPING);
        bulkAndRefresh(LEFT_INDEX, bulkBodyFor(ROW_COUNT));

        createParquetIndex(RIGHT_INDEX, SHARDS, "{\"id\": {\"type\": \"integer\"}, \"category\": {\"type\": \"keyword\"}}");
        StringBuilder rightBulk = new StringBuilder();
        for (int i = 0; i < ROW_COUNT; i++) {
            rightBulk.append("{\"index\":{}}\n");
            rightBulk.append("{\"id\":").append(i).append(",\"category\":\"cat-").append(i % 4).append("\"}\n");
        }
        bulkAndRefresh(RIGHT_INDEX, rightBulk.toString());
        dataProvisioned = true;
    }

    private void createParquetIndex(String name, int shards, String mappingProperties) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + shards + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": { \"properties\": " + mappingProperties + " }"
            + "}";
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "Create index " + name);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + name);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("timeout", "60s");
        client().performRequest(health);
    }

    private void bulkAndRefresh(String indexName, String bulkBody) throws IOException {
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(bulkBody);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        client().performRequest(bulkRequest);
        client().performRequest(new Request("POST", "/" + indexName + "/_flush?force=true"));
    }

    /** Column names + rows from the {@code /_analytics/ppl} shim. */
    private record PplResult(List<String> columns, List<List<Object>> rows) {
        /** Index of the {@code id} column — the join/sort key the assertions read. */
        int idColumn() {
            int i = columns.indexOf("id");
            return i >= 0 ? i : 0;
        }
    }

    @SuppressWarnings("unchecked")
    private PplResult executePplRows(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        Map<String, Object> body = assertOkAndParse(response, "PPL: " + ppl);
        List<List<Object>> rows = (List<List<Object>>) body.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, rows);
        List<String> columns = new ArrayList<>();
        Object rawColumns = body.get("columns") != null ? body.get("columns") : body.get("schema");
        if (rawColumns instanceof List<?> entries) {
            for (Object entry : entries) {
                if (entry instanceof Map<?, ?> m) {
                    columns.add(String.valueOf(m.get("name")));
                } else {
                    columns.add(String.valueOf(entry));
                }
            }
        }
        return new PplResult(columns, rows);
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

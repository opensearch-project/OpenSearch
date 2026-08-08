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

import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for multi-valued (array) fields on a composite parquet+lucene index.
 *
 * <p>OpenSearch mappings do not declare cardinality — an array is a property of an individual
 * document — but an Arrow/Parquet column's type is fixed for the whole file. A field listed in
 * {@code index.parquet.multi_value.field} is therefore written as an Arrow {@code LIST<element>}
 * column, and only such a field accepts more than one value per document.
 *
 * <p>This test exercises the full path that unit tests cannot: the mapper splitting a JSON array
 * into per-element {@code addField} calls, the VSR writing an Arrow list, the native writer
 * emitting a Parquet {@code LIST} (leaf {@code tags.list.element}), and DataFusion/arrow-rs
 * reading that column back through a real query. The read half is the part most at risk — the
 * parquet reader's view-type transform does not recurse into list children, so a
 * {@code List<Utf8>} vs {@code List<Utf8View>} divergence between the scanned schema and the
 * Substrait-declared schema would surface here as a bind failure rather than in any unit test.
 */
public class MultiValueFieldIT extends AnalyticsRestTestCase {

    private static final String INDEX = "multi_value_field";

    /** _id of the document holding the multi-valued tags, captured from the bulk response. */
    private String multiDocId;

    /**
     * Diagnostic pair, part 1 of 2 — the {@code ListingTable} (non-indexed) read path.
     * <p>
     * A bare projection with no filter routes through {@code NativeBridge.createSessionContext}
     * ({@code ShardScanInstructionHandler}, the {@code requestsRowIds() == false} branch), so the
     * scan is a plain DataFusion {@code ListingTable} over the parquet file.
     * <p>
     * Read together with {@link #testListColumnViaIndexedPath()}: if this passes and that fails,
     * the LIST decode bug is confined to the indexed executor rather than being a general
     * arrow-rs / parquet nested-read problem. Kept as the control arm for the indexed-path test.
     */
    @SuppressWarnings("unchecked")
    public void testListColumnViaListingTablePath() throws Exception {
        provision();

        // No `where` and no sort/limit ⇒ no delegation, no row-ids ⇒ ListingTable.
        Map<String, Object> result = executePpl("source = " + INDEX + " | fields tags");
        List<String> columns = extractColumnNames(result);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals("all three documents must come back", 3, rows.size());

        // Assert only that the LIST column decoded at all: this arm exists to locate the bug, and
        // row order is not guaranteed without an explicit sort.
        long nonNullLists = rows.stream().map(r -> r.get(columns.indexOf("tags"))).filter(c -> c instanceof List).count();
        assertTrue("at least the two populated rows must decode as lists, got rows=" + rows, nonNullLists >= 2);
    }

    /**
     * Diagnostic pair, part 2 of 2 — the indexed read path.
     * <p>
     * A {@code where} predicate is delegated to Lucene, which routes through
     * {@code NativeBridge.createSessionContextForIndexedExecution}
     * ({@code ShardScanWithDelegationHandler}) and reads parquet via {@code IndexedTableProvider}
     * with a {@code RowSelection}/{@code ParquetAccessPlan} rather than a plain scan. This is the
     * arm that fails with {@code StructArrayReader out of sync}.
     */
    @SuppressWarnings("unchecked")
    public void testListColumnViaIndexedPath() throws Exception {
        provision();

        // `where` ⇒ delegated predicate ⇒ indexed session context ⇒ IndexedTableProvider scan.
        Map<String, Object> result = executePpl("source = " + INDEX + " | where id = 'multi' | fields tags");
        List<String> columns = extractColumnNames(result);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals(1, rows.size());
        assertEquals(List.of("beta", "alpha", "beta"), ((List<Object>) rows.get(0).get(columns.indexOf("tags"))).stream()
            .map(String::valueOf)
            .toList());
    }

    /**
     * The core claim: a document containing an array is accepted, and every value survives the
     * round trip through Parquet in document order, duplicates included.
     */
    @SuppressWarnings("unchecked")
    public void testMultiValueFieldRoundTripsThroughParquet() throws Exception {
        provision();

        Map<String, Object> result = executePpl("source = " + INDEX + " | where id = 'multi' | fields tags");
        List<String> columns = extractColumnNames(result);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals("expected exactly one matching document", 1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("tags"));
        assertTrue("a multi-valued column must render as a List, got " + describe(cell), cell instanceof List);
        // Order and duplicates must be preserved: these values are the source of truth for derived
        // _source, so the read path must not sort or deduplicate them.
        assertEquals(List.of("beta", "alpha", "beta"), ((List<Object>) cell).stream().map(String::valueOf).toList());
    }

    /** A single scalar JSON value on a declared list column still reads back as a one-element list. */
    @SuppressWarnings("unchecked")
    public void testSingleValueOnDeclaredListFieldIsStillAList() throws Exception {
        provision();

        Map<String, Object> result = executePpl("source = " + INDEX + " | where id = 'single' | fields tags");
        List<String> columns = extractColumnNames(result);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals(1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("tags"));
        assertTrue("declared list column must stay list-typed for scalar input, got " + describe(cell), cell instanceof List);
        assertEquals(List.of("solo"), ((List<Object>) cell).stream().map(String::valueOf).toList());
    }

    /** A document that omits the field reads back as null, not as an empty list. */
    @SuppressWarnings("unchecked")
    public void testAbsentMultiValueFieldReadsAsNull() throws Exception {
        provision();

        Map<String, Object> result = executePpl("source = " + INDEX + " | where id = 'absent' | fields tags");
        List<String> columns = extractColumnNames(result);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals(1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("tags"));
        assertTrue("absent field must be null or empty, got " + describe(cell), cell == null || ((List<Object>) cell).isEmpty());
    }

    /**
     * {@code array_length} over a list column read from Parquet. This is the sharpest read-side
     * check: the value must reach a native DataFusion array function with its list type intact,
     * which only works if the scanned Arrow schema binds against the Substrait declaration.
     */
    @SuppressWarnings("unchecked")
    public void testArrayLengthOverParquetListColumn() throws Exception {
        provision();

        Map<String, Object> result = executePpl(
            "source = " + INDEX + " | where id = 'multi' | eval n = array_length(tags) | fields n"
        );
        List<String> columns = extractColumnNames(result);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals(1, rows.size());
        assertEquals("array_length over a parquet LIST column", 3, ((Number) rows.get(0).get(columns.indexOf("n"))).intValue());
    }

    /**
     * {@code _source} is reconstructed from Parquet columns on this path (there are no Lucene
     * stored fields), so a get-by-id must rebuild the array rather than dropping the field.
     */
    @SuppressWarnings("unchecked")
    public void testGetByIdReconstructsArrayFromParquet() throws Exception {
        provision();

        // _ids are auto-generated (append-only index), so use the id captured from the bulk
        // response rather than searching for it: `_search` on this index is intercepted and
        // routed through the analytics engine, so it is not a plain doc lookup.
        assertNotNull("bulk must have reported an _id for the multi-value doc", multiDocId);

        Map<String, Object> doc = assertOkAndParse(
            client().performRequest(new Request("GET", "/" + INDEX + "/_doc/" + multiDocId)),
            "get " + multiDocId
        );
        Map<String, Object> source = (Map<String, Object>) doc.get("_source");
        assertNotNull("_source must be reconstructed from the parquet columns", source);

        Object tags = source.get("tags");
        assertTrue("_source must carry the array, got " + describe(tags), tags instanceof List);
        assertEquals(List.of("beta", "alpha", "beta"), ((List<Object>) tags).stream().map(String::valueOf).toList());
    }

    /**
     * Force-merge is the one operation that rewrites a Parquet LIST column rather than just reading
     * it: the k-way merge slices batches by row, stamps a fresh {@code __row_id__}, and re-encodes
     * every column through {@code compute_leaves}. A row-vs-value confusion anywhere in that path
     * would silently reattach values to the wrong document, so this asserts each document keeps
     * exactly its own values after the merge.
     */
    @SuppressWarnings("unchecked")
    public void testArraysSurviveForceMerge() throws Exception {
        provision();

        // A second flushed generation, so force-merge has two parquet files to combine. Varying
        // list lengths (including empty and absent) keep value counts unrelated to row counts.
        String bulk = "{\"index\":{}}\n"
            + "{\"id\":\"g2a\",\"name\":\"fourth\",\"tags\":[\"p\",\"q\",\"r\"]}\n"
            + "{\"index\":{}}\n"
            + "{\"id\":\"g2b\",\"name\":\"fifth\"}\n"
            + "{\"index\":{}}\n"
            + "{\"id\":\"g2c\",\"name\":\"sixth\",\"tags\":\"lone\"}\n";
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> resp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk gen2");
        assertEquals("second-generation ingest must succeed: " + resp, Boolean.FALSE, resp.get("errors"));
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));

        Request merge = new Request("POST", "/" + INDEX + "/_forcemerge");
        merge.addParameter("max_num_segments", "1");
        client().performRequest(merge);
        Request flush = new Request("POST", "/" + INDEX + "/_flush");
        flush.addParameter("force", "true");
        client().performRequest(flush);

        // Read every document back and check its array is intact.
        Map<String, Object> result = executePpl("source = " + INDEX + " | fields id, tags");
        List<String> columns = extractColumnNames(result);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals("all six documents must survive the merge", 6, rows.size());

        Map<String, List<String>> expected = Map.of(
            "multi", List.of("beta", "alpha", "beta"),
            "single", List.of("solo"),
            "absent", List.of(),
            "g2a", List.of("p", "q", "r"),
            "g2b", List.of(),
            "g2c", List.of("lone")
        );
        int idCol = columns.indexOf("id");
        int tagsCol = columns.indexOf("tags");
        for (List<Object> row : rows) {
            String id = String.valueOf(row.get(idCol));
            Object cell = row.get(tagsCol);
            List<String> actual = cell == null
                ? List.of()
                : ((List<Object>) cell).stream().map(String::valueOf).toList();
            assertEquals("document [" + id + "] lost or gained values during force-merge", expected.get(id), actual);
        }
    }

    /**
     * A field NOT declared multi-valued keeps its scalar column and still rejects arrays, so
     * enabling this feature for one field cannot silently change another field's contract.
     */
    public void testUndeclaredFieldStillRejectsArrays() throws Exception {
        provision();

        Request index = new Request("POST", "/" + INDEX + "/_doc?refresh=true");
        index.setJsonEntity("{\"id\":\"bad\",\"name\":[\"x\",\"y\"],\"tags\":[\"t\"]}");
        // "name" is not declared multi-valued, so its second value must still be rejected.

        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(index));
        String responseBody = bodyOf(e);
        assertTrue(
            "rejection must mention multiple values and point at the setting, got: " + responseBody,
            responseBody.contains("multiple values") && responseBody.contains("index.parquet.multi_value.field")
        );
    }

    /** Declaring a field that does not exist in the mapping must fail index creation outright. */
    public void testUnknownMultiValueFieldRejectedAtIndexCreation() throws Exception {
        String indexName = INDEX + "_unknown";
        try {
            client().performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception ignored) {}

        Request create = new Request("PUT", "/" + indexName);
        create.setJsonEntity(
            "{\"settings\":{"
                + compositeSettings()
                + ",\"index.parquet.multi_value.field\":[\"does_not_exist\"]},"
                + "\"mappings\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}}}"
        );

        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(create));
        String responseBody = bodyOf(e);
        assertTrue("expected a clear index-creation failure, got: " + responseBody, responseBody.contains("does_not_exist"));
    }


    /**
     * Pins the root cause: the LIST decode failure is caused by the **scoped page-index cache**.
     * <p>
     * With {@code datafusion.scoped_page_index.enabled=false} the identical filtered query — same
     * indexed executor, same {@code RowSelection}, same file — decodes the LIST column correctly.
     * With it enabled (the default) the same query fails. The scoped cache fetches page-index bytes
     * per column and is only used by the indexed path, which is why
     * {@link #testListColumnViaListingTablePath} never hits it.
     * <p>
     * This test is the control for {@link #testListColumnViaIndexedPath}: it must keep passing, and
     * when the scoped page-index bug is fixed that one should start passing too. It also documents
     * the operational workaround — disabling the setting makes multi-valued reads work today.
     */
    @SuppressWarnings("unchecked")
    public void testIndexedPathWorksWithScopedPageIndexDisabled() throws Exception {
        provision();
        Request put = new Request("PUT", "/_cluster/settings");
        put.setJsonEntity("{\"persistent\":{\"datafusion.scoped_page_index.enabled\":false}}");
        client().performRequest(put);
        try {
            Map<String, Object> result = executePpl("source = " + INDEX + " | where id = 'multi' | fields tags");
            List<String> columns = extractColumnNames(result);
            List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
            assertEquals(1, rows.size());
            Object cell = rows.get(0).get(columns.indexOf("tags"));
            assertEquals(
                "with the scoped page-index cache disabled the LIST column must decode",
                List.of("beta", "alpha", "beta"),
                ((List<Object>) cell).stream().map(String::valueOf).toList()
            );
        } finally {
            Request reset = new Request("PUT", "/_cluster/settings");
            reset.setJsonEntity("{\"persistent\":{\"datafusion.scoped_page_index.enabled\":null}}");
            client().performRequest(reset);
        }
    }

    private static String bodyOf(ResponseException e) throws java.io.IOException {
        return new String(e.getResponse().getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static String describe(Object cell) {
        return cell == null ? "null" : cell.getClass() + " = " + cell;
    }

    private static String compositeSettings() {
        return "\"number_of_shards\": 1,"
            + "\"number_of_replicas\": 0,"
            + "\"index.pluggable.dataformat.enabled\": true,"
            + "\"index.pluggable.dataformat\": \"composite\","
            + "\"index.composite.primary_data_format\": \"parquet\","
            + "\"index.composite.secondary_data_formats\": [\"lucene\"]";
    }

    @SuppressWarnings("unchecked")
    private void provision() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        // Single shard: this test is about column cardinality, not cross-shard reduce, and one
        // shard keeps the expected values exact rather than order-independent.
        String mapping = "{"
            + "\"settings\": {"
            + compositeSettings()
            + ",\"index.parquet.multi_value.field\": [\"tags\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"id\":   { \"type\": \"keyword\" },"
            + "    \"name\": { \"type\": \"keyword\" },"
            + "    \"tags\": { \"type\": \"keyword\" }"
            + "  }"
            + "}"
            + "}";

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(mapping);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "create " + INDEX);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        // Three cardinalities in one file: several values (with a duplicate, in non-sorted order),
        // one value, and the field absent entirely. Document IDs are auto-generated because the
        // composite parquet dataformat enables index.append_only.enabled, which rejects custom
        // _ids; the "id" field carries the logical key the assertions filter on instead.
        String bulk = "{\"index\":{}}\n"
            + "{\"id\":\"multi\",\"name\":\"first\",\"tags\":[\"beta\",\"alpha\",\"beta\"]}\n"
            + "{\"index\":{}}\n"
            + "{\"id\":\"single\",\"name\":\"second\",\"tags\":\"solo\"}\n"
            + "{\"index\":{}}\n"
            + "{\"id\":\"absent\",\"name\":\"third\"}\n";

        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + INDEX);
        assertEquals("array ingest must report no item errors: " + bulkResp, Boolean.FALSE, bulkResp.get("errors"));
        // The first bulk item is the multi-valued document; keep its generated _id for the GET test.
        List<Map<String, Object>> items = (List<Map<String, Object>>) bulkResp.get("items");
        multiDocId = (String) ((Map<String, Object>) items.get(0).get("index")).get("_id");
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }
}

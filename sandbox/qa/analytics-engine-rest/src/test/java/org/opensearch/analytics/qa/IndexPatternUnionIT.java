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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for an index pattern (or alias) that resolves to parquet-backed indices
 * with <em>differing field sets</em>. The scan's row type is the union of all fields, but no
 * single backing index declares all of them.
 *
 * <p>Two failures had to be fixed for this to work, and this IT guards both:
 * <ul>
 *   <li><b>Coordinator planning</b> — {@code OpenSearchTableScanRule} resolved field storage
 *   from one index, throwing {@code Field [age] not found} when that index omitted a union field.
 *   Now field storage is merged across all concrete indices.</li>
 *   <li><b>Data-node execution</b> — a shard whose parquet omits a referenced column blew up,
 *   since the table was registered from the inferred file schema. The data node now reconciles
 *   against the plan's union base_schema and null-fills the absent columns.</li>
 * </ul>
 *
 * <p>Mirrors {@code CalcitePPLBasicIT.testIndexPatterns} in the SQL plugin, but runs end-to-end
 * against the analytics-engine route with the real DataFusion backend.
 */
public class IndexPatternUnionIT extends AnalyticsRestTestCase {

    private static final String INDEX_A = "union_a";  // {name, age}
    private static final String INDEX_B = "union_b";  // {name, alias}
    private static final String ALIAS = "union_alias";

    private static boolean provisioned = false;

    private void ensureProvisioned() throws IOException {
        if (provisioned) {
            return;
        }
        // `name` mapped as text with a .keyword multi-field — mirrors dynamic mapping (what
        // CalcitePPLBasicIT.testIndexPatterns gets), which adds an extra `name.keyword` column to
        // the parquet schema. This is the case that exposed the column-order swap.
        String nameMultiField = "\"name\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}}";
        createParquetIndex(INDEX_A, "{\"properties\":{" + nameMultiField + ",\"age\":{\"type\":\"long\"}}}");
        createParquetIndex(INDEX_B, "{\"properties\":{" + nameMultiField + ",\"alias\":{\"type\":\"keyword\"}}}");
        bulk(INDEX_A, "{\"name\":\"hello\",\"age\":20}\n{\"name\":\"world\",\"age\":30}\n");
        bulk(INDEX_B, "{\"name\":\"HELLO\",\"alias\":\"Hello\"}\n");
        putAlias(ALIAS, List.of(INDEX_A, INDEX_B));
        provisioned = true;
    }

    /**
     * {@code source=union_alias} projects the union {name, age, alias}. Each backing index serves
     * the fields it has and null-fills the rest: {@code union_a} rows have a null alias,
     * {@code union_b} rows have a null age. Without the union field-storage merge this fails at
     * planning; without the data-node null-fill it fails at the parquet scan.
     *
     * <p>The analytics PPL route registers concrete indices and aliases as Calcite tables but not
     * wildcard patterns, so an alias is the route's vehicle for a multi-index union. (The SQL
     * frontend reaches the same {@code OpenSearchTableScanRule} path for {@code source=test*}.)
     */
    public void testAliasUnionWithNullFill() throws IOException {
        ensureProvisioned();
        assertUnionRows("source=" + ALIAS + " | fields name, age, alias");
    }

    /**
     * Same union, but with NO explicit projection — columns come back in the scan's natural order
     * (plus the implicit head/limit), exactly like {@code source=test*}. This is the shape that
     * surfaced a name/alias value swap when the scan's column order didn't line up with the
     * per-shard null-filled output.
     */
    public void testAliasUnionNoProjection() throws IOException {
        ensureProvisioned();
        assertUnionRows("source=" + ALIAS);
    }

    /**
     * The result columns must come back in the SAME order the query requested
     * ({@code fields name, age, alias}). The SQL frontend's AnalyticsExecutionEngine zips returned
     * row values POSITIONALLY against the Calcite plan's row type, so a reordered union output
     * silently mismaps values to the wrong column names (observed as a name/alias swap on
     * {@code source=test*}). The PPL route carries column names so it tolerates reordering — this
     * asserts the positional contract the SQL route depends on.
     */
    public void testUnionPreservesRequestedColumnOrder() throws IOException {
        ensureProvisioned();
        Map<String, Object> body = executePpl("source=" + ALIAS + " | fields name, age, alias");
        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(body);
        assertEquals("union output column order must match requested fields", List.of("name", "age", "alias"), columns);
    }


    /**
     * A dotted index name (date-based, e.g. {@code logs-2024.01.01}) must register and bind
     * end-to-end. The data node builds its DataFusion table reference from the Substrait
     * NamedTable's name parts, so the dotted name stays a single identifier rather than being
     * split into catalog/schema/table (which a {@code &str}-parsed reference would do → scan miss).
     */
    public void testDottedIndexNameRegistersAndScans() throws IOException {
        String dotted = "logs-2024.01.01";
        createParquetIndex(dotted, "{\"properties\":{\"v\":{\"type\":\"long\"}}}");
        bulk(dotted, "{\"v\":1}\n{\"v\":2}\n");
        Map<String, Object> body = executePpl("source=" + dotted + " | stats count() as c");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals("single count row", 1, rows.size());
        assertEquals("dotted index must scan its 2 rows", 2L, ((Number) rows.get(0).get(0)).longValue());
    }

    /** Fan-out sanity: the alias spans both indices, so 2 + 1 = 3 rows. */
    public void testAliasFansOutAcrossDifferingIndices() throws IOException {
        ensureProvisioned();
        Map<String, Object> body = executePpl("source=" + ALIAS + " | stats count() as c");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals("single count row", 1, rows.size());
        assertEquals("union_a (2) + union_b (1)", 3L, ((Number) rows.get(0).get(0)).longValue());
    }

    // ── assertions ─────────────────────────────────────────────────────────

    private void assertUnionRows(String ppl) throws IOException {
        Map<String, Object> body = executePpl(ppl);
        Map<String, Integer> col = columnIndex(body);
        assertTrue("schema must carry all union columns", col.containsKey("name") && col.containsKey("age") && col.containsKey("alias"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals("union row count", 3, rows.size());

        Map<String, List<Object>> byName = new HashMap<>();
        for (List<Object> row : rows) {
            byName.put(String.valueOf(row.get(col.get("name"))), row);
        }

        List<Object> hello = byName.get("hello");
        assertNotNull("row from union_a (name=hello)", hello);
        assertEquals("hello.age", 20L, ((Number) hello.get(col.get("age"))).longValue());
        assertNull("union_a has no alias column → null-filled", hello.get(col.get("alias")));

        List<Object> world = byName.get("world");
        assertNotNull("row from union_a (name=world)", world);
        assertEquals("world.age", 30L, ((Number) world.get(col.get("age"))).longValue());
        assertNull("union_a has no alias column → null-filled", world.get(col.get("alias")));

        List<Object> upperHello = byName.get("HELLO");
        assertNotNull("row from union_b (name=HELLO)", upperHello);
        assertEquals("HELLO.alias", "Hello", upperHello.get(col.get("alias")));
        assertNull("union_b has no age column → null-filled", upperHello.get(col.get("age")));
    }

    /** Maps each column name in the PPL response to its position in the row arrays. */
    @SuppressWarnings("unchecked")
    private static Map<String, Integer> columnIndex(Map<String, Object> body) {
        List<String> columns = extractColumnNames(body);
        assertNotNull("response must carry columns", columns);
        Map<String, Integer> col = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            col.put(columns.get(i), i);
        }
        return col;
    }

    // ── helpers ──────────────────────────────────────────────────────────


    private void createParquetIndex(String name, String mappingJson) throws IOException {
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.number_of_shards\":1,\"index.number_of_replicas\":0},"
                + "\"mappings\":" + mappingJson + "}"
        );
        try {
            client().performRequest(create);
        } catch (ResponseException re) {
            if (entityAsString(re.getResponse()).contains("resource_already_exists_exception") == false) {
                throw re;
            }
        }
    }

    private void bulk(String index, String ndjsonDocs) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (String doc : ndjsonDocs.split("\n")) {
            if (doc.isBlank()) continue;
            bulk.append("{\"index\": {}}\n").append(doc).append("\n");
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.setJsonEntity(bulk.toString());
        request.addParameter("refresh", "true");
        request.setOptions(request.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "bulk " + index);
        assertEquals("bulk into " + index + " had errors", false, response.get("errors"));
    }

    private void putAlias(String alias, List<String> indices) throws IOException {
        StringBuilder actions = new StringBuilder("{\"actions\":[");
        for (int i = 0; i < indices.size(); i++) {
            if (i > 0) actions.append(",");
            actions.append("{\"add\":{\"index\":\"").append(indices.get(i)).append("\",\"alias\":\"").append(alias).append("\"}}");
        }
        actions.append("]}");
        Request put = new Request("POST", "/_aliases");
        put.setJsonEntity(actions.toString());
        client().performRequest(put);
    }

    private static String entityAsString(Response response) throws IOException {
        try (var is = response.getEntity().getContent()) {
            return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}

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
import java.util.List;
import java.util.Map;

/**
 * Per-type coverage test for composite-engine + parquet indexing and the analytics-engine
 * PPL read path. One test method per OpenSearch mapping type the sandbox targets.
 *
 * <p>Each test is a two-phase script:
 * <ol>
 *   <li><b>Ingest phase</b> — {@link #ingest} creates the index, posts N docs, and returns
 *       the raw bulk response without asserting on it.</li>
 *   <li><b>Assertion phase</b> — one or more of:
 *     <ul>
 *       <li>{@link #assertBulkSucceeded} — {@code errors=false} and flush. Use first when ingest is expected to succeed.</li>
 *       <li>{@link #assertBulkErrored} — {@code errors=true}. For types whose parquet writer throws.</li>
 *       <li>{@link #assertScanSucceeds} — bare {@code source=<index>} scan; materializes every column.</li>
 *       <li>{@link #assertScanFails} — bare scan must throw. Known-bug guard for types DataFusion can't materialize.</li>
 *     </ul>
 *   </li>
 * </ol>
 */
public class FieldTypeCoverageIT extends AnalyticsRestTestCase {

    // ── Numeric ───────────────────────────────────────────────────────────────────

    public void testByte() throws IOException {
        Map<String, Object> bulk = ingest("ft_byte", "byte", 1, 2, 3);
        assertBulkSucceeded(bulk, "ft_byte");
        assertScanSucceeds("ft_byte", 3);
    }

    public void testShort() throws IOException {
        Map<String, Object> bulk = ingest("ft_short", "short", 100, 200, 300);
        assertBulkSucceeded(bulk, "ft_short");
        assertScanSucceeds("ft_short", 3);
    }

    public void testInteger() throws IOException {
        Map<String, Object> bulk = ingest("ft_integer", "integer", 1000, 2000, 3000);
        assertBulkSucceeded(bulk, "ft_integer");
        assertScanSucceeds("ft_integer", 3);
    }

    public void testLong() throws IOException {
        Map<String, Object> bulk = ingest("ft_long", "long", 100000000L, 200000000L, 300000000L);
        assertBulkSucceeded(bulk, "ft_long");
        assertScanSucceeds("ft_long", 3);
    }

    public void testUnsignedLong() throws IOException {
        // Test values stay below 2^63 - 1 because BIGINT is signed; values above wrap.
        Map<String, Object> bulk = ingest(
            "ft_unsigned_long",
            "unsigned_long",
            12345678901234567L,
            23456789012345678L,
            34567890123456789L
        );
        assertBulkSucceeded(bulk, "ft_unsigned_long");
        assertScanSucceeds("ft_unsigned_long", 3);
    }

    public void testHalfFloat() throws IOException {
        // half_float lands as Arrow Float16. OpenSearchSchemaBuilder maps it to REAL and
        // schema_coerce widens Float16 → Float32 before substrait binds; SchemaAdapter
        // inserts the IEEE half→single cast per batch on read.
        Map<String, Object> bulk = ingest("ft_half_float", "half_float", 47.6, 45.5, 52.1);
        assertBulkSucceeded(bulk, "ft_half_float");
        assertScanSucceeds("ft_half_float", 3);
    }

    public void testFloat() throws IOException {
        Map<String, Object> bulk = ingest("ft_float", "float", 47.6, 45.5, 52.1);
        assertBulkSucceeded(bulk, "ft_float");
        assertScanSucceeds("ft_float", 3);
    }

    public void testDouble() throws IOException {
        Map<String, Object> bulk = ingest("ft_double", "double", 47.6, 45.5, 52.1);
        assertBulkSucceeded(bulk, "ft_double");
        assertScanSucceeds("ft_double", 3);
    }

    public void testScaledFloat() throws IOException {
        // Scan binds as BIGINT (storage is Int64). Projections / predicates / aggregates
        // expose the *scaled* long, not the original float.
        Map<String, Object> bulk = ingestWithMapping("ft_scaled_float", "scaled_float", ", \"scaling_factor\": 100", 19.99, 25.50, 99.00);
        assertBulkSucceeded(bulk, "ft_scaled_float");
        assertScanSucceeds("ft_scaled_float", 3);
    }

    // ── Text / keyword ────────────────────────────────────────────────────────────

    public void testKeyword() throws IOException {
        Map<String, Object> bulk = ingest("ft_keyword", "keyword", "alice", "bob", "carol");
        assertBulkSucceeded(bulk, "ft_keyword");
        assertScanSucceeds("ft_keyword", 3);
    }

    public void testText() throws IOException {
        Map<String, Object> bulk = ingest("ft_text", "text", "connection refused", "host unreachable", "peer reset");
        assertBulkSucceeded(bulk, "ft_text");
        assertScanSucceeds("ft_text", 3);
    }

    public void testMatchOnlyText() throws IOException {
        Map<String, Object> bulk = ingest(
            "ft_match_only_text",
            "match_only_text",
            "timeout on socket",
            "dns lookup failed",
            "tls handshake error"
        );
        assertBulkSucceeded(bulk, "ft_match_only_text");
        assertScanSucceeds("ft_match_only_text", 3);
    }

    // ── Temporal ─────────────────────────────────────────────────────────────────

    public void testDate() throws IOException {
        Map<String, Object> bulk = ingest("ft_date", "date", "\"1990-01-15\"", "\"1995-05-20\"", "\"1988-03-10\"");
        assertBulkSucceeded(bulk, "ft_date");
        assertScanSucceeds("ft_date", 3);
    }

    public void testDateNanos() throws IOException {
        // Scan binds as TIMESTAMP. Sub-millisecond precision is silently truncated because
        // Calcite TIMESTAMP is millisecond-precision.
        Map<String, Object> bulk = ingest(
            "ft_date_nanos",
            "date_nanos",
            "\"1990-01-15T10:00:00.123456789Z\"",
            "\"1995-05-20T11:00:00.987654321Z\"",
            "\"1988-03-10T12:00:00.111222333Z\""
        );
        assertBulkSucceeded(bulk, "ft_date_nanos");
        assertScanSucceeds("ft_date_nanos", 3);
    }

    // ── Other ────────────────────────────────────────────────────────────────────

    public void testBoolean() throws IOException {
        Map<String, Object> bulk = ingest("ft_boolean", "boolean", true, false, true);
        assertBulkSucceeded(bulk, "ft_boolean");
        assertScanSucceeds("ft_boolean", 3);
    }

    public void testBinary() throws IOException {
        // Scan binds as VARBINARY; the BinaryView → Binary rewrite happens in
        // schema_coerce::coerce_for_substrait. Predicates on binary columns now work via the
        // BINARY(varchar) placeholder (BinaryFunctionAdapter rewrites it into a VARBINARY
        // literal that DataFusion compares natively). Filter coverage lives in testIpFilters
        // — binary columns share the same code path.
        Map<String, Object> bulk = ingest("ft_binary", "binary", "\"YWxpY2U=\"", "\"Ym9i\"", "\"Y2Fyb2w=\"");
        assertBulkSucceeded(bulk, "ft_binary");
        assertScanSucceeds("ft_binary", 3);
    }

    public void testIp() throws IOException {
        // Scan binds as VARBINARY; the BinaryView → Binary rewrite happens in
        // schema_coerce::coerce_for_substrait. Projections return raw 16-byte IPv6-mapped
        // bytes. Filter / aggregation coverage on `ip` columns is in testIpFilters.
        Map<String, Object> bulk = ingest("ft_ip", "ip", "\"192.168.1.1\"", "\"10.0.0.1\"", "\"172.16.0.1\"");
        assertBulkSucceeded(bulk, "ft_ip");
        assertScanSucceeds("ft_ip", 3);
    }

    /**
     * End-to-end coverage of filter / aggregation shapes against an {@code ip} column.
     * Each predicate shape exercises a different code path:
     * <ul>
     *   <li>{@code =} — comparator with VARBINARY column + VARCHAR literal, expanded via
     *       {@code ExtendedRexBuilder.makeCast} to {@code BINARY(varchar)} and resolved by
     *       {@code BinaryFunctionAdapter}.</li>
     *   <li>{@code !=}, {@code >} — same path, different comparator.</li>
     *   <li>{@code IN} — exercised the {@code OpenSearchTypeFactory.leastRestrictive} override
     *       for VARBINARY ⇄ VARCHAR.</li>
     *   <li>{@code BETWEEN} — same {@code leastRestrictive} path with a 3-arg shape.</li>
     *   <li>{@code cidrmatch} — exercised the inline expansion in
     *       {@code PPLFuncImpTable.populate} that rewrites cidr literals into byte-range AND.</li>
     *   <li>{@code AND}-combination — exercised the {@code RexUtil.flatten} guard in
     *       {@code OpenSearchFilter.stripAnnotations}.</li>
     * </ul>
     */
    public void testIpFilters() throws IOException {
        Map<String, Object> bulk = ingest("ft_ip_filters", "ip", "\"192.168.1.1\"", "\"10.0.0.1\"", "\"172.16.0.1\"");
        assertBulkSucceeded(bulk, "ft_ip_filters");
        assertScanSucceeds("ft_ip_filters", 3);

        assertFilterRowCount("source=ft_ip_filters | where val = '192.168.1.1'", 1);
        assertFilterRowCount("source=ft_ip_filters | where val != '192.168.1.1'", 2);
        assertFilterRowCount("source=ft_ip_filters | where val > '10.0.0.50'", 2);
        assertFilterRowCount("source=ft_ip_filters | where val < '172.16.0.1'", 1);
        assertFilterRowCount("source=ft_ip_filters | where val in ('192.168.1.1', '10.0.0.1')", 2);
        assertFilterRowCount("source=ft_ip_filters | where val between '10.0.0.0' and '10.255.255.255'", 1);

        assertFilterRowCount("source=ft_ip_filters | where cidrmatch(val, '192.168.0.0/16')", 1);
        assertFilterRowCount("source=ft_ip_filters | where cidrmatch(val, '10.0.0.0/8')", 1);
        assertFilterRowCount("source=ft_ip_filters | where cidrmatch(val, '0.0.0.0/0')", 3);
        assertFilterRowCount("source=ft_ip_filters | where NOT cidrmatch(val, '192.168.0.0/16')", 2);

        // AND-combination exercises the flatten guard in OpenSearchFilter.stripAnnotations
        // (>= 4 conjuncts after SARG/cidr expansion).
        assertFilterRowCount(
            "source=ft_ip_filters | where val > '10.0.0.0' AND val < '200.0.0.0'"
                + " AND cidrmatch(val, '0.0.0.0/0')",
            3
        );
    }

    /**
     * Project-side coverage of {@code BinaryFunctionAdapter}. PPL {@code eval if(col=lit, …)},
     * {@code case(col=lit, …)}, and {@code count(eval(col=lit))} all lower to a
     * {@code BINARY('lit':VARCHAR)} placeholder inside a {@code LogicalProject} (or a CASE in
     * the project tree above an aggregate).
     */
    public void testIpAndBinaryProjectExpressions() throws IOException {
        Map<String, Object> ipBulk = ingest("ft_ip_project", "ip", "\"192.168.1.1\"", "\"10.0.0.1\"", "\"172.16.0.1\"");
        assertBulkSucceeded(ipBulk, "ft_ip_project");
        assertFilterRowCount("source=ft_ip_project | eval is_local=if(val='192.168.1.1','y','n')", 3);
        assertFilterRowCount(
            "source=ft_ip_project | eval cls=case(val='192.168.1.1','a',val='10.0.0.1','b',true,'c')",
            3
        );
        assertFilterRowCount("source=ft_ip_project | stats count(eval(val='192.168.1.1')) as cnt", 1);

        Map<String, Object> binBulk = ingest("ft_binary_project", "binary", "\"YWxpY2U=\"", "\"Ym9i\"", "\"Y2Fyb2w=\"");
        assertBulkSucceeded(binBulk, "ft_binary_project");
        assertFilterRowCount("source=ft_binary_project | eval is_alice=if(val='YWxpY2U=','y','n')", 3);
        assertFilterRowCount("source=ft_binary_project | stats count(eval(val='YWxpY2U=')) as c", 1);
        assertFilterRowCount("source=ft_ip_project | where val='192.168.1.1' | fields val", 1);
    }

    // ── Phase 1: Ingest ──────────────────────────────────────────────────────────

    /**
     * Create a single-field index and post N docs (named a/b/c/...). Returns the parsed
     * bulk response without asserting on it — the caller decides whether to expect
     * success ({@link #assertBulkSucceeded}) or failure ({@link #assertBulkErrored}).
     */
    private Map<String, Object> ingest(String index, String type, Object... values) throws IOException {
        return ingestWithMapping(index, type, "", values);
    }

    /**
     * Variant of {@link #ingest} that lets the caller inject extra mapping properties into
     * the field definition (e.g. {@code ", \"scaling_factor\": 100"} for {@code scaled_float}).
     * The fragment must be a leading-comma-prefixed JSON snippet or empty.
     *
     * <p>Distinct method name (rather than another {@code ingest} overload) because
     * {@code ingest(idx, type, "...string-value...", ...)} would otherwise bind to the
     * extra-mapping overload — Java picks {@code String} over {@code Object} as more specific
     * — silently eating the first row value as a mapping fragment.
     */
    private Map<String, Object> ingestWithMapping(String index, String type, String extraMappingJson, Object... values)
        throws IOException {
        String mapping = "\"val\": { \"type\": \"" + type + "\"" + extraMappingJson + " }";
        createIndex(index, createBody(index, mapping));
        return bulkRaw(index, asJsonArray(values));
    }

    // ── Phase 2: Query assertions ────────────────────────────────────────────────

    /**
     * Bulk response must report {@code errors=false}. Flushes so the parquet rowgroup is
     * sealed before any subsequent scan assertion runs.
     */
    private void assertBulkSucceeded(Map<String, Object> bulkResponse, String index) throws IOException {
        assertEquals("Expected bulk ingest to succeed for [" + index + "]", Boolean.FALSE, bulkResponse.get("errors"));
        flush(index);
    }

    /**
     * Bulk response must report {@code errors=true}. For types where the primary writer's
     * {@code ParquetField} throws (byte, half_float).
     */
    private void assertBulkErrored(Map<String, Object> bulkResponse, String type) {
        assertEquals(
            "Type [" + type + "] unexpectedly succeeded at ingest. Known-bug guard: update the test if this is now fixed.",
            Boolean.TRUE,
            bulkResponse.get("errors")
        );
    }

    /**
     * Bare {@code source=<index>} scan must return {@code expected} rows. Materializes every
     * column, so this catches per-column read-path bugs that {@code count()} misses.
     */
    private void assertScanSucceeds(String index, int expected) throws IOException {
        Map<String, Object> resp = executePpl("source=" + index);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) resp.get("rows");
        assertNotNull("source=" + index + " response missing rows", rows);
        assertEquals("source=" + index + " row count", expected, rows.size());
    }

    /**
     * Runs a PPL query and asserts the row count matches {@code expected}. Used by tests
     * that exercise filter / aggregation shapes (e.g. {@link #testIpFilters}) where the
     * row count is the meaningful signal.
     */
    private void assertFilterRowCount(String ppl, int expected) throws IOException {
        Map<String, Object> resp = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) resp.get("rows");
        assertNotNull("[" + ppl + "] response missing rows", rows);
        assertEquals("[" + ppl + "] row count", expected, rows.size());
    }

    /**
     * Bare {@code source=<index>} scan must throw. Known-bug guard for types where the
     * column never lands in parquet (e.g. match_only_text has no parquet writer). When
     * the underlying gap is closed and the scan starts succeeding, this assertion will
     * fail and prompt the test to be flipped to {@link #assertScanSucceeds}.
     */
    private void assertScanFails(String index) {
        Request req = new Request("POST", "/_analytics/ppl");
        req.setJsonEntity("{\"query\": \"source=" + index + "\"}");
        try {
            Response resp = client().performRequest(req);
            fail(
                "Expected source=" + index + " to fail, got status "
                    + resp.getStatusLine().getStatusCode()
                    + ". Known-bug guard: update the test if this is now fixed."
            );
        } catch (ResponseException expected) {
            assertTrue(
                "Expected 5xx for source=" + index + ", got " + expected.getResponse().getStatusLine().getStatusCode(),
                expected.getResponse().getStatusLine().getStatusCode() >= 500
            );
        } catch (IOException io) {
            // Transport-level error — also counts as "scan failed".
        }
    }

    // ── Lower-level helpers ──────────────────────────────────────────────────────

    private static String createBody(String index, String valMappingFragment) {
        return "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": { \"type\": \"keyword\" },"
            + "    " + valMappingFragment
            + "  }"
            + "}"
            + "}";
    }

    private void createIndex(String index, String body) throws IOException {
        // Be forgiving of leftover state from previous runs (preserveIndicesUponCompletion = true).
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (ResponseException ignored) {
            // expected on first run
        }
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(body);
        Map<String, Object> resp = assertOkAndParse(client().performRequest(create), "Create index " + index);
        assertEquals(Boolean.TRUE, resp.get("acknowledged"));
    }

    /** Convert each value to its JSON literal form. Strings get wrapped in quotes unless already pre-quoted. */
    private static String[] asJsonArray(Object... values) {
        String[] out = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            Object v = values[i];
            if (v instanceof String) {
                String s = (String) v;
                out[i] = s.startsWith("\"") ? s : "\"" + s + "\"";
            } else {
                out[i] = String.valueOf(v);
            }
        }
        return out;
    }

    /**
     * Fire an N-doc bulk with sequential names {@code a, b, c, …} and {@code val=jsonValues[i]},
     * returning the parsed response.
     */
    private Map<String, Object> bulkRaw(String index, String[] jsonValues) throws IOException {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < jsonValues.length; i++) {
            body.append("{\"index\":{}}\n");
            body.append("{\"name\":\"").append(rowName(i)).append("\",\"val\":").append(jsonValues[i]).append("}\n");
        }
        Request bulk = new Request("POST", "/" + index + "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        bulk.setJsonEntity(body.toString());
        return assertOkAndParse(client().performRequest(bulk), "Bulk " + index);
    }

    /** Generate a stable row name for index i: a, b, …, z, aa, ab, … */
    private static String rowName(int i) {
        StringBuilder sb = new StringBuilder();
        do {
            sb.insert(0, (char) ('a' + (i % 26)));
            i = i / 26 - 1;
        } while (i >= 0);
        return sb.toString();
    }

    private void flush(String index) throws IOException {
        client().performRequest(new Request("POST", "/" + index + "/_flush?force=true"));
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        return assertOkAndParse(client().performRequest(request), "PPL: " + ppl);
    }
}

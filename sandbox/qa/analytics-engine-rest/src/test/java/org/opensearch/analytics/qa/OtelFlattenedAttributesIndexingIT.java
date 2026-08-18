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
 * End-to-end indexing verification for {@code flat_object} attribute maps on a real cluster, using the
 * OpenTelemetry-logs ("Textbench") mapping shape over the shared 100-document {@code otel_logs} dataset.
 *
 * <p>This is the outermost verification layer for the flat_object → Parquet {@code MAP<utf8, utf8>}
 * column: unlike the unit tests and the {@code internalClusterTest} suite, it runs against a fully
 * assembled node with the analytics, composite-engine and parquet-data-format plugins installed and
 * drives everything over the REST API, exactly as a benchmark client would. It therefore covers the
 * pieces only a real node exercises — plugin wiring and capability assignment across the composite
 * primary/secondary formats, the bulk path, and flush plus force-merge of MAP columns through the
 * native writer.
 *
 * <p>The documents are the real OTel corpus already used by {@link OtelLogsPplIT}; only the mapping
 * differs. Where that dataset declares {@code resource}, {@code log} and {@code instrumentationScope}
 * as explicit object trees, here they are {@code flat_object}, which is how an OTel pipeline models
 * attribute bags whose keys are not known up front. The corpus is a good stress for that: {@code
 * resource} alone carries nine sub-trees and the documents span five services, so key sets differ from
 * document to document — the case a fixed-schema column cannot express.
 *
 * <p>Scope: indexing. Values are asserted to be accepted and durable across bulk → flush →
 * force-merge; projecting them back is the columnar read path's job and is not wired yet (see
 * {@code FlatObjectMapColumnIT#testAttributesAreNotYetReturnedInSource}).
 */
public class OtelFlattenedAttributesIndexingIT extends AnalyticsRestTestCase {

    private static final String INDEX = "otel_logs_flattened";
    private static final int EXPECTED_DOCS = 100;

    /**
     * The Textbench mapping shape: scalars typed as in the OTel spec, and every attribute bag typed
     * {@code flat_object} so it becomes a single Parquet MAP column. Composite/parquet settings match
     * what {@link DatasetProvisioner} injects for the other datasets.
     */
    private static final String MAPPING = "{"
        + "\"settings\":{"
        + "  \"index.pluggable.dataformat.enabled\": true,"
        + "  \"index.pluggable.dataformat\": \"composite\","
        + "  \"index.composite.primary_data_format\": \"parquet\","
        + "  \"index.composite.secondary_data_formats\": [\"lucene\"],"
        + "  \"number_of_shards\": 1,"
        + "  \"number_of_replicas\": 0"
        + "},"
        + "\"mappings\":{\"properties\":{"
        + "  \"time\":{\"type\":\"date\"},"
        + "  \"observedTimestamp\":{\"type\":\"date\"},"
        + "  \"traceId\":{\"type\":\"keyword\"},"
        + "  \"spanId\":{\"type\":\"keyword\"},"
        + "  \"flags\":{\"type\":\"byte\"},"
        + "  \"severityText\":{\"type\":\"keyword\"},"
        + "  \"severityNumber\":{\"type\":\"byte\"},"
        + "  \"serviceName\":{\"type\":\"keyword\"},"
        + "  \"body\":{\"type\":\"text\"},"
        + "  \"droppedAttributesCount\":{\"type\":\"long\"},"
        + "  \"schemaUrl\":{\"type\":\"keyword\"},"
        // The three attribute bags — the point of this test.
        + "  \"resource\":{\"type\":\"flat_object\"},"
        + "  \"log\":{\"type\":\"flat_object\"},"
        + "  \"instrumentationScope\":{\"type\":\"flat_object\"}"
        + "}}}";

    /**
     * Creates the index, or fails the test with the server's explanation if the mapping is rejected.
     * <p>
     * The suite preserves indices between test methods ({@code preserveIndicesUponCompletion}), so the
     * index is dropped first to keep each method independent of execution order.
     */
    private void createFlattenedIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (ResponseException e) {
            // 404 on the first run: nothing to drop.
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(MAPPING);
        try {
            assertOkAndParse(client().performRequest(create), "create " + INDEX);
        } catch (ResponseException e) {
            fail("flat_object mapping must be accepted on a composite parquet index, but got: " + e.getMessage());
        }
    }

    /** Bulk-indexes the shared OTel corpus, asserting the server reported no per-item failures. */
    private void bulkIndexOtelCorpus() throws IOException {
        String ndjson = DatasetProvisioner.loadResource(OtelLogsTestHelper.DATASET.bulkResourcePath());
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(ndjson);
        Map<String, Object> response = assertOkAndParse(client().performRequest(bulk), "bulk into " + INDEX);
        // "errors" is the only signal that individual documents were rejected; a 200 alone would hide
        // per-item mapper failures, which is exactly how a silently-dropped attribute bag would look.
        assertEquals("bulk must not report any item failures: " + response.get("items"), Boolean.FALSE, response.get("errors"));
    }

    private long docCount() throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + INDEX + "/_stats"));
        Map<String, Object> stats = assertOkAndParse(response, "stats for " + INDEX);
        @SuppressWarnings("unchecked")
        Map<String, Object> indices = (Map<String, Object>) stats.get("indices");
        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) indices.get(INDEX);
        @SuppressWarnings("unchecked")
        Map<String, Object> total = (Map<String, Object>) index.get("total");
        @SuppressWarnings("unchecked")
        Map<String, Object> docs = (Map<String, Object>) total.get("docs");
        return ((Number) docs.get("count")).longValue();
    }

    private void flush() throws IOException {
        Request flush = new Request("POST", "/" + INDEX + "/_flush");
        flush.addParameter("force", "true");
        client().performRequest(flush);
    }

    /**
     * The full lifecycle a benchmark run performs: create with flat_object attribute bags, bulk-index
     * the corpus, flush, then force-merge to a single segment. Force-merge is the stage that most
     * specifically exercises this change, because the native merge has to resolve an attribute column
     * to its two parquet leaves ({@code entries.key} and {@code entries.value}) rather than one.
     */
    public void testOtelCorpusIndexesWithFlattenedAttributeBags() throws IOException {
        createFlattenedIndex();

        // Mapping round-trips: the attribute bags stay flat_object rather than being turned into
        // dynamic object trees.
        Map<String, Object> mappingResponse = assertOkAndParse(
            client().performRequest(new Request("GET", "/" + INDEX + "/_mapping")),
            "mapping for " + INDEX
        );
        String rendered = mappingResponse.toString();
        for (String bag : List.of("resource", "log", "instrumentationScope")) {
            assertTrue("mapping must retain attribute bag [" + bag + "]: " + rendered, rendered.contains(bag));
        }
        assertTrue("attribute bags must stay flat_object: " + rendered, rendered.contains("flat_object"));

        bulkIndexOtelCorpus();
        assertEquals("every document must be indexed", EXPECTED_DOCS, docCount());

        flush();
        assertEquals("document count must survive flush", EXPECTED_DOCS, docCount());

        Request forceMerge = new Request("POST", "/" + INDEX + "/_forcemerge");
        forceMerge.addParameter("max_num_segments", "1");
        Map<String, Object> merge = assertOkAndParse(client().performRequest(forceMerge), "force-merge " + INDEX);
        @SuppressWarnings("unchecked")
        Map<String, Object> shards = (Map<String, Object>) merge.get("_shards");
        assertEquals("force-merge must not fail any shard: " + merge, 0, ((Number) shards.get("failed")).intValue());

        flush();
        assertEquals("document count must survive force-merge", EXPECTED_DOCS, docCount());
    }

    /**
     * Pins where basic search on a flat_object leaf currently stops on a composite index.
     *
     * <p>The storage side is in place: the lucene secondary indexes each leaf as a term
     * ({@code _value} for a bare value, {@code _valueAndPath} for {@code <field>.<path>=<value>}), the
     * same terms a plain Lucene index holds — {@code FlatObjectEngineParityIT} asserts that directly
     * against the Lucene {@code FieldInfos}. What is missing is query routing: on a composite index
     * {@code _search} is handled by the DSL query executor, whose {@code ConversionContext} resolves a
     * field name against the <em>columnar</em> row type. A dotted sub-path of a flat_object is not a
     * column of its own — the column is the parent {@code MAP} — so conversion fails before any
     * backend is chosen, and the Lucene terms are never consulted.
     *
     * <p>Fixing it means teaching that layer to route a flat_object predicate to the Lucene backend
     * (or to rewrite it into a key lookup on the MAP column), the same class of routing gap this suite
     * already documents for multi-field text in {@code OtelLogsPplIT}'s skip list. Asserted rather
     * than skipped so the behaviour is visible and this test flips when the routing lands.
     */
    public void testBasicSearchOnFlattenedLeafIsNotRoutedYet() throws IOException {
        createFlattenedIndex();
        bulkIndexOtelCorpus();
        flush();

        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.setJsonEntity("{\"size\":0,\"query\":{\"term\":{\"resource.service.name\":\"cart\"}}}");
        ResponseException failure = expectThrows(ResponseException.class, () -> client().performRequest(search));
        String body = org.opensearch.common.io.Streams.readFully(failure.getResponse().getEntity().getContent()).utf8ToString();
        assertTrue(
            "expected the columnar schema lookup to be what fails, got: " + body,
            body.contains("not found in schema") && body.contains("resource.service.name")
        );
    }

    /**
     * A benchmark corpus is not uniform, so the shapes a real OTel pipeline emits must all be
     * admitted into the same MAP column: an empty bag, an omitted bag, an array leaf (duplicate keys),
     * an explicit null, and a bag whose keys no earlier document used.
     */
    public void testHeterogeneousAttributeBagsAreAdmitted() throws IOException {
        createFlattenedIndex();

        String ndjson = "{\"index\":{}}\n"
            + "{\"serviceName\":\"cart\",\"log\":{}}\n"
            + "{\"index\":{}}\n"
            + "{\"serviceName\":\"cart\"}\n"
            + "{\"index\":{}}\n"
            + "{\"serviceName\":\"cart\",\"log\":{\"tag\":[\"a\",\"b\"]}}\n"
            + "{\"index\":{}}\n"
            + "{\"serviceName\":\"cart\",\"log\":{\"http\":{\"status\":null}}}\n"
            + "{\"index\":{}}\n"
            + "{\"serviceName\":\"cart\",\"log\":{\"brand\":{\"new\":{\"deep\":\"key\"}}}}\n";

        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(ndjson);
        Map<String, Object> response = assertOkAndParse(client().performRequest(bulk), "heterogeneous bulk");
        assertEquals("no shape may be rejected: " + response.get("items"), Boolean.FALSE, response.get("errors"));
        assertEquals(5, docCount());

        // And they must survive being made durable and re-encoded.
        flush();
        Request forceMerge = new Request("POST", "/" + INDEX + "/_forcemerge");
        forceMerge.addParameter("max_num_segments", "1");
        Map<String, Object> merge = assertOkAndParse(client().performRequest(forceMerge), "force-merge heterogeneous");
        @SuppressWarnings("unchecked")
        Map<String, Object> shards = (Map<String, Object>) merge.get("_shards");
        assertEquals("force-merge must not fail any shard: " + merge, 0, ((Number) shards.get("failed")).intValue());
        assertEquals(5, docCount());
    }
}

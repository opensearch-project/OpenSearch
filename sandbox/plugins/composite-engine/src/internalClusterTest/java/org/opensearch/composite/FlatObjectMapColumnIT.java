/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;

/**
 * Indexing-side support for {@code flat_object} fields on a composite parquet+lucene index, using
 * the OpenTelemetry-logs ("Textbench") mapping shape: keyword/date_nanos/byte/text scalars plus three
 * {@code flat_object} attribute maps.
 *
 * <p>A {@code flat_object} field is stored as a single Arrow/Parquet {@code MAP<utf8, utf8>} column
 * (leaves {@code <field>.entries.key} and {@code <field>.entries.value}) rather than being dropped, so
 * every stage that writes or rewrites those files is a place the map encoding can break independently
 * of the query path: the VSR flush writes the map offsets and the entries-struct validity bits, and
 * force-merge re-encodes each column through the native {@code compute_leaves} path, which must walk
 * two leaves for this column instead of one.
 *
 * <p>Scope: this exercises the <em>write</em> path only — index creation, document admission, and the
 * durability stages above. Reading the values back is a separate, unwired concern, so the assertions
 * here are deliberately about acceptance and durability (document counts survive each stage, no shard
 * fails) rather than about projecting map values; {@link #testAttributesAreNotYetReturnedInSource}
 * pins that boundary explicitly, and the remaining negative tests pin the arity and sort-key limits.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class FlatObjectMapColumnIT extends AbstractCompositeEngineIT {

    private static final String OTEL_INDEX = "otel-logs-idx";

    /**
     * The OpenTelemetry-logs mapping. {@code flattened} is the Elasticsearch spelling of this type;
     * the OpenSearch equivalent, used here, is {@code flat_object}.
     */
    private static final String OTEL_MAPPING = "{\"properties\":{"
        + "\"Timestamp\":{\"type\":\"date_nanos\"},"
        + "\"TraceId\":{\"type\":\"keyword\"},"
        + "\"SpanId\":{\"type\":\"keyword\"},"
        + "\"TraceFlags\":{\"type\":\"byte\"},"
        + "\"SeverityText\":{\"type\":\"keyword\"},"
        + "\"SeverityNumber\":{\"type\":\"byte\"},"
        + "\"ServiceName\":{\"type\":\"keyword\"},"
        + "\"Body\":{\"type\":\"text\"},"
        + "\"ResourceSchemaUrl\":{\"type\":\"keyword\"},"
        + "\"ResourceAttributes\":{\"type\":\"flat_object\"},"
        + "\"ScopeSchemaUrl\":{\"type\":\"keyword\"},"
        + "\"ScopeName\":{\"type\":\"keyword\"},"
        + "\"ScopeVersion\":{\"type\":\"keyword\"},"
        + "\"ScopeAttributes\":{\"type\":\"flat_object\"},"
        + "\"LogAttributes\":{\"type\":\"flat_object\"}"
        + "}}";

    private Settings compositeSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .build();
    }

    private void createOtelIndex() {
        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(OTEL_INDEX)
                .setSettings(compositeSettings())
                .setMapping(OTEL_MAPPING)
                .get()
                .isAcknowledged()
        );
        ensureGreen(OTEL_INDEX);
    }

    /**
     * One OTel log line. {@code i} varies the attribute keys and values so the MAP column sees a
     * different key set per document — the case a fixed-schema column cannot represent and the reason
     * these fields are a map rather than sub-columns.
     */
    private static String logLine(int i) {
        return "{"
            + "\"Timestamp\":\"2026-08-17T10:00:0"
            + (i % 10)
            + ".123456789Z\","
            + "\"TraceId\":\"trace-"
            + i
            + "\",\"SpanId\":\"span-"
            + i
            + "\",\"TraceFlags\":1,"
            + "\"SeverityText\":\"INFO\",\"SeverityNumber\":9,"
            + "\"ServiceName\":\"checkout\",\"Body\":\"request completed in "
            + i
            + "ms\","
            + "\"ResourceSchemaUrl\":\"https://opentelemetry.io/schemas/1.20.0\","
            + "\"ResourceAttributes\":{\"host\":{\"name\":\"node-"
            + i
            + "\"},\"k8s\":{\"pod\":\"web-"
            + i
            + "\"}},"
            + "\"ScopeSchemaUrl\":\"https://opentelemetry.io/schemas/1.20.0\","
            + "\"ScopeName\":\"io.opentelemetry.checkout\",\"ScopeVersion\":\"1.0."
            + i
            + "\","
            + "\"ScopeAttributes\":{\"library\":\"otel-java\"},"
            + "\"LogAttributes\":{\"http\":{\"status\":"
            + (200 + i)
            + ",\"method\":\"GET\"},\"retry\":"
            + (i % 2 == 0)
            + ",\"attempt-"
            + i
            + "\":\"v"
            + i
            + "\"}"
            + "}";
    }

    private void indexLogLines(int count, WriteRequest.RefreshPolicy policy) {
        for (int i = 0; i < count; i++) {
            client().prepareIndex(OTEL_INDEX).setRefreshPolicy(policy).setSource(logLine(i), XContentType.JSON).get();
        }
    }

    /** Asserts the shard still reports exactly {@code expected} live documents. */
    private void assertDocCount(String stage, long expected) {
        client().admin().indices().prepareRefresh(OTEL_INDEX).get();
        IndicesStatsResponse stats = client().admin().indices().prepareStats(OTEL_INDEX).get();
        assertEquals(stage + ": document count must be unchanged", expected, stats.getIndex(OTEL_INDEX).getTotal().getDocs().getCount());
    }

    /**
     * The whole OTel mapping — three {@code flat_object} fields alongside date_nanos/byte/text/keyword
     * — must be accepted. Before flat_object was backed by a MAP column, index creation failed:
     * enabling a pluggable data format also enables derived source, and flat_object could not satisfy
     * that check.
     */
    public void testOtelLogsMappingIsAccepted() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createOtelIndex();

        // The mapping round-trips with all three attribute fields still typed flat_object.
        String mapping = client().admin().indices().prepareGetMappings(OTEL_INDEX).get().mappings().get(OTEL_INDEX).source().toString();
        for (String attributeField : List.of("ResourceAttributes", "ScopeAttributes", "LogAttributes")) {
            assertTrue("mapping must retain " + attributeField, mapping.contains(attributeField));
        }
        assertTrue("attribute fields must stay flat_object", mapping.contains("flat_object"));
    }

    /**
     * Documents whose attribute objects are nested, differently-keyed, and of mixed value types must
     * all be admitted. Every leaf becomes one MAP entry, stringified — so a per-document key set is
     * representable in a column whose type is fixed for the whole file.
     */
    public void testDocumentsWithVaryingAttributeKeysAreIndexed() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createOtelIndex();

        indexLogLines(25, WriteRequest.RefreshPolicy.NONE);
        assertDocCount("after indexing", 25);
    }

    /**
     * refresh → flush → force-merge. Each stage either makes the in-memory VSR durable or rewrites the
     * Parquet files; force-merge is the interesting one, because the native merge must resolve this
     * column to its two leaves rather than one and would fail the shard if it could not.
     */
    public void testMapColumnSurvivesRefreshFlushAndForceMerge() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createOtelIndex();

        indexLogLines(10, WriteRequest.RefreshPolicy.NONE);
        client().admin().indices().prepareRefresh(OTEL_INDEX).get();
        assertDocCount("after refresh", 10);

        client().admin().indices().prepareFlush(OTEL_INDEX).setForce(true).get();
        assertDocCount("after flush", 10);

        // A second generation, so force-merge has more than one file to combine.
        indexLogLines(10, WriteRequest.RefreshPolicy.IMMEDIATE);
        client().admin().indices().prepareFlush(OTEL_INDEX).setForce(true).get();
        assertDocCount("after second generation", 20);

        ForceMergeResponse merge = client().admin().indices().prepareForceMerge(OTEL_INDEX).setMaxNumSegments(1).get();
        assertEquals("force-merge must not fail any shard", 0, merge.getFailedShards());
        client().admin().indices().prepareFlush(OTEL_INDEX).setForce(true).get();
        assertDocCount("after force-merge", 20);
    }

    /**
     * Write-path edge shapes for one map cell: an empty object, an absent field, duplicate keys from
     * an array leaf, and an explicit null value. All are legal documents and none may be rejected.
     */
    public void testEmptyAbsentDuplicateAndNullAttributeShapes() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createOtelIndex();

        // Empty object → a zero-entry, non-null map cell.
        client().prepareIndex(OTEL_INDEX).setSource("{\"ServiceName\":\"a\",\"LogAttributes\":{}}", XContentType.JSON).get();
        // Field absent entirely → a null cell, which stays distinct from the empty object above.
        client().prepareIndex(OTEL_INDEX).setSource("{\"ServiceName\":\"b\"}", XContentType.JSON).get();
        // An array leaf yields the same key twice; a Parquet MAP is a repeated group, so both survive
        // rather than one overwriting the other.
        client().prepareIndex(OTEL_INDEX)
            .setSource("{\"ServiceName\":\"c\",\"LogAttributes\":{\"tag\":[\"x\",\"y\"]}}", XContentType.JSON)
            .get();
        // An explicit null value: the key is dropped, matching flat_object's Lucene behaviour.
        client().prepareIndex(OTEL_INDEX).setSource("{\"ServiceName\":\"d\",\"LogAttributes\":{\"k\":null}}", XContentType.JSON).get();
        // Deeply nested leaves flatten to dotted keys.
        client().prepareIndex(OTEL_INDEX)
            .setSource("{\"ServiceName\":\"e\",\"LogAttributes\":{\"a\":{\"b\":{\"c\":\"deep\"}}}}", XContentType.JSON)
            .get();

        assertDocCount("after edge shapes", 5);

        // These shapes must also survive being made durable and re-encoded.
        client().admin().indices().prepareFlush(OTEL_INDEX).setForce(true).get();
        ForceMergeResponse merge = client().admin().indices().prepareForceMerge(OTEL_INDEX).setMaxNumSegments(1).get();
        assertEquals("force-merge must not fail any shard", 0, merge.getFailedShards());
        assertDocCount("after force-merge of edge shapes", 5);
    }

    /**
     * A {@code flat_object} field added to a live index must become a MAP column too, so an OTel
     * mapping can gain a new attribute bag without reindexing.
     */
    public void testFlatObjectAddedByMappingUpdate() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createOtelIndex();

        indexLogLines(3, WriteRequest.RefreshPolicy.IMMEDIATE);

        assertTrue(
            client().admin()
                .indices()
                .preparePutMapping(OTEL_INDEX)
                .setSource("{\"properties\":{\"ExtraAttributes\":{\"type\":\"flat_object\"}}}", XContentType.JSON)
                .get()
                .isAcknowledged()
        );

        client().prepareIndex(OTEL_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource("{\"ServiceName\":\"after-update\",\"ExtraAttributes\":{\"tenant\":\"acme\"}}", XContentType.JSON)
            .get();

        assertDocCount("after mapping update", 4);
        client().admin().indices().prepareFlush(OTEL_INDEX).setForce(true).get();
        assertDocCount("after flush following mapping update", 4);
    }

    /**
     * Documents the current read-back boundary, so it is a deliberate, visible state rather than a
     * silent surprise: a document carrying a flat_object field is retrievable and its scalar fields
     * come back, but the attribute object is <em>not</em> yet present in the reconstructed
     * {@code _source}.
     *
     * <p>The values are written and durable in the Parquet MAP column — the rest of this class proves
     * that — but nothing surfaces them back yet: derived source is rebuilt from the Lucene secondary,
     * which deliberately holds no doc values or stored fields for a field the primary format owns, so
     * {@link org.opensearch.index.mapper.FlatObjectFieldMapper#deriveSource} has no source to read and
     * omits the field.
     *
     * <p>When the columnar read path learns to project a MAP column this assertion must be inverted —
     * that is the point of asserting it rather than leaving it untested.
     */
    public void testAttributesAreNotYetReturnedInSource() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createOtelIndex();

        org.opensearch.action.index.IndexResponse indexed = client().prepareIndex(OTEL_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource("{\"ServiceName\":\"probe\",\"LogAttributes\":{\"http\":{\"status\":500}}}", XContentType.JSON)
            .get();
        client().admin().indices().prepareFlush(OTEL_INDEX).setForce(true).get();

        org.opensearch.action.get.GetResponse resp = client().prepareGet(OTEL_INDEX, indexed.getId()).setRealtime(false).get();
        assertTrue("the document itself must be retrievable", resp.isExists());
        java.util.Map<String, Object> source = resp.getSourceAsMap();
        assertEquals("scalar fields must round-trip", "probe", source.get("ServiceName"));
        assertNull(
            "flat_object is not yet reconstructed into _source; invert this once the MAP read path lands",
            source.get("LogAttributes")
        );
    }

    // === BOUNDARIES ===

    /**
     * {@code flat_object} is single-arity: one map cell already holds any number of entries, so
     * declaring it {@code multi_value: true} would ask for a {@code LIST<MAP>} the writer does not
     * build. It must fail at creation time rather than at first write.
     */
    public void testFlatObjectRejectsMultiValue() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .prepareCreate("otel-mv-flat-idx")
                .setSettings(compositeSettings())
                .setMapping("{\"properties\":{\"LogAttributes\":{\"type\":\"flat_object\",\"multi_value\":true}}}")
                .get()
        );
    }

    /**
     * A MAP column has no single value to sort on, so it cannot be an {@code index.sort.field}. The
     * native k-way merge can only build a sort key from a primitive leaf and would otherwise fail on
     * the first merge, long after the index started accepting writes.
     */
    public void testFlatObjectRejectedAsIndexSortField() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        Settings sorted = Settings.builder().put(compositeSettings()).put("index.sort.field", "LogAttributes").build();
        expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareCreate("otel-sorted-idx").setSettings(sorted).setMapping(OTEL_MAPPING).get()
        );
    }
}

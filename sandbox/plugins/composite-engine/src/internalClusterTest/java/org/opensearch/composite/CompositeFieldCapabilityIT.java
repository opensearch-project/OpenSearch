/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Integration tests validating field capability assignment for composite indices.
 * Verifies that supported field types create successfully, documents can be indexed,
 * and both parquet and lucene structures contain expected fields.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeFieldCapabilityIT extends AbstractCompositeEngineIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    private Settings dfaSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    private void startCluster() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
    }

    private void assertIndexCreationSucceeds(String indexName, String fieldName, String mappingType) {
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(dfaSettings())
            .setMapping(fieldName, mappingType)
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);
    }

    private void assertIndexCreationFails(String indexName, String fieldName, String mappingType) {
        expectThrows(
            MapperParsingException.class,
            () -> client().admin().indices().prepareCreate(indexName).setSettings(dfaSettings()).setMapping(fieldName, mappingType).get()
        );
    }

    // === SUPPORTED FIELDS (expect 200) ===

    public void testLongFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-long", "field", "type=long");
    }

    public void testIntegerFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-integer", "field", "type=integer");
    }

    public void testShortFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-short", "field", "type=short");
    }

    public void testByteFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-byte", "field", "type=byte");
    }

    public void testDoubleFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-double", "field", "type=double");
    }

    public void testFloatFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-float", "field", "type=float");
    }

    public void testHalfFloatFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-half-float", "field", "type=half_float");
    }

    public void testUnsignedLongFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-unsigned-long", "field", "type=unsigned_long");
    }

    public void testDateFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-date", "field", "type=date");
    }

    public void testDateNanosFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-date-nanos", "field", "type=date_nanos");
    }

    public void testBooleanFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-boolean", "field", "type=boolean,index=false");
    }

    public void testKeywordFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-keyword", "field", "type=keyword");
    }

    public void testKeywordFieldSupportedWithIgnoreAbove() {
        startCluster();
        assertIndexCreationSucceeds("test-keyword", "field", "type=keyword,ignore_above=256");
    }

    public void testTextFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-text", "field", "type=text");
    }

    public void testIpFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-ip", "field", "type=ip");
    }

    public void testBinaryFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-binary", "field", "type=binary,store=true");
    }

    public void testMatchOnlyTextFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-match-only-text", "field", "type=match_only_text");
    }

    // === UNSUPPORTED FIELDS (expect 400 MapperParsingException) ===

    public void testGeoPointFieldUnsupported() {
        startCluster();
        assertIndexCreationFails("test-geo-point", "field", "type=geo_point");
    }

    public void testGeoShapeFieldUnsupported() {
        startCluster();
        assertIndexCreationFails("test-geo-shape", "field", "type=geo_shape");
    }

    public void testCompletionFieldUnsupported() {
        startCluster();
        assertIndexCreationFails("test-completion", "field", "type=completion");
    }

    public void testNestedFieldUnsupported() {
        startCluster();
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().admin()
                .indices()
                .prepareCreate("test-nested")
                .setSettings(dfaSettings())
                .setMapping("field", "type=nested")
                .get()
        );
        assertTrue(ex.getMessage().contains("nested type is not supported with pluggable data format"));
    }

    public void testFlatObjectFieldUnsupported() {
        startCluster();
        assertIndexCreationFails("test-flat-object", "field", "type=flat_object");
    }

    public void testWildcardFieldUnsupported() {
        startCluster();
        assertIndexCreationFails("test-wildcard", "field", "type=wildcard");
    }

    // === SPECIAL CASES ===

    public void testMultiFieldTextWithKeywordSubfield() throws Exception {
        startCluster();
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"field\": {\n"
            + "      \"type\": \"text\",\n"
            + "      \"fields\": {\n"
            + "        \"raw\": { \"type\": \"keyword\" }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-multi-field")
            .setSettings(dfaSettings())
            .setMapping(mapping)
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-multi-field");
    }

    public void testFieldWithOnlyDocValues() {
        startCluster();
        assertIndexCreationSucceeds("test-doc-values-only", "field", "type=keyword,index=false,doc_values=true");
    }

    public void testFieldWithOnlyStored() {
        startCluster();
        assertIndexCreationSucceeds("test-stored-only", "field", "type=keyword,index=false,doc_values=false,store=true");
    }

    public void testFieldWithAllDisabled() {
        startCluster();
        assertIndexCreationFails("test-all-disabled", "field", "type=keyword,index=false,doc_values=false,store=false");
    }

    public void testMultipleFieldsAllSupported() {
        startCluster();
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-multiple-fields")
            .setSettings(dfaSettings())
            .setMapping("f_long", "type=long", "f_keyword", "type=keyword", "f_date", "type=date", "f_text", "type=text")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-multiple-fields");
    }

    // === INDEXING + STRUCTURE VERIFICATION TESTS ===

    /**
     * Tests indexing and dynamic mapping for ALL supported field types.
     * Creates an index with every supported type, indexes documents, adds a dynamic field,
     * and verifies parquet/lucene structures contain expected fields and all docs coexist.
     */
    public void testAllSupportedFieldTypesIndexAndVerify() throws Exception {
        startCluster();
        String indexName = "test-all-types-index";
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"f_long\": { \"type\": \"long\" },\n"
            + "    \"f_integer\": { \"type\": \"integer\" },\n"
            + "    \"f_short\": { \"type\": \"short\" },\n"
            + "    \"f_byte\": { \"type\": \"byte\" },\n"
            + "    \"f_double\": { \"type\": \"double\" },\n"
            + "    \"f_float\": { \"type\": \"float\" },\n"
            + "    \"f_half_float\": { \"type\": \"half_float\" },\n"
            + "    \"f_unsigned_long\": { \"type\": \"unsigned_long\" },\n"
            + "    \"f_date\": { \"type\": \"date\" },\n"
            + "    \"f_date_nanos\": { \"type\": \"date_nanos\" },\n"
            + "    \"f_boolean\": { \"type\": \"boolean\", \"index\": false },\n"
            + "    \"f_keyword\": { \"type\": \"keyword\" },\n"
            + "    \"f_keyword_ignore\": { \"type\": \"keyword\", \"ignore_above\": 256 },\n"
            + "    \"f_text\": { \"type\": \"text\" },\n"
            + "    \"f_ip\": { \"type\": \"ip\" },\n"
            + "    \"f_match_only_text\": { \"type\": \"match_only_text\" }\n"
            + "  }\n"
            + "}";
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(dfaSettings())
            .setMapping(mapping)
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);

        // Index doc with all field types populated
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName)
                .setSource(
                    "f_long",
                    100L,
                    "f_integer",
                    42,
                    "f_short",
                    7,
                    "f_byte",
                    1,
                    "f_double",
                    3.14,
                    "f_float",
                    2.5f,
                    "f_half_float",
                    1.5f,
                    "f_unsigned_long",
                    9999999999L,
                    "f_date",
                    "2024-01-15",
                    "f_date_nanos",
                    "2024-01-15T10:30:00.123456789Z",
                    "f_boolean",
                    true,
                    "f_keyword",
                    "alpha",
                    "f_keyword_ignore",
                    "short_val",
                    "f_text",
                    "hello world",
                    "f_ip",
                    "192.168.1.1",
                    "f_match_only_text",
                    "searchable text"
                )
                .get()
                .status()
        );

        // Index second doc with some fields
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName)
                .setSource(
                    "f_long",
                    200L,
                    "f_integer",
                    99,
                    "f_keyword",
                    "beta",
                    "f_text",
                    "second doc",
                    "f_boolean",
                    false,
                    "f_date",
                    "2024-06-01"
                )
                .get()
                .status()
        );

        // Dynamic mapping: add a new field not in original mapping
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName)
                .setSource("f_long", 300L, "f_keyword", "gamma", "f_text", "third with dynamic", "dynamic_new_field", "dynamic_value")
                .get()
                .status()
        );

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        // Verify all 3 docs exist
        assertDocCount(indexName, 3);

        // Verify parquet contains expected fields
        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> parquetRows = readAllParquetRows(parquetDir, shard);
        assertEquals(3, parquetRows.size());

        // All original fields should appear in at least one row
        assertTrue("f_long in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_long")));
        assertTrue("f_integer in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_integer")));
        assertTrue("f_short in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_short")));
        assertTrue("f_byte in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_byte")));
        assertTrue("f_double in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_double")));
        assertTrue("f_float in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_float")));
        assertTrue("f_half_float in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_half_float")));
        assertTrue("f_unsigned_long in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_unsigned_long")));
        assertTrue("f_date in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_date")));
        assertTrue("f_date_nanos in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_date_nanos")));
        assertTrue("f_boolean in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_boolean")));
        assertTrue("f_keyword in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_keyword")));
        assertTrue("f_keyword_ignore in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_keyword_ignore")));
        assertTrue("f_text in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_text")));
        assertTrue("f_ip in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_ip")));
        assertTrue("f_match_only_text in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("f_match_only_text")));

        // Dynamic field should appear
        assertTrue("dynamic_new_field in parquet", parquetRows.stream().anyMatch(r -> r.containsKey("dynamic_new_field")));

        // Verify lucene has expected indexed fields (inverted index: text/keyword types)
        Path luceneDir = shard.shardPath().resolveIndex();
        Set<String> luceneFields = getLuceneFields(luceneDir);
        assertTrue("Lucene should have 'f_keyword'", luceneFields.contains("f_keyword"));
        assertTrue("Lucene should have 'f_keyword_ignore'", luceneFields.contains("f_keyword_ignore"));
        assertTrue("Lucene should have 'f_text'", luceneFields.contains("f_text"));
        assertTrue("Lucene should have 'f_match_only_text'", luceneFields.contains("f_match_only_text"));
        assertTrue("Lucene should have 'dynamic_new_field'", luceneFields.contains("dynamic_new_field"));
    }

    /**
     * Tests that a keyword field with ignore_above can be indexed and the sourceKeywordFieldType
     * is stored in parquet when the value exceeds ignore_above.
     * Also verifies dynamic mapping works and old+new docs coexist.
     */
    public void testKeywordIgnoreAboveIndexAndVerify() throws Exception {
        startCluster();
        String indexName = "test-keyword-ignore-above-verify";
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(dfaSettings())
            .setMapping("field", "type=keyword,ignore_above=10", "id_field", "type=keyword")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);

        // Index doc with value within ignore_above
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("field", "short", "id_field", "doc1").get().status());
        // Index doc with value exceeding ignore_above (value ignored, sourceKeywordFieldType stores raw)
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName).setSource("field", "this_exceeds_ignore_above_limit", "id_field", "doc2").get().status()
        );

        // Dynamic mapping: add a new field
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName).setSource("field", "dynamic", "id_field", "doc3", "new_dynamic_field", "hello").get().status()
        );

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        // Verify doc count via stats
        assertDocCount(indexName, 3);

        // Verify parquet contains expected fields
        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> parquetRows = readAllParquetRows(parquetDir, shard);
        assertEquals(3, parquetRows.size());

        // id_field should be in all rows
        assertEquals(3, parquetRows.stream().filter(r -> r.containsKey("id_field")).count());

        // Verify ignore_above behavior:
        // - doc1 ("short", len=5 <= 10): field should have the normalized value
        // - doc2 ("this_exceeds_ignore_above_limit", len=32 > 10): field should be null/absent,
        // but _ignored_source.field should store the raw text value
        Map<String, Object> doc1Row = parquetRows.stream().filter(r -> "doc1".equals(r.get("id_field"))).findFirst().orElseThrow();
        Map<String, Object> doc2Row = parquetRows.stream().filter(r -> "doc2".equals(r.get("id_field"))).findFirst().orElseThrow();

        // doc1: value within ignore_above — field is stored, no source keyword field needed
        assertEquals("short", doc1Row.get("field"));

        // doc2: value exceeds ignore_above — field is null (ignored), source keyword stores raw value
        assertNull("field should be null for doc2 (exceeds ignore_above)", doc2Row.get("field"));
        assertEquals(
            "sourceKeywordFieldType should store raw value for doc2",
            "this_exceeds_ignore_above_limit",
            doc2Row.get("_ignored_source.field")
        );

        // new_dynamic_field should appear in parquet (at least 1 row has it)
        assertTrue(parquetRows.stream().anyMatch(r -> r.containsKey("new_dynamic_field")));

        // Verify lucene has expected indexed fields
        Path luceneDir = shard.shardPath().resolveIndex();
        Set<String> luceneFields = getLuceneFields(luceneDir);
        assertTrue("Lucene should have field 'field'", luceneFields.contains("field"));
        assertTrue("Lucene should have field 'id_field'", luceneFields.contains("id_field"));
        assertTrue("Lucene should have dynamic field 'new_dynamic_field'", luceneFields.contains("new_dynamic_field"));
    }

    /**
     * Tests that a keyword field with a normalizer stores the raw value in sourceKeywordFieldType
     * even when the value is within ignore_above. The normalized value goes into the main field,
     * and the original un-normalized value is stored separately for source derivation.
     */
    public void testKeywordNormalizerStoresSourceSeparately() throws Exception {
        startCluster();
        String indexName = "test-keyword-normalizer-verify";

        Settings settings = Settings.builder()
            .put(dfaSettings())
            .put("index.analysis.normalizer.my_lower.type", "custom")
            .putList("index.analysis.normalizer.my_lower.filter", "lowercase")
            .build();

        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"field\": { \"type\": \"keyword\", \"normalizer\": \"my_lower\" },\n"
            + "    \"id_field\": { \"type\": \"keyword\" }\n"
            + "  }\n"
            + "}";

        CreateIndexResponse response = client().admin().indices().prepareCreate(indexName).setSettings(settings).setMapping(mapping).get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);

        // Index doc — value is within any ignore_above but normalizer transforms it
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("field", "Hello", "id_field", "doc1").get().status());
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("field", "WORLD", "id_field", "doc2").get().status());

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        assertDocCount(indexName, 2);

        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> parquetRows = readAllParquetRows(parquetDir, shard);
        assertEquals(2, parquetRows.size());

        Map<String, Object> doc1Row = parquetRows.stream().filter(r -> "doc1".equals(r.get("id_field"))).findFirst().orElseThrow();
        Map<String, Object> doc2Row = parquetRows.stream().filter(r -> "doc2".equals(r.get("id_field"))).findFirst().orElseThrow();

        // Main field stores the normalized (lowercased) value
        assertEquals("hello", doc1Row.get("field"));
        assertEquals("world", doc2Row.get("field"));

        // sourceKeywordFieldType stores the original un-normalized value for source derivation
        assertEquals("Hello", doc1Row.get("_ignored_source.field"));
        assertEquals("WORLD", doc2Row.get("_ignored_source.field"));
    }

    /**
     * Tests that a keyword field with a normalizer stores the raw value in sourceKeywordFieldType
     * even when the value is within ignore_above. The normalized value goes into the main field,
     * and the original un-normalized value is stored separately for source derivation.
     */
    public void testKeywordNormalizerStoresSourceSeparatelyWhenDynamicFieldIsPresent() throws Exception {
        startCluster();
        String indexName = "test-keyword-normalizer-verify-dynamic-field";

        Settings settings = Settings.builder()
            .put(dfaSettings())
            .put("index.analysis.normalizer.my_lower.type", "custom")
            .putList("index.analysis.normalizer.my_lower.filter", "lowercase")
            .build();

        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"field\": { \"type\": \"keyword\", \"normalizer\": \"my_lower\" }\n"
            + "  }\n"
            + "}";

        CreateIndexResponse response = client().admin().indices().prepareCreate(indexName).setSettings(settings).setMapping(mapping).get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);

        // Index doc — value is within any ignore_above but normalizer transforms it
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("field", "Hello", "id_field1", "doc1").get().status());
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("field", "WORLD", "id_field2", "doc2").get().status());

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        assertDocCount(indexName, 2);

        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> parquetRows = readAllParquetRows(parquetDir, shard);
        assertEquals(2, parquetRows.size());

        Map<String, Object> doc1Row = parquetRows.stream().filter(r -> "doc1".equals(r.get("id_field1"))).findFirst().orElseThrow();
        Map<String, Object> doc2Row = parquetRows.stream().filter(r -> "doc2".equals(r.get("id_field2"))).findFirst().orElseThrow();

        // Main field stores the normalized (lowercased) value
        assertEquals("hello", doc1Row.get("field"));
        assertEquals("world", doc2Row.get("field"));

        // sourceKeywordFieldType stores the original un-normalized value for source derivation
        assertEquals("Hello", doc1Row.get("_ignored_source.field"));
        assertEquals("WORLD", doc2Row.get("_ignored_source.field"));
    }

    /**
     * Tests multi-field text+keyword indexing and verifies both parquet and lucene structures.
     * Also adds a dynamic field and verifies old+new docs coexist.
     */
    public void testMultiFieldIndexAndVerify() throws Exception {
        startCluster();
        String indexName = "test-multi-field-verify";
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"content\": {\n"
            + "      \"type\": \"text\",\n"
            + "      \"fields\": {\n"
            + "        \"raw\": { \"type\": \"keyword\" }\n"
            + "      }\n"
            + "    },\n"
            + "    \"tag\": { \"type\": \"keyword\" }\n"
            + "  }\n"
            + "}";
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(dfaSettings())
            .setMapping(mapping)
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);

        // Index initial docs
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName).setSource("content", "hello world", "tag", "greeting").get().status()
        );
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("content", "foo bar", "tag", "test").get().status());

        // Dynamic mapping: add new field
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName).setSource("content", "dynamic doc", "tag", "new", "extra_field", 42).get().status()
        );

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        // Verify doc count
        assertDocCount(indexName, 3);

        // Verify parquet
        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> parquetRows = readAllParquetRows(parquetDir, shard);
        assertEquals(3, parquetRows.size());
        assertTrue(parquetRows.stream().anyMatch(r -> r.containsKey("content")));
        assertTrue(parquetRows.stream().anyMatch(r -> r.containsKey("tag")));
        assertTrue(parquetRows.stream().anyMatch(r -> r.containsKey("extra_field")));

        // Verify lucene has text field and keyword sub-field
        Path luceneDir = shard.shardPath().resolveIndex();
        Set<String> luceneFields = getLuceneFields(luceneDir);
        assertTrue("Lucene should have 'content'", luceneFields.contains("content"));
        assertTrue("Lucene should have 'content.raw'", luceneFields.contains("content.raw"));
        assertTrue("Lucene should have 'tag'", luceneFields.contains("tag"));
    }

    /**
     * Tests that multiple supported field types can be indexed together and verified.
     * Also adds a dynamic field and verifies old+new docs coexist.
     */
    public void testMultipleFieldTypesIndexAndVerify() throws Exception {
        startCluster();
        String indexName = "test-multi-types-verify";
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(dfaSettings())
            .setMapping("f_long", "type=long", "f_keyword", "type=keyword", "f_date", "type=date", "f_text", "type=text")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);

        // Index initial docs
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName)
                .setSource("f_long", 100L, "f_keyword", "alpha", "f_date", "2024-01-01", "f_text", "some text")
                .get()
                .status()
        );
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName)
                .setSource("f_long", 200L, "f_keyword", "beta", "f_date", "2024-06-15", "f_text", "more text")
                .get()
                .status()
        );

        // Dynamic mapping: add new field
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName)
                .setSource("f_long", 300L, "f_keyword", "gamma", "f_date", "2024-12-31", "f_text", "final", "dyn_bool", true)
                .get()
                .status()
        );

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        // Verify doc count
        assertDocCount(indexName, 3);

        // Verify parquet
        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> parquetRows = readAllParquetRows(parquetDir, shard);
        assertEquals(3, parquetRows.size());
        assertTrue(parquetRows.stream().anyMatch(r -> r.containsKey("f_long")));
        assertTrue(parquetRows.stream().anyMatch(r -> r.containsKey("f_keyword")));
        assertTrue(parquetRows.stream().anyMatch(r -> r.containsKey("f_date")));
        assertTrue(parquetRows.stream().anyMatch(r -> r.containsKey("f_text")));
        assertTrue(parquetRows.stream().anyMatch(r -> r.containsKey("dyn_bool")));

        // Verify lucene
        Path luceneDir = shard.shardPath().resolveIndex();
        Set<String> luceneFields = getLuceneFields(luceneDir);
        assertTrue("Lucene should have 'f_keyword'", luceneFields.contains("f_keyword"));
        assertTrue("Lucene should have 'f_text'", luceneFields.contains("f_text"));
    }

    // === MAPPING UPDATE FAILURE TEST ===

    /**
     * Tests that index creation succeeds, a document can be added, but a mapping update
     * with an unsupported field type causes the shard to fail when applying the mapping.
     */
    public void testMappingUpdateFailsWithUnsupportedField() throws Exception {
        startCluster();
        String indexName = "test-mapping-update-fail";
        assertIndexCreationSucceeds(indexName, "field", "type=keyword");

        // Index a document successfully
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("field", "value1").get().status());

        client().admin().indices().prepareRefresh(indexName).get();

        // Verify doc was indexed
        assertDocCount(indexName, 1);

        // Attempt to update mapping with an unsupported field type (geo_point).
        // The cluster-manager accepts the mapping update, but the data node fails
        // when trying to apply it (capability assignment fails for geo_point).
        client().admin()
            .indices()
            .preparePutMapping(indexName)
            .setSource("{\"properties\":{\"unsupported_geo\":{\"type\":\"geo_point\"}}}", MediaTypeRegistry.JSON)
            .get();

        // The shard should become unhealthy as the mapping update fails on the data node.
        // Wait for the cluster to detect the failure.
        assertBusy(() -> {
            String health = client().admin().cluster().prepareHealth(indexName).get().getStatus().name();
            assertTrue(
                "Index should be RED or YELLOW after unsupported mapping update, got: " + health,
                health.equals("RED") || health.equals("YELLOW")
            );
        });
    }

    /**
     * Tests dynamic mapping with a numeric field and verifies old+new docs coexist.
     */
    public void testDynamicMappingNumericField() throws Exception {
        startCluster();
        String indexName = "test-dynamic-mapping";
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(dfaSettings())
            .setMapping("name", "type=keyword")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);

        // Index initial doc
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("name", "first").get().status());

        // Index doc with dynamic numeric field
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("name", "second", "dynamic_num", 42).get().status());

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        // Verify both docs exist
        assertDocCount(indexName, 2);

        // Verify parquet has both docs
        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> parquetRows = readAllParquetRows(parquetDir, shard);
        assertEquals(2, parquetRows.size());
        assertTrue(parquetRows.stream().anyMatch(r -> r.containsKey("dynamic_num")));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers
    // ══════════════════════════════════════════════════════════════════════

    private void assertDocCount(String indexName, long expectedCount) {
        IndicesStatsResponse stats = client().admin().indices().prepareStats(indexName).clear().setDocs(true).get();
        assertEquals(expectedCount, stats.getIndex(indexName).getPrimaries().getDocs().getCount());
    }

    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private List<Map<String, Object>> readAllParquetRows(Path parquetDir, IndexShard shard) throws IOException {
        assertTrue("Parquet directory should exist", Files.isDirectory(parquetDir));
        List<Map<String, Object>> allRows = new ArrayList<>();
        try (GatedCloseable<CatalogSnapshot> snapshot = shard.getCatalogSnapshot()) {
            for (Segment segment : snapshot.get().getSegments()) {
                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
                if (wfs != null) {
                    for (String file : wfs.files()) {
                        Path filePath = parquetDir.resolve(file);
                        allRows.addAll(parseJsonRows(RustBridge.readAsJson(filePath.toString())));
                    }
                }
            }
        }
        return allRows;
    }

    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private List<Map<String, Object>> parseJsonRows(String json) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            return parser.list().stream().map(o -> (Map<String, Object>) o).collect(Collectors.toList());
        }
    }

    private Set<String> getLuceneFields(Path luceneDir) throws IOException {
        Set<String> allFields = new HashSet<>();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                for (FieldInfo fi : ctx.reader().getFieldInfos()) {
                    allFields.add(fi.name);
                }
            }
        }
        return allFields;
    }
}

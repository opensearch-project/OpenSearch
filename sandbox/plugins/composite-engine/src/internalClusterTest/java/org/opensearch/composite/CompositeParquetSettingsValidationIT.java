/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests verifying that Parquet field-level encoding/compression
 * and type-level encoding/compression validations are enforced at index creation time.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeParquetSettingsValidationIT extends AbstractCompositeEngineIT {

    private static final String INDEX_NAME = "test-settings-validation";

    // --- Field-level encoding validation ---

    public void testValidFieldEncodingAccepted() {
        createCompositeIndexWithSettings(
            Settings.builder()
                .putList("index.parquet.encoding.field", "value")
                .putList("index.parquet.encoding.value", "DELTA_BINARY_PACKED")
                .build()
        );
        ensureGreen(INDEX_NAME);
    }

    public void testInvalidFieldEncodingRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createCompositeIndexWithSettings(
                Settings.builder()
                    .putList("index.parquet.encoding.field", "value")
                    .putList("index.parquet.encoding.value", "INVALID_ENCODING")
                    .build()
            )
        );
        assertTrue(e.getMessage().contains("Invalid encoding"));
    }

    // --- Field-level compression validation ---

    public void testValidFieldCompressionAccepted() {
        createCompositeIndexWithSettings(
            Settings.builder()
                .putList("index.parquet.compression.field", "value")
                .putList("index.parquet.compression.value", "SNAPPY")
                .build()
        );
        ensureGreen(INDEX_NAME);
    }

    public void testInvalidFieldCompressionRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createCompositeIndexWithSettings(
                Settings.builder()
                    .putList("index.parquet.compression.field", "value")
                    .putList("index.parquet.compression.value", "INVALID_COMPRESSION")
                    .build()
            )
        );
        assertTrue(e.getMessage().contains("Invalid compression"));
    }

    // --- Type-level encoding validation ---

    public void testValidTypeEncodingAccepted() {
        createCompositeIndexWithNodeSettings(Settings.builder().put("parquet.type_encoding.int64.encoding", "DELTA_BINARY_PACKED").build());
    }

    public void testInvalidArrowTypeInTypeEncodingRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createCompositeIndexWithNodeSettings(Settings.builder().put("parquet.type_encoding.banana.encoding", "PLAIN").build())
        );
        assertTrue(e.getMessage().contains("Invalid arrow type"));
        assertTrue(e.getMessage().contains("banana"));
    }

    public void testInvalidEncodingValueInTypeEncodingRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createCompositeIndexWithNodeSettings(Settings.builder().put("parquet.type_encoding.int64.encoding", "INVALID").build())
        );
        assertTrue(e.getMessage().contains("Invalid encoding"));
    }

    public void testIncompatibleTypeLevelEncodingRejected() {
        // DELTA_BINARY_PACKED is only valid for integer types, not utf8
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createCompositeIndexWithNodeSettings(
                Settings.builder().put("parquet.type_encoding.utf8.encoding", "DELTA_BINARY_PACKED").build()
            )
        );
        assertTrue(e.getMessage().contains("not compatible") || e.getMessage().contains("not supported"));
    }

    // --- Type-level compression validation ---

    public void testValidTypeCompressionAccepted() {
        createCompositeIndexWithNodeSettings(Settings.builder().put("parquet.type_compression.utf8.compression", "ZSTD").build());
    }

    public void testInvalidArrowTypeInTypeCompressionRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createCompositeIndexWithNodeSettings(
                Settings.builder().put("parquet.type_compression.foobar.compression", "SNAPPY").build()
            )
        );
        assertTrue(e.getMessage().contains("Invalid arrow type"));
        assertTrue(e.getMessage().contains("foobar"));
    }

    public void testInvalidCompressionValueInTypeCompressionRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createCompositeIndexWithNodeSettings(
                Settings.builder().put("parquet.type_compression.utf8.compression", "INVALID").build()
            )
        );
        assertTrue(e.getMessage().contains("Invalid compression"));
    }

    // --- Encoding-type compatibility validation ---

    public void testIncompatibleEncodingForFieldTypeRejected() {
        // DELTA_BINARY_PACKED is only valid for integer types, not keyword (utf8)
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createCompositeIndexWithSettings(
                Settings.builder()
                    .putList("index.parquet.encoding.field", "name")
                    .putList("index.parquet.encoding.value", "DELTA_BINARY_PACKED")
                    .build()
            )
        );
        assertTrue(e.getMessage().contains("not compatible") || e.getMessage().contains("not supported"));
    }

    public void testCompatibleEncodingForFieldTypeAccepted() {
        // DELTA_BINARY_PACKED is valid for integer types
        createCompositeIndexWithSettings(
            Settings.builder()
                .putList("index.parquet.encoding.field", "value")
                .putList("index.parquet.encoding.value", "DELTA_BINARY_PACKED")
                .build()
        );
        ensureGreen(INDEX_NAME);
    }

    // --- Case insensitivity ---

    public void testEncodingCaseInsensitive() {
        createCompositeIndexWithSettings(
            Settings.builder()
                .putList("index.parquet.encoding.field", "value")
                .putList("index.parquet.encoding.value", "delta_binary_packed")
                .build()
        );
        ensureGreen(INDEX_NAME);
    }

    public void testCompressionCaseInsensitive() {
        createCompositeIndexWithSettings(
            Settings.builder()
                .putList("index.parquet.compression.field", "value")
                .putList("index.parquet.compression.value", "snappy")
                .build()
        );
        ensureGreen(INDEX_NAME);
    }

    // --- 3-tier precedence tests (field > type > global) ---

    /**
     * Verifies that global compression (index.parquet.compression_type) is applied
     * when no field-level or type-level config is set.
     */
    public void testGlobalCompressionApplied() throws IOException {
        createCompositeIndexWithSettings(Settings.builder().put("index.parquet.compression_type", "SNAPPY").build());
        ensureGreen(INDEX_NAME);
        indexDocs(INDEX_NAME, 5, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        Map<String, Object> colMeta = getColumnInfo(INDEX_NAME, "value");
        assertCompression(colMeta, "SNAPPY");
    }

    /**
     * Verifies that field-level encoding is applied and visible in parquet metadata.
     */
    public void testFieldLevelEncodingApplied() throws IOException {
        createCompositeIndexWithSettings(
            Settings.builder()
                .putList("index.parquet.encoding.field", "name")
                .putList("index.parquet.encoding.value", "DELTA_BYTE_ARRAY")
                .build()
        );
        ensureGreen(INDEX_NAME);
        indexDocs(INDEX_NAME, 5, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        Map<String, Object> colMeta = getColumnInfo(INDEX_NAME, "name");
        assertHasEncoding(colMeta, "DELTA_BYTE_ARRAY");
    }

    /**
     * Verifies that field-level compression is applied and visible in parquet metadata.
     */
    public void testFieldLevelCompressionApplied() throws IOException {
        createCompositeIndexWithSettings(
            Settings.builder()
                .putList("index.parquet.compression.field", "value")
                .putList("index.parquet.compression.value", "ZSTD")
                .build()
        );
        ensureGreen(INDEX_NAME);
        indexDocs(INDEX_NAME, 5, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        Map<String, Object> colMeta = getColumnInfo(INDEX_NAME, "value");
        assertCompression(colMeta, "ZSTD");
    }

    /**
     * Verifies field-level encoding with global compression.
     */
    public void testFieldLevelEncodingWithGlobalCompression() throws IOException {
        createCompositeIndexWithSettings(
            Settings.builder()
                .put("index.parquet.compression_type", "SNAPPY")
                .putList("index.parquet.encoding.field", "name")
                .putList("index.parquet.encoding.value", "DELTA_BYTE_ARRAY")
                .build()
        );
        ensureGreen(INDEX_NAME);
        indexDocs(INDEX_NAME, 5, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        Map<String, Object> nameMeta = getColumnInfo(INDEX_NAME, "name");
        assertHasEncoding(nameMeta, "DELTA_BYTE_ARRAY");
        assertCompression(nameMeta, "SNAPPY");
    }

    /**
     * Verifies field-level compression overrides global compression for a specific column.
     */
    public void testFieldLevelCompressionOverridesGlobal() throws IOException {
        createCompositeIndexWithSettings(
            Settings.builder()
                .put("index.parquet.compression_type", "LZ4_RAW")
                .putList("index.parquet.compression.field", "value")
                .putList("index.parquet.compression.value", "ZSTD")
                .build()
        );
        ensureGreen(INDEX_NAME);
        indexDocs(INDEX_NAME, 5, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        Map<String, Object> valueMeta = getColumnInfo(INDEX_NAME, "value");
        assertCompression(valueMeta, "ZSTD");

        Map<String, Object> nameMeta = getColumnInfo(INDEX_NAME, "name");
        assertCompression(nameMeta, "LZ4_RAW");
    }

    /**
     * Verifies multiple field-level settings can coexist on different columns.
     */
    public void testMultipleFieldLevelSettings() throws IOException {
        createCompositeIndexWithSettings(
            Settings.builder()
                .putList("index.parquet.encoding.field", "name", "value")
                .putList("index.parquet.encoding.value", "DELTA_BYTE_ARRAY", "DELTA_BINARY_PACKED")
                .putList("index.parquet.compression.field", "name", "value")
                .putList("index.parquet.compression.value", "SNAPPY", "ZSTD")
                .build()
        );
        ensureGreen(INDEX_NAME);
        indexDocs(INDEX_NAME, 5, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        Map<String, Object> nameMeta = getColumnInfo(INDEX_NAME, "name");
        assertHasEncoding(nameMeta, "DELTA_BYTE_ARRAY");
        assertCompression(nameMeta, "SNAPPY");

        Map<String, Object> valueMeta = getColumnInfo(INDEX_NAME, "value");
        assertHasEncoding(valueMeta, "DELTA_BINARY_PACKED");
        assertCompression(valueMeta, "ZSTD");
    }

    // --- Bloom filter tests ---

    /**
     * Verifies that bloom filters are disabled by default for all columns.
     */
    public void testBloomFilterDisabledByDefault() throws IOException {
        createCompositeIndexWithSettings(Settings.builder().build());
        ensureGreen(INDEX_NAME);
        indexDocs(INDEX_NAME, 5, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        Map<String, Object> nameMeta = getColumnInfo(INDEX_NAME, "name");
        assertEquals(Boolean.FALSE, nameMeta.get("bloom_filter"));

        Map<String, Object> valueMeta = getColumnInfo(INDEX_NAME, "value");
        assertEquals(Boolean.FALSE, valueMeta.get("bloom_filter"));
    }

    /**
     * Verifies that global bloom_filter_enabled=true enables bloom filters for all columns.
     */
    public void testGlobalBloomFilterEnabled() throws IOException {
        createCompositeIndexWithSettings(Settings.builder().put("index.parquet.bloom_filter_enabled", "true").build());
        ensureGreen(INDEX_NAME);
        indexDocs(INDEX_NAME, 5, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        Map<String, Object> nameMeta = getColumnInfo(INDEX_NAME, "name");
        assertEquals(Boolean.TRUE, nameMeta.get("bloom_filter"));

        Map<String, Object> valueMeta = getColumnInfo(INDEX_NAME, "value");
        assertEquals(Boolean.TRUE, valueMeta.get("bloom_filter"));
    }

    /**
     * Verifies that field-level bloom_filter_enabled=true enables bloom filter for that column only.
     */
    public void testFieldLevelBloomFilterEnabled() throws IOException {
        createCompositeIndexWithSettings(
            Settings.builder()
                .putList("index.parquet.bloom_filter_enabled.field", "name")
                .putList("index.parquet.bloom_filter_enabled.value", "true")
                .build()
        );
        ensureGreen(INDEX_NAME);
        indexDocs(INDEX_NAME, 5, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        Map<String, Object> nameMeta = getColumnInfo(INDEX_NAME, "name");
        assertEquals(Boolean.TRUE, nameMeta.get("bloom_filter"));

        // Other columns should still have no bloom filter (global default is disabled)
        Map<String, Object> valueMeta = getColumnInfo(INDEX_NAME, "value");
        assertEquals(Boolean.FALSE, valueMeta.get("bloom_filter"));
    }

    // --- Encoding/compression exhaustive combination tests ---

    /**
     * Loops over every (arrow_type, encoding) pair for type-level settings.
     * Valid combos must be accepted; incompatible combos must throw IllegalArgumentException.
     */
    public void testTypeLevelEncodingAllCombinations() {
        for (String arrowType : ParquetSettings.VALID_ARROW_TYPES) {
            for (String encoding : ParquetSettings.VALID_ENCODINGS) {
                ArrowType arrowTypeInstance = ParquetSettings.ARROW_TYPE_NAME_TO_INSTANCE.get(arrowType);
                boolean compatible = arrowTypeInstance == null || ParquetSettings.isEncodingValidForArrowType(encoding, arrowTypeInstance);
                Settings nodeSettings = Settings.builder().put("parquet.type_encoding." + arrowType + ".encoding", encoding).build();
                if (compatible) {
                    // Should be accepted — no exception
                    client().admin().cluster().prepareUpdateSettings().setTransientSettings(nodeSettings).get();
                } else {
                    IllegalArgumentException e = expectThrows(
                        IllegalArgumentException.class,
                        () -> client().admin().cluster().prepareUpdateSettings().setTransientSettings(nodeSettings).get()
                    );
                    assertTrue(
                        "Expected incompatibility error for type=" + arrowType + " encoding=" + encoding + ", got: " + e.getMessage(),
                        e.getMessage().contains("not compatible")
                    );
                }
            }
        }
    }

    /**
     * Loops over every (field, encoding) pair for field-level settings.
     * Uses keyword (utf8) and integer (int32) fields to cover string and numeric types.
     * Valid combos must be accepted at index creation; incompatible combos must throw IllegalArgumentException.
     */
    public void testFieldLevelEncodingAllCombinations() {
        // field -> arrow type instance for compatibility check
        Map<String, ArrowType> fieldToArrowType = Map.of(
            "name",
            ParquetSettings.ARROW_TYPE_NAME_TO_INSTANCE.get("utf8"),
            "value",
            ParquetSettings.ARROW_TYPE_NAME_TO_INSTANCE.get("int32")
        );
        for (Map.Entry<String, ArrowType> fieldEntry : fieldToArrowType.entrySet()) {
            String field = fieldEntry.getKey();
            ArrowType arrowType = fieldEntry.getValue();
            for (String encoding : ParquetSettings.VALID_ENCODINGS) {
                boolean compatible = ParquetSettings.isEncodingValidForArrowType(encoding, arrowType);
                Settings indexSettings = Settings.builder()
                    .putList("index.parquet.encoding.field", field)
                    .putList("index.parquet.encoding.value", encoding)
                    .build();
                if (compatible) {
                    createCompositeIndexWithSettings(indexSettings);
                    client().admin().indices().prepareDelete(INDEX_NAME).get();
                } else {
                    IllegalArgumentException e = expectThrows(
                        IllegalArgumentException.class,
                        () -> createCompositeIndexWithSettings(indexSettings)
                    );
                    assertTrue(
                        "Expected incompatibility error for field=" + field + " encoding=" + encoding + ", got: " + e.getMessage(),
                        e.getMessage().contains("not compatible") || e.getMessage().contains("not supported")
                    );
                }
            }
        }
    }

    /**
     * Loops over every (arrow_type, compression) pair for type-level settings.
     * All compressions are valid for all types — every combo must be accepted.
     */
    public void testTypeLevelCompressionAllCombinations() {
        for (String arrowType : ParquetSettings.VALID_ARROW_TYPES) {
            for (String compression : ParquetSettings.VALID_COMPRESSIONS) {
                Settings nodeSettings = Settings.builder()
                    .put("parquet.type_compression." + arrowType + ".compression", compression)
                    .build();
                // All combos valid — must not throw
                client().admin().cluster().prepareUpdateSettings().setTransientSettings(nodeSettings).get();
            }
        }
    }

    /**
     * Loops over every (field, compression) pair for field-level settings.
     * All compressions are valid for all field types — every combo must be accepted.
     */
    public void testFieldLevelCompressionAllCombinations() {
        for (String field : List.of("name", "value")) {
            for (String compression : ParquetSettings.VALID_COMPRESSIONS) {
                Settings indexSettings = Settings.builder()
                    .putList("index.parquet.compression.field", field)
                    .putList("index.parquet.compression.value", compression)
                    .build();
                // All combos valid — must not throw
                createCompositeIndexWithSettings(indexSettings);
                client().admin().indices().prepareDelete(INDEX_NAME).get();
            }
        }
    }

    // --- Helpers ---

    private void createCompositeIndexWithSettings(Settings extraSettings) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .put(extraSettings);

        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(builder)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
    }

    private void createCompositeIndexWithNodeSettings(Settings nodeSettings) {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(nodeSettings).get();

        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats");

        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(builder)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getColumnInfo(String indexName, String columnName) throws IOException {
        String json = getFirstFileColumnMetadata(indexName);
        // Extract the JSON object for the specific column
        String key = "\"" + columnName + "\":{";
        int start = json.indexOf(key);
        assertFalse("Column '" + columnName + "' not found in metadata: " + json, start == -1);
        int braceStart = json.indexOf('{', start + key.length() - 1);
        int braceEnd = json.indexOf('}', braceStart);
        String colJson = json.substring(braceStart, braceEnd + 1);

        // Parse encodings list
        int encStart = colJson.indexOf('[');
        int encEnd = colJson.indexOf(']');
        String encodingsStr = colJson.substring(encStart + 1, encEnd);
        List<String> encodings = new ArrayList<>();
        for (String enc : encodingsStr.split(",")) {
            encodings.add(enc.trim().replace("\"", ""));
        }

        // Parse compression
        String compKey = "\"compression\":\"";
        int compStart = colJson.indexOf(compKey) + compKey.length();
        int compEnd = colJson.indexOf('"', compStart);
        String compression = colJson.substring(compStart, compEnd);

        // Parse bloom_filter
        String bfKey = "\"bloom_filter\":";
        int bfStart = colJson.indexOf(bfKey);
        boolean hasBloomFilter = false;
        if (bfStart != -1) {
            String bfVal = colJson.substring(bfStart + bfKey.length()).split("[,}]")[0].trim();
            hasBloomFilter = "true".equals(bfVal);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("encodings", encodings);
        result.put("compression", compression);
        result.put("bloom_filter", hasBloomFilter);
        return result;
    }

    @SuppressWarnings("unchecked")
    private void assertHasEncoding(Map<String, Object> colMeta, String expectedEncoding) {
        List<String> encodings = (List<String>) colMeta.get("encodings");
        assertTrue("Expected encoding '" + expectedEncoding + "' in " + encodings, encodings.contains(expectedEncoding));
    }

    private void assertCompression(Map<String, Object> colMeta, String expectedPrefix) {
        String compression = (String) colMeta.get("compression");
        assertTrue(
            "Expected compression starting with '" + expectedPrefix + "', got: " + compression,
            compression.startsWith(expectedPrefix)
        );
    }

    private String getFirstFileColumnMetadata(String indexName) throws IOException {
        CatalogSnapshot snapshot = acquireAndGetSnapshot(indexName);
        Path parquetDir = getPrimaryShard(indexName).shardPath().getDataPath().resolve("parquet");

        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            if (wfs != null) {
                for (String file : wfs.files()) {
                    Path filePath = parquetDir.resolve(file);
                    if (Files.exists(filePath)) {
                        return RustBridge.getColumnMetadata(filePath.toString());
                    }
                }
            }
        }
        fail("No parquet file found for index: " + indexName);
        return null;
    }
}

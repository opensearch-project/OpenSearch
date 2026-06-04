/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests verifying 3-tier settings priority: field-level > type-level > global.
 * Uses numDataNodes=0 so nodes can be started with static type-level node settings.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeParquet3TierSettingsIT extends AbstractCompositeEngineIT {

    private static final String INDEX_NAME = "test-3tier";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    // --- Bloom filter 3-tier tests ---

    /**
     * Type-level bloom filter enabled for utf8 enables bloom on keyword columns only.
     */
    public void testTypeLevelBloomFilterEnabled() throws IOException {
        startCluster(Settings.builder().put("parquet.type_bloom_filter.utf8.enabled", "true").build());
        createIndex(Settings.EMPTY);
        indexAndFlush();

        assertEquals(Boolean.TRUE, getColumnInfo(INDEX_NAME, "name").get("bloom_filter"));
        assertEquals(Boolean.FALSE, getColumnInfo(INDEX_NAME, "value").get("bloom_filter"));
    }

    /**
     * Field-level bloom_filter_enabled=false overrides type-level enabled=true.
     */
    public void testFieldLevelBloomFilterOverridesTypeLevel() throws IOException {
        startCluster(Settings.builder().put("parquet.type_bloom_filter.utf8.enabled", "true").build());
        createIndex(
            Settings.builder()
                .putList("index.parquet.bloom_filter_enabled.field", "name")
                .putList("index.parquet.bloom_filter_enabled.value", "false")
                .build()
        );
        indexAndFlush();

        assertEquals(Boolean.FALSE, getColumnInfo(INDEX_NAME, "name").get("bloom_filter"));
    }

    /**
     * Field-level bloom_filter_enabled=true overrides global disabled (default).
     */
    public void testFieldLevelBloomFilterOverridesGlobal() throws IOException {
        startCluster(Settings.EMPTY);
        createIndex(
            Settings.builder()
                .putList("index.parquet.bloom_filter_enabled.field", "name")
                .putList("index.parquet.bloom_filter_enabled.value", "true")
                .build()
        );
        indexAndFlush();

        assertEquals(Boolean.TRUE, getColumnInfo(INDEX_NAME, "name").get("bloom_filter"));
        assertEquals(Boolean.FALSE, getColumnInfo(INDEX_NAME, "value").get("bloom_filter"));
    }

    /**
     * Type-level enabled=true overrides global disabled (default) for int32.
     */
    public void testTypeLevelBloomFilterOverridesGlobal() throws IOException {
        startCluster(Settings.builder().put("parquet.type_bloom_filter.int32.enabled", "true").build());
        createIndex(Settings.EMPTY);
        indexAndFlush();

        assertEquals(Boolean.TRUE, getColumnInfo(INDEX_NAME, "value").get("bloom_filter"));
        assertEquals(Boolean.FALSE, getColumnInfo(INDEX_NAME, "name").get("bloom_filter"));
    }

    /**
     * Full 3-tier: field=true overrides type=false overrides global=false.
     */
    public void testFullThreeTierBloomFilter() throws IOException {
        startCluster(Settings.builder().put("parquet.type_bloom_filter.utf8.enabled", "false").build());
        createIndex(
            Settings.builder()
                .putList("index.parquet.bloom_filter_enabled.field", "name")
                .putList("index.parquet.bloom_filter_enabled.value", "true")
                .build()
        );
        indexAndFlush();

        assertEquals(Boolean.TRUE, getColumnInfo(INDEX_NAME, "name").get("bloom_filter"));
    }

    // --- Encoding 3-tier tests ---

    /**
     * Type-level encoding for int32 applies to integer columns.
     */
    public void testTypeLevelEncodingApplied() throws IOException {
        startCluster(Settings.builder().put("parquet.type_encoding.int32.encoding", "DELTA_BINARY_PACKED").build());
        createIndex(Settings.EMPTY);
        indexAndFlush();

        assertHasEncoding(getColumnInfo(INDEX_NAME, "value"), "DELTA_BINARY_PACKED");
    }

    /**
     * Field-level encoding overrides type-level encoding.
     */
    public void testFieldLevelEncodingOverridesTypeLevel() throws IOException {
        // Type-level sets DELTA_BINARY_PACKED for int32
        startCluster(Settings.builder().put("parquet.type_encoding.int32.encoding", "DELTA_BINARY_PACKED").build());
        // Field-level sets PLAIN for "value"
        createIndex(
            Settings.builder().putList("index.parquet.encoding.field", "value").putList("index.parquet.encoding.value", "PLAIN").build()
        );
        indexAndFlush();

        assertHasEncoding(getColumnInfo(INDEX_NAME, "value"), "PLAIN");
        assertDoesNotHaveEncoding(getColumnInfo(INDEX_NAME, "value"), "DELTA_BINARY_PACKED");
    }

    /**
     * Type-level encoding for utf8 applies to keyword columns.
     */
    public void testTypeLevelEncodingForUtf8() throws IOException {
        startCluster(Settings.builder().put("parquet.type_encoding.utf8.encoding", "DELTA_BYTE_ARRAY").build());
        createIndex(Settings.EMPTY);
        indexAndFlush();

        assertHasEncoding(getColumnInfo(INDEX_NAME, "name"), "DELTA_BYTE_ARRAY");
    }

    // --- Compression 3-tier tests ---

    /**
     * Type-level compression for utf8 applies to keyword columns.
     */
    public void testTypeLevelCompressionApplied() throws IOException {
        startCluster(Settings.builder().put("parquet.type_compression.utf8.compression", "SNAPPY").build());
        createIndex(Settings.EMPTY);
        indexAndFlush();

        assertCompression(getColumnInfo(INDEX_NAME, "name"), "SNAPPY");
    }

    /**
     * Field-level compression overrides type-level compression.
     */
    public void testFieldLevelCompressionOverridesTypeLevel() throws IOException {
        // Type-level sets SNAPPY for utf8
        startCluster(Settings.builder().put("parquet.type_compression.utf8.compression", "SNAPPY").build());
        // Field-level sets ZSTD for "name"
        createIndex(
            Settings.builder().putList("index.parquet.compression.field", "name").putList("index.parquet.compression.value", "ZSTD").build()
        );
        indexAndFlush();

        assertCompression(getColumnInfo(INDEX_NAME, "name"), "ZSTD");
    }

    /**
     * Type-level compression overrides global compression.
     */
    public void testTypeLevelCompressionOverridesGlobal() throws IOException {
        startCluster(Settings.builder().put("parquet.type_compression.int32.compression", "ZSTD").build());
        // Global is LZ4_RAW (default)
        createIndex(Settings.EMPTY);
        indexAndFlush();

        assertCompression(getColumnInfo(INDEX_NAME, "value"), "ZSTD");
        // "name" (utf8) should still use global default LZ4_RAW
        assertCompression(getColumnInfo(INDEX_NAME, "name"), "LZ4_RAW");
    }

    /**
     * Full 3-tier compression: field > type > global.
     */
    public void testFullThreeTierCompression() throws IOException {
        // Type-level sets SNAPPY for utf8
        startCluster(Settings.builder().put("parquet.type_compression.utf8.compression", "SNAPPY").build());
        // Field-level overrides with ZSTD for "name", global is LZ4_RAW
        createIndex(
            Settings.builder().putList("index.parquet.compression.field", "name").putList("index.parquet.compression.value", "ZSTD").build()
        );
        indexAndFlush();

        assertCompression(getColumnInfo(INDEX_NAME, "name"), "ZSTD");
    }

    // --- All-type encoding/compression exhaustive tests ---

    /**
     * Type-level encoding set for all arrow types simultaneously.
     * Verifies each column in the parquet file has the expected encoding.
     */
    public void testTypeLevelEncodingAllValidCombinations() throws IOException {
        startCluster(
            Settings.builder()
                .put("parquet.type_encoding.utf8.encoding", "DELTA_BYTE_ARRAY")
                .put("parquet.type_encoding.int32.encoding", "DELTA_BINARY_PACKED")
                .put("parquet.type_encoding.int64.encoding", "DELTA_BINARY_PACKED")
                .put("parquet.type_encoding.float32.encoding", "BYTE_STREAM_SPLIT")
                .put("parquet.type_encoding.float64.encoding", "BYTE_STREAM_SPLIT")
                .put("parquet.type_encoding.boolean.encoding", "RLE")
                .put("parquet.type_encoding.binary.encoding", "DELTA_BYTE_ARRAY")
                .put("parquet.type_encoding.timestamp.encoding", "PLAIN")
                .build()
        );
        createAllTypeIndex(Settings.EMPTY);
        indexAllTypeDocs();

        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_utf8"), "DELTA_BYTE_ARRAY");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_int32"), "DELTA_BINARY_PACKED");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_int64"), "DELTA_BINARY_PACKED");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_float32"), "BYTE_STREAM_SPLIT");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_float64"), "BYTE_STREAM_SPLIT");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_boolean"), "RLE");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_binary"), "DELTA_BYTE_ARRAY");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_timestamp"), "PLAIN");
    }

    /**
     * Field-level encoding set per column for all types simultaneously.
     * Verifies each column in the parquet file has the expected encoding.
     */
    public void testFieldLevelEncodingAllValidCombinations() throws IOException {
        startCluster(Settings.EMPTY);
        createAllTypeIndex(
            Settings.builder()
                .putList(
                    "index.parquet.encoding.field",
                    "col_utf8",
                    "col_int32",
                    "col_int64",
                    "col_float32",
                    "col_float64",
                    "col_boolean",
                    "col_binary",
                    "col_timestamp"
                )
                .putList(
                    "index.parquet.encoding.value",
                    "DELTA_BYTE_ARRAY",
                    "DELTA_BINARY_PACKED",
                    "DELTA_BINARY_PACKED",
                    "BYTE_STREAM_SPLIT",
                    "BYTE_STREAM_SPLIT",
                    "RLE",
                    "DELTA_BYTE_ARRAY",
                    "PLAIN"
                )
                .build()
        );
        indexAllTypeDocs();

        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_utf8"), "DELTA_BYTE_ARRAY");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_int32"), "DELTA_BINARY_PACKED");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_int64"), "DELTA_BINARY_PACKED");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_float32"), "BYTE_STREAM_SPLIT");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_float64"), "BYTE_STREAM_SPLIT");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_boolean"), "RLE");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_binary"), "DELTA_BYTE_ARRAY");
        assertHasEncoding(getColumnInfo(INDEX_NAME, "col_timestamp"), "PLAIN");
    }

    /**
     * Type-level compression set for all arrow types simultaneously.
     * Verifies each column has the expected compression in the parquet file.
     */
    public void testTypeLevelCompressionAllCombinations() throws IOException {
        startCluster(
            Settings.builder()
                .put("parquet.type_compression.utf8.compression", "SNAPPY")
                .put("parquet.type_compression.int32.compression", "ZSTD")
                .put("parquet.type_compression.int64.compression", "GZIP")
                .put("parquet.type_compression.float32.compression", "BROTLI")
                .put("parquet.type_compression.float64.compression", "LZ4_RAW")
                .put("parquet.type_compression.boolean.compression", "SNAPPY")
                .put("parquet.type_compression.binary.compression", "ZSTD")
                .put("parquet.type_compression.timestamp.compression", "GZIP")
                .build()
        );
        createAllTypeIndex(Settings.EMPTY);
        indexAllTypeDocs();

        assertCompression(getColumnInfo(INDEX_NAME, "col_utf8"), "SNAPPY");
        assertCompression(getColumnInfo(INDEX_NAME, "col_int32"), "ZSTD");
        assertCompression(getColumnInfo(INDEX_NAME, "col_int64"), "GZIP");
        assertCompression(getColumnInfo(INDEX_NAME, "col_float32"), "BROTLI");
        assertCompression(getColumnInfo(INDEX_NAME, "col_float64"), "LZ4_RAW");
        assertCompression(getColumnInfo(INDEX_NAME, "col_boolean"), "SNAPPY");
        assertCompression(getColumnInfo(INDEX_NAME, "col_binary"), "ZSTD");
        assertCompression(getColumnInfo(INDEX_NAME, "col_timestamp"), "GZIP");
    }

    /**
     * Field-level compression set per column for all types simultaneously.
     * Verifies each column has the expected compression in the parquet file.
     */
    public void testFieldLevelCompressionAllCombinations() throws IOException {
        startCluster(Settings.EMPTY);
        createAllTypeIndex(
            Settings.builder()
                .putList(
                    "index.parquet.compression.field",
                    "col_utf8",
                    "col_int32",
                    "col_int64",
                    "col_float32",
                    "col_float64",
                    "col_boolean",
                    "col_binary",
                    "col_timestamp"
                )
                .putList("index.parquet.compression.value", "SNAPPY", "ZSTD", "GZIP", "BROTLI", "LZ4_RAW", "SNAPPY", "ZSTD", "GZIP")
                .build()
        );
        indexAllTypeDocs();

        assertCompression(getColumnInfo(INDEX_NAME, "col_utf8"), "SNAPPY");
        assertCompression(getColumnInfo(INDEX_NAME, "col_int32"), "ZSTD");
        assertCompression(getColumnInfo(INDEX_NAME, "col_int64"), "GZIP");
        assertCompression(getColumnInfo(INDEX_NAME, "col_float32"), "BROTLI");
        assertCompression(getColumnInfo(INDEX_NAME, "col_float64"), "LZ4_RAW");
        assertCompression(getColumnInfo(INDEX_NAME, "col_boolean"), "SNAPPY");
        assertCompression(getColumnInfo(INDEX_NAME, "col_binary"), "ZSTD");
        assertCompression(getColumnInfo(INDEX_NAME, "col_timestamp"), "GZIP");
    }

    // --- Helpers ---

    private void startCluster(Settings extraNodeSettings) {
        Settings nodeSettings = Settings.builder()
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(extraNodeSettings)
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
    }

    private void createIndex(Settings extraIndexSettings) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .put(extraIndexSettings);

        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(builder)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);
    }

    private void indexAndFlush() {
        indexDocs(INDEX_NAME, 5, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);
    }

    private void createAllTypeIndex(Settings extraIndexSettings) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .put(extraIndexSettings);

        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(builder)
            .setMapping(
                "col_utf8",
                "type=keyword",
                "col_int32",
                "type=integer",
                "col_int64",
                "type=long",
                "col_float32",
                "type=float",
                "col_float64",
                "type=double",
                "col_boolean",
                "type=boolean",
                "col_binary",
                "type=binary",
                "col_timestamp",
                "type=date"
            )
            .get();
        ensureGreen(INDEX_NAME);
    }

    private void indexAllTypeDocs() {
        for (int i = 0; i < 5; i++) {
            client().prepareIndex()
                .setIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource(
                    "col_utf8",
                    "val_" + i,
                    "col_int32",
                    i,
                    "col_int64",
                    (long) i,
                    "col_float32",
                    (float) i,
                    "col_float64",
                    (double) i,
                    "col_boolean",
                    i % 2 == 0,
                    "col_timestamp",
                    "2024-01-0" + (i + 1)
                )
                .get();
        }
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getColumnInfo(String indexName, String columnName) throws IOException {
        String json = getFirstFileColumnMetadata(indexName);
        String key = "\"" + columnName + "\":{";
        int start = json.indexOf(key);
        assertFalse("Column '" + columnName + "' not found in metadata: " + json, start == -1);
        int braceStart = json.indexOf('{', start + key.length() - 1);
        int braceEnd = json.indexOf('}', braceStart);
        String colJson = json.substring(braceStart, braceEnd + 1);

        Map<String, Object> result = new HashMap<>();

        // Parse encodings list
        int encStart = colJson.indexOf('[');
        int encEnd = colJson.indexOf(']');
        if (encStart != -1 && encEnd != -1) {
            String encodingsStr = colJson.substring(encStart + 1, encEnd);
            List<String> encodings = new ArrayList<>();
            for (String enc : encodingsStr.split(",")) {
                encodings.add(enc.trim().replace("\"", ""));
            }
            result.put("encodings", encodings);
        }

        // Parse compression
        String compKey = "\"compression\":\"";
        int compStart = colJson.indexOf(compKey);
        if (compStart != -1) {
            compStart += compKey.length();
            int compEnd = colJson.indexOf('"', compStart);
            result.put("compression", colJson.substring(compStart, compEnd));
        }

        // Parse bloom_filter
        String bfKey = "\"bloom_filter\":";
        int bfStart = colJson.indexOf(bfKey);
        boolean hasBloomFilter = false;
        if (bfStart != -1) {
            String bfVal = colJson.substring(bfStart + bfKey.length()).split("[,}]")[0].trim();
            hasBloomFilter = "true".equals(bfVal);
        }
        result.put("bloom_filter", hasBloomFilter);

        return result;
    }

    @SuppressWarnings("unchecked")
    private void assertHasEncoding(Map<String, Object> colMeta, String expectedEncoding) {
        List<String> encodings = (List<String>) colMeta.get("encodings");
        assertTrue("Expected encoding '" + expectedEncoding + "' in " + encodings, encodings.contains(expectedEncoding));
    }

    @SuppressWarnings("unchecked")
    private void assertDoesNotHaveEncoding(Map<String, Object> colMeta, String encoding) {
        List<String> encodings = (List<String>) colMeta.get("encodings");
        assertFalse("Did not expect encoding '" + encoding + "' in " + encodings, encodings.contains(encoding));
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

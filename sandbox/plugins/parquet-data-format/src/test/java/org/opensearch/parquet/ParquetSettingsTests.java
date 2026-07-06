/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class ParquetSettingsTests extends OpenSearchTestCase {

    // --- FIELD_SETTINGS validation tests ---

    public void testFieldSettingsValidEncoding() {
        Settings settings = Settings.builder()
            .putList("index.parquet.encoding.field", "name")
            .putList("index.parquet.encoding.value", "DELTA_BYTE_ARRAY")
            .build();
        Map<String, String> encodings = ParquetSettings.getFieldEncodings(settings);
        assertEquals("DELTA_BYTE_ARRAY", encodings.get("name"));
    }

    public void testFieldSettingsInvalidEncodingThrows() {
        Settings settings = Settings.builder()
            .putList("index.parquet.encoding.field", "name")
            .putList("index.parquet.encoding.value", "INVALID")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ParquetSettings.getFieldEncodings(settings));
        assertTrue(e.getMessage().contains("Invalid encoding"));
        assertTrue(e.getMessage().contains("INVALID"));
    }

    public void testFieldSettingsValidCompression() {
        Settings settings = Settings.builder()
            .putList("index.parquet.compression.field", "name")
            .putList("index.parquet.compression.value", "SNAPPY")
            .build();
        Map<String, String> compressions = ParquetSettings.getFieldCompressions(settings);
        assertEquals("SNAPPY", compressions.get("name"));
    }

    public void testFieldSettingsInvalidCompressionThrows() {
        Settings settings = Settings.builder()
            .putList("index.parquet.compression.field", "name")
            .putList("index.parquet.compression.value", "INVALID")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ParquetSettings.getFieldCompressions(settings));
        assertTrue(e.getMessage().contains("Invalid compression"));
        assertTrue(e.getMessage().contains("INVALID"));
    }

    // --- TYPE_ENCODING_SETTINGS validation tests ---

    public void testTypeEncodingValidTypeAndEncoding() {
        Settings settings = Settings.builder().put("parquet.type_encoding.int64.encoding", "DELTA_BINARY_PACKED").build();
        Map<String, String> encodings = ParquetSettings.getTypeEncodings(settings);
        assertEquals("DELTA_BINARY_PACKED", encodings.get("int64"));
    }

    public void testTypeEncodingInvalidArrowTypeThrows() {
        Settings settings = Settings.builder().put("parquet.type_encoding.banana.encoding", "PLAIN").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ParquetSettings.getTypeEncodings(settings));
        assertTrue(e.getMessage().contains("Invalid arrow type"));
        assertTrue(e.getMessage().contains("banana"));
    }

    public void testTypeEncodingInvalidEncodingValueThrows() {
        Settings settings = Settings.builder().put("parquet.type_encoding.int64.encoding", "INVALID").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ParquetSettings.getTypeEncodings(settings));
        assertTrue(e.getMessage().contains("Invalid encoding"));
        assertTrue(e.getMessage().contains("INVALID"));
    }

    // --- TYPE_COMPRESSION_SETTINGS validation tests ---

    public void testTypeCompressionValidTypeAndCompression() {
        Settings settings = Settings.builder().put("parquet.type_compression.utf8.compression", "ZSTD").build();
        Map<String, String> compressions = ParquetSettings.getTypeCompressions(settings);
        assertEquals("ZSTD", compressions.get("utf8"));
    }

    public void testTypeCompressionInvalidArrowTypeThrows() {
        Settings settings = Settings.builder().put("parquet.type_compression.foobar.compression", "SNAPPY").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ParquetSettings.getTypeCompressions(settings));
        assertTrue(e.getMessage().contains("Invalid arrow type"));
        assertTrue(e.getMessage().contains("foobar"));
    }

    public void testTypeCompressionInvalidCompressionValueThrows() {
        Settings settings = Settings.builder().put("parquet.type_compression.utf8.compression", "INVALID").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ParquetSettings.getTypeCompressions(settings));
        assertTrue(e.getMessage().contains("Invalid compression"));
        assertTrue(e.getMessage().contains("INVALID"));
    }

    // --- All valid arrow types accepted ---

    public void testAllValidArrowTypesAccepted() {
        for (String type : ParquetSettings.VALID_ARROW_TYPES) {
            Settings settings = Settings.builder().put("parquet.type_encoding." + type + ".encoding", "PLAIN").build();
            Map<String, String> encodings = ParquetSettings.getTypeEncodings(settings);
            assertEquals("PLAIN", encodings.get(type));
        }
    }

    // --- Encoding-type compatibility tests ---

    public void testEncodingCompatibilityDeltaBinaryPackedValidForInt() {
        Schema schema = new Schema(List.of(new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        Map<String, String> encodings = Map.of("age", "DELTA_BINARY_PACKED");
        ParquetSettings.validateFieldConfigurations(encodings, Map.of(), Map.of(), schema);
    }

    public void testEncodingCompatibilityDeltaBinaryPackedInvalidForUtf8() {
        Schema schema = new Schema(List.of(new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        Map<String, String> encodings = Map.of("name", "DELTA_BINARY_PACKED");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.validateFieldConfigurations(encodings, Map.of(), Map.of(), schema)
        );
        assertTrue(e.getMessage().contains("not compatible"));
    }

    public void testEncodingCompatibilityRleValidForBoolean() {
        Schema schema = new Schema(List.of(new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), null)));
        Map<String, String> encodings = Map.of("flag", "RLE");
        ParquetSettings.validateFieldConfigurations(encodings, Map.of(), Map.of(), schema);
    }

    public void testEncodingCompatibilityRleInvalidForInt() {
        Schema schema = new Schema(List.of(new Field("count", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        Map<String, String> encodings = Map.of("count", "RLE");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.validateFieldConfigurations(encodings, Map.of(), Map.of(), schema)
        );
        assertTrue(e.getMessage().contains("not compatible"));
    }

    public void testEncodingCompatibilityByteStreamSplitValidForFloat() {
        Schema schema = new Schema(
            List.of(new Field("score", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null))
        );
        Map<String, String> encodings = Map.of("score", "BYTE_STREAM_SPLIT");
        ParquetSettings.validateFieldConfigurations(encodings, Map.of(), Map.of(), schema);
    }

    public void testEncodingCompatibilityByteStreamSplitInvalidForUtf8() {
        Schema schema = new Schema(List.of(new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        Map<String, String> encodings = Map.of("name", "BYTE_STREAM_SPLIT");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.validateFieldConfigurations(encodings, Map.of(), Map.of(), schema)
        );
        assertTrue(e.getMessage().contains("not compatible"));
    }

    public void testEncodingCompatibilityDictionaryInvalidForBoolean() {
        Schema schema = new Schema(List.of(new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), null)));
        Map<String, String> encodings = Map.of("flag", "RLE_DICTIONARY");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.validateFieldConfigurations(encodings, Map.of(), Map.of(), schema)
        );
        assertTrue(e.getMessage().contains("not compatible"));
    }

    public void testEncodingCompatibilityFieldNotInSchemaThrows() {
        Schema schema = new Schema(List.of(new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        Map<String, String> encodings = Map.of("nonexistent", "PLAIN");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.validateFieldConfigurations(encodings, Map.of(), Map.of(), schema)
        );
        assertTrue(e.getMessage().contains("does not exist in mappings"));
    }

    // --- Case insensitivity tests ---

    public void testEncodingCaseInsensitive() {
        Settings settings = Settings.builder()
            .putList("index.parquet.encoding.field", "name")
            .putList("index.parquet.encoding.value", "delta_byte_array")
            .build();
        Map<String, String> encodings = ParquetSettings.getFieldEncodings(settings);
        assertEquals("DELTA_BYTE_ARRAY", encodings.get("name"));
    }

    public void testCompressionCaseInsensitive() {
        Settings settings = Settings.builder()
            .putList("index.parquet.compression.field", "name")
            .putList("index.parquet.compression.value", "snappy")
            .build();
        Map<String, String> compressions = ParquetSettings.getFieldCompressions(settings);
        assertEquals("SNAPPY", compressions.get("name"));
    }

    // --- Field-level bloom filter tests ---

    public void testFieldBloomFilterEnabled() {
        Settings settings = Settings.builder()
            .putList("index.parquet.bloom_filter_enabled.field", "name", "value")
            .putList("index.parquet.bloom_filter_enabled.value", "true", "false")
            .build();
        Map<String, Boolean> result = ParquetSettings.getFieldBloomFilterEnabled(settings);
        assertEquals(Boolean.TRUE, result.get("name"));
        assertEquals(Boolean.FALSE, result.get("value"));
    }

    public void testFieldBloomFilterFpp() {
        Settings settings = Settings.builder().put("index.parquet.field.name.bloom_filter_fpp", "0.01").build();
        Map<String, Double> result = ParquetSettings.getFieldBloomFilterFpp(settings);
        assertEquals(0.01, result.get("name"), 0.0001);
    }

    public void testFieldBloomFilterNdv() {
        Settings settings = Settings.builder().put("index.parquet.field.name.bloom_filter_ndv", "50000").build();
        Map<String, Long> result = ParquetSettings.getFieldBloomFilterNdv(settings);
        assertEquals(Long.valueOf(50000L), result.get("name"));
    }

    // --- Type-level bloom filter validation tests ---

    public void testTypeBloomFilterValidSettings() {
        Settings settings = Settings.builder()
            .put("parquet.type_bloom_filter.utf8.enabled", "true")
            .put("parquet.type_bloom_filter.utf8.fpp", "0.05")
            .put("parquet.type_bloom_filter.utf8.ndv", "100000")
            .build();
        Map<String, Boolean> enabled = ParquetSettings.getTypeBloomFilterEnabled(settings);
        Map<String, Double> fpp = ParquetSettings.getTypeBloomFilterFpp(settings);
        Map<String, Long> ndv = ParquetSettings.getTypeBloomFilterNdv(settings);
        assertEquals(Boolean.TRUE, enabled.get("utf8"));
        assertEquals(0.05, fpp.get("utf8"), 0.0001);
        assertEquals(Long.valueOf(100000L), ndv.get("utf8"));
    }

    public void testTypeBloomFilterInvalidArrowTypeThrows() {
        Settings settings = Settings.builder().put("parquet.type_bloom_filter.banana.enabled", "true").build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.getTypeBloomFilterEnabled(settings)
        );
        assertTrue(e.getMessage().contains("Invalid arrow type"));
    }

    public void testTypeBloomFilterInvalidFppThrows() {
        Settings settings = Settings.builder().put("parquet.type_bloom_filter.utf8.fpp", "1.5").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ParquetSettings.getTypeBloomFilterFpp(settings));
        assertTrue(e.getMessage().contains("fpp"));
    }

    public void testTypeBloomFilterInvalidNdvThrows() {
        Settings settings = Settings.builder().put("parquet.type_bloom_filter.utf8.ndv", "-1").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ParquetSettings.getTypeBloomFilterNdv(settings));
        assertTrue(e.getMessage().contains("ndv"));
    }

    public void testTypeBloomFilterInvalidEnabledValueThrows() {
        Settings settings = Settings.builder().put("parquet.type_bloom_filter.utf8.enabled", "maybe").build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.getTypeBloomFilterEnabled(settings)
        );
        assertTrue(e.getMessage().contains("enabled"));
    }

    public void testTypeBloomFilterMultipleTypes() {
        Settings settings = Settings.builder()
            .put("parquet.type_bloom_filter.utf8.enabled", "true")
            .put("parquet.type_bloom_filter.utf8.fpp", "0.01")
            .put("parquet.type_bloom_filter.int64.enabled", "false")
            .put("parquet.type_bloom_filter.int64.ndv", "50000")
            .build();
        Map<String, Boolean> enabled = ParquetSettings.getTypeBloomFilterEnabled(settings);
        Map<String, Double> fpp = ParquetSettings.getTypeBloomFilterFpp(settings);
        Map<String, Long> ndv = ParquetSettings.getTypeBloomFilterNdv(settings);
        assertEquals(Boolean.TRUE, enabled.get("utf8"));
        assertEquals(Boolean.FALSE, enabled.get("int64"));
        assertEquals(0.01, fpp.get("utf8"), 0.0001);
        assertEquals(Long.valueOf(50000L), ndv.get("int64"));
    }

    // --- RAM-scaled batch-size default tests ---

    private static final long GIB = 1024L * 1024 * 1024;

    public void testMaxRowsPerVsrDefaultScalesWithRam() {
        int base = ParquetSettings.DEFAULT_MAX_ROWS_PER_VSR;       // 65,536 @ 64 GiB
        int floor = ParquetSettings.MAX_ROWS_PER_VSR_FLOOR;        // 8,192
        int ceil = ParquetSettings.MAX_ROWS_PER_VSR_CEIL;         // 131,072
        // Matrix (total RAM -> VSR rows)
        assertEquals(8_192, ParquetSettings.deriveBatchSizeDefault(8 * GIB, base, floor, ceil));
        assertEquals(16_384, ParquetSettings.deriveBatchSizeDefault(16 * GIB, base, floor, ceil));
        assertEquals(32_768, ParquetSettings.deriveBatchSizeDefault(32 * GIB, base, floor, ceil));
        assertEquals(65_536, ParquetSettings.deriveBatchSizeDefault(64 * GIB, base, floor, ceil)); // anchor
        assertEquals(131_072, ParquetSettings.deriveBatchSizeDefault(128 * GIB, base, floor, ceil));
        assertEquals(131_072, ParquetSettings.deriveBatchSizeDefault(256 * GIB, base, floor, ceil)); // capped
    }

    public void testMergeBatchSizeDefaultScalesWithRam() {
        int base = ParquetSettings.DEFAULT_MERGE_BATCH_SIZE;       // 100,000 @ 64 GiB
        int floor = ParquetSettings.MERGE_BATCH_SIZE_FLOOR;        // 12,500
        int ceil = ParquetSettings.MERGE_BATCH_SIZE_CEIL;         // 100,000
        assertEquals(12_500, ParquetSettings.deriveBatchSizeDefault(8 * GIB, base, floor, ceil));
        assertEquals(25_000, ParquetSettings.deriveBatchSizeDefault(16 * GIB, base, floor, ceil));
        assertEquals(50_000, ParquetSettings.deriveBatchSizeDefault(32 * GIB, base, floor, ceil));
        assertEquals(100_000, ParquetSettings.deriveBatchSizeDefault(64 * GIB, base, floor, ceil)); // anchor (== ceil)
        assertEquals(100_000, ParquetSettings.deriveBatchSizeDefault(128 * GIB, base, floor, ceil)); // capped
        assertEquals(100_000, ParquetSettings.deriveBatchSizeDefault(256 * GIB, base, floor, ceil)); // capped
    }

    public void testBatchSizeDefaultsClampToFloorAndCeiling() {
        int vsrBase = ParquetSettings.DEFAULT_MAX_ROWS_PER_VSR;
        int vsrFloor = ParquetSettings.MAX_ROWS_PER_VSR_FLOOR;
        int vsrCeil = ParquetSettings.MAX_ROWS_PER_VSR_CEIL;
        // Below anchor/8 -> clamps to floor; far above -> clamps to ceiling.
        assertEquals(vsrFloor, ParquetSettings.deriveBatchSizeDefault(2 * GIB, vsrBase, vsrFloor, vsrCeil));
        assertEquals(vsrCeil, ParquetSettings.deriveBatchSizeDefault(1024 * GIB, vsrBase, vsrFloor, vsrCeil));

        int mrgBase = ParquetSettings.DEFAULT_MERGE_BATCH_SIZE;
        int mrgFloor = ParquetSettings.MERGE_BATCH_SIZE_FLOOR;
        int mrgCeil = ParquetSettings.MERGE_BATCH_SIZE_CEIL;
        assertEquals(mrgFloor, ParquetSettings.deriveBatchSizeDefault(2 * GIB, mrgBase, mrgFloor, mrgCeil));
        assertEquals(mrgCeil, ParquetSettings.deriveBatchSizeDefault(1024 * GIB, mrgBase, mrgFloor, mrgCeil));
    }

    public void testSortInMemoryThresholdDefaultScalesWithRam() {
        int base = ParquetSettings.SORT_IN_MEMORY_THRESHOLD_BASE_MB;   // 32 MiB @ 64 GiB
        int floor = ParquetSettings.SORT_IN_MEMORY_THRESHOLD_FLOOR_MB; // 16 MiB
        int ceil = ParquetSettings.SORT_IN_MEMORY_THRESHOLD_CEIL_MB;  // 64 MiB
        // (total RAM -> sort threshold, in MiB)
        assertEquals(16, ParquetSettings.deriveBatchSizeDefault(8 * GIB, base, floor, ceil));   // 4 -> floor
        assertEquals(16, ParquetSettings.deriveBatchSizeDefault(16 * GIB, base, floor, ceil));  // 8 -> floor
        assertEquals(16, ParquetSettings.deriveBatchSizeDefault(32 * GIB, base, floor, ceil));  // 16
        assertEquals(32, ParquetSettings.deriveBatchSizeDefault(64 * GIB, base, floor, ceil));  // anchor
        assertEquals(64, ParquetSettings.deriveBatchSizeDefault(128 * GIB, base, floor, ceil)); // 64 (ceil)
        assertEquals(64, ParquetSettings.deriveBatchSizeDefault(256 * GIB, base, floor, ceil)); // capped
    }

    public void testSortInMemoryThresholdFallsBackToBaseWhenRamUnavailable() {
        assertEquals(
            ParquetSettings.SORT_IN_MEMORY_THRESHOLD_BASE_MB,
            ParquetSettings.deriveBatchSizeDefault(
                -1,
                ParquetSettings.SORT_IN_MEMORY_THRESHOLD_BASE_MB,
                ParquetSettings.SORT_IN_MEMORY_THRESHOLD_FLOOR_MB,
                ParquetSettings.SORT_IN_MEMORY_THRESHOLD_CEIL_MB
            )
        );
    }

    public void testBatchSizeDefaultFallsBackToBaseWhenRamUnavailable() {
        assertEquals(
            ParquetSettings.DEFAULT_MAX_ROWS_PER_VSR,
            ParquetSettings.deriveBatchSizeDefault(
                -1,
                ParquetSettings.DEFAULT_MAX_ROWS_PER_VSR,
                ParquetSettings.MAX_ROWS_PER_VSR_FLOOR,
                ParquetSettings.MAX_ROWS_PER_VSR_CEIL
            )
        );
        assertEquals(
            ParquetSettings.DEFAULT_MERGE_BATCH_SIZE,
            ParquetSettings.deriveBatchSizeDefault(
                0,
                ParquetSettings.DEFAULT_MERGE_BATCH_SIZE,
                ParquetSettings.MERGE_BATCH_SIZE_FLOOR,
                ParquetSettings.MERGE_BATCH_SIZE_CEIL
            )
        );
    }
}

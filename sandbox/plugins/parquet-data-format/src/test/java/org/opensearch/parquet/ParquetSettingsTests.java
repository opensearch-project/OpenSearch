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
        Settings settings = Settings.builder().put("index.parquet.field.name.encoding", "DELTA_BYTE_ARRAY").build();
        Map<String, String> encodings = ParquetSettings.getFieldEncodings(settings);
        assertEquals("DELTA_BYTE_ARRAY", encodings.get("name"));
    }

    public void testFieldSettingsInvalidEncodingThrows() {
        Settings settings = Settings.builder().put("index.parquet.field.name.encoding", "INVALID").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ParquetSettings.getFieldEncodings(settings));
        assertTrue(e.getMessage().contains("Invalid encoding"));
        assertTrue(e.getMessage().contains("INVALID"));
    }

    public void testFieldSettingsValidCompression() {
        Settings settings = Settings.builder().put("index.parquet.field.name.compression", "SNAPPY").build();
        Map<String, String> compressions = ParquetSettings.getFieldCompressions(settings);
        assertEquals("SNAPPY", compressions.get("name"));
    }

    public void testFieldSettingsInvalidCompressionThrows() {
        Settings settings = Settings.builder().put("index.parquet.field.name.compression", "INVALID").build();
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
        ParquetSettings.validateEncodingTypeCompatibility(encodings, schema);
    }

    public void testEncodingCompatibilityDeltaBinaryPackedInvalidForUtf8() {
        Schema schema = new Schema(List.of(new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        Map<String, String> encodings = Map.of("name", "DELTA_BINARY_PACKED");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.validateEncodingTypeCompatibility(encodings, schema)
        );
        assertTrue(e.getMessage().contains("not compatible"));
    }

    public void testEncodingCompatibilityRleValidForBoolean() {
        Schema schema = new Schema(List.of(new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), null)));
        Map<String, String> encodings = Map.of("flag", "RLE");
        ParquetSettings.validateEncodingTypeCompatibility(encodings, schema);
    }

    public void testEncodingCompatibilityRleInvalidForInt() {
        Schema schema = new Schema(List.of(new Field("count", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        Map<String, String> encodings = Map.of("count", "RLE");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.validateEncodingTypeCompatibility(encodings, schema)
        );
        assertTrue(e.getMessage().contains("not compatible"));
    }

    public void testEncodingCompatibilityByteStreamSplitValidForFloat() {
        Schema schema = new Schema(
            List.of(new Field("score", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null))
        );
        Map<String, String> encodings = Map.of("score", "BYTE_STREAM_SPLIT");
        ParquetSettings.validateEncodingTypeCompatibility(encodings, schema);
    }

    public void testEncodingCompatibilityByteStreamSplitInvalidForUtf8() {
        Schema schema = new Schema(List.of(new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        Map<String, String> encodings = Map.of("name", "BYTE_STREAM_SPLIT");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.validateEncodingTypeCompatibility(encodings, schema)
        );
        assertTrue(e.getMessage().contains("not compatible"));
    }

    public void testEncodingCompatibilityDictionaryInvalidForBoolean() {
        Schema schema = new Schema(List.of(new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), null)));
        Map<String, String> encodings = Map.of("flag", "RLE_DICTIONARY");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.validateEncodingTypeCompatibility(encodings, schema)
        );
        assertTrue(e.getMessage().contains("not compatible"));
    }

    public void testEncodingCompatibilityFieldNotInSchemaThrows() {
        Schema schema = new Schema(List.of(new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        Map<String, String> encodings = Map.of("nonexistent", "PLAIN");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetSettings.validateEncodingTypeCompatibility(encodings, schema)
        );
        assertTrue(e.getMessage().contains("does not exist in mappings"));
    }

    // --- Case insensitivity tests ---

    public void testEncodingCaseInsensitive() {
        Settings settings = Settings.builder().put("index.parquet.field.name.encoding", "delta_byte_array").build();
        Map<String, String> encodings = ParquetSettings.getFieldEncodings(settings);
        assertEquals("DELTA_BYTE_ARRAY", encodings.get("name"));
    }

    public void testCompressionCaseInsensitive() {
        Settings settings = Settings.builder().put("index.parquet.field.name.compression", "snappy").build();
        Map<String, String> compressions = ParquetSettings.getFieldCompressions(settings);
        assertEquals("SNAPPY", compressions.get("name"));
    }
}

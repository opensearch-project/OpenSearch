/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.dataformat.FieldCodec;
import org.opensearch.index.engine.dataformat.FieldStorageParameters;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Translates the storage-neutral {@code codec} and {@code bloom_filter} field mapping parameters into the physical
 * Parquet column properties the native writer understands, and merges them with the deprecated per-field index
 * settings so both declaration styles keep working.
 *
 * <p>Precedence per field, highest first: explicit mapping parameter ({@code codec}, {@code bloom_filter}),
 * deprecated per-field index setting, the {@code cardinality} hint, then the per-type and index-wide defaults
 * resolved by the native writer.
 */
public final class ParquetFieldCodecs {

    private ParquetFieldCodecs() {}

    /**
     * Per-field physical column configuration handed to the native writer, keyed by field name. Values use the
     * writer's vocabulary (for example {@code DELTA_BINARY_PACKED}, {@code ZSTD(3)}).
     *
     * @param encodings         physical encoding per field
     * @param compressions      physical compression per field, optionally with an inline level such as {@code ZSTD(3)}
     * @param bloomFilterEnabled bloom filter toggle per field
     */
    public record FieldStorageConfig(Map<String, String> encodings, Map<String, String> compressions, Map<
        String,
        Boolean> bloomFilterEnabled) {
        public static final FieldStorageConfig EMPTY = new FieldStorageConfig(Map.of(), Map.of(), Map.of());

        public FieldStorageConfig {
            encodings = Collections.unmodifiableMap(new HashMap<>(encodings));
            compressions = Collections.unmodifiableMap(new HashMap<>(compressions));
            bloomFilterEnabled = Collections.unmodifiableMap(new HashMap<>(bloomFilterEnabled));
        }

        public boolean isEmpty() {
            return encodings.isEmpty() && compressions.isEmpty() && bloomFilterEnabled.isEmpty();
        }

        /** The deprecated per-field index settings only. */
        @SuppressWarnings("deprecation")
        public static FieldStorageConfig fromSettings(Settings settings) {
            return new FieldStorageConfig(
                ParquetSettings.getFieldEncodings(settings),
                ParquetSettings.getFieldCompressions(settings),
                ParquetSettings.getFieldBloomFilterEnabled(settings)
            );
        }
    }

    /**
     * Resolves the effective per-field configuration for an index: the deprecated per-field index settings, overlaid
     * with the {@code codec} and {@code bloom_filter} values declared in the mapping. A mapping value on a field
     * replaces the setting for that field; fields without a mapping value keep their setting.
     */
    public static FieldStorageConfig resolve(Settings settings, @Nullable MapperService mapperService) {
        FieldStorageConfig legacy = FieldStorageConfig.fromSettings(settings);
        if (mapperService == null) {
            return legacy;
        }
        Map<String, String> encodings = new HashMap<>(legacy.encodings());
        Map<String, String> compressions = new HashMap<>(legacy.compressions());
        Map<String, Boolean> bloom = new HashMap<>(legacy.bloomFilterEnabled());

        for (Map.Entry<String, FieldCodec> entry : mappingCodecs(mapperService).entrySet()) {
            String field = entry.getKey();
            FieldCodec codec = entry.getValue();
            MappedFieldType fieldType = mapperService.fieldType(field);
            ArrowType arrowType = fieldType == null ? null : arrowTypeFor(fieldType.typeName());
            String encoding = physicalEncoding(codec, arrowType);
            if (encoding != null) {
                encodings.put(field, encoding);
            }
            String compression = physicalCompression(codec);
            if (compression != null) {
                compressions.put(field, compression);
            }
        }
        for (String field : mappingBloomFilterFields(mapperService)) {
            bloom.put(field, Boolean.TRUE);
        }
        // Cardinality hints fill in only what nothing explicit (mapping codec/bloom_filter or a deprecated
        // per-field setting) has already decided for the field.
        for (Map.Entry<String, String> entry : mappingCardinalities(mapperService).entrySet()) {
            String field = entry.getKey();
            MappedFieldType fieldType = mapperService.fieldType(field);
            ArrowType arrowType = fieldType == null ? null : arrowTypeFor(fieldType.typeName());
            String encoding = encodingForCardinality(entry.getValue(), arrowType);
            if (encoding != null) {
                encodings.putIfAbsent(field, encoding);
            }
            if (FieldStorageParameters.CARDINALITY_LOW.equals(entry.getValue())) {
                bloom.putIfAbsent(field, Boolean.TRUE);
            }
        }
        return new FieldStorageConfig(encodings, compressions, bloom);
    }

    /** Fields carrying a {@code cardinality} hint, with its value ({@code low} or {@code high}). */
    public static Map<String, String> mappingCardinalities(MapperService mapperService) {
        return FieldStorageParameters.cardinalities(mapperService);
    }

    /**
     * The default Parquet encoding a cardinality hint selects for a column of the given Arrow type, or {@code null}
     * to leave the writer's default. {@code low} dictionary-encodes (run-length for booleans, which cannot be
     * dictionary-encoded); {@code high} skips dictionary attempts in favour of the type's best plain layout: delta for
     * integers and time types, byte-stream split for floats, plain for strings and binary.
     */
    @Nullable
    static String encodingForCardinality(String hint, @Nullable ArrowType arrowType) {
        if (FieldStorageParameters.CARDINALITY_LOW.equals(hint)) {
            return arrowType instanceof ArrowType.Bool ? "RLE" : "RLE_DICTIONARY";
        }
        if (FieldStorageParameters.CARDINALITY_HIGH.equals(hint)) {
            if (arrowType instanceof ArrowType.Int || arrowType instanceof ArrowType.Timestamp || arrowType instanceof ArrowType.Date) {
                return "DELTA_BINARY_PACKED";
            }
            if (arrowType instanceof ArrowType.FloatingPoint) {
                return "BYTE_STREAM_SPLIT";
            }
            if (arrowType instanceof ArrowType.Utf8
                || arrowType instanceof ArrowType.LargeUtf8
                || arrowType instanceof ArrowType.Binary
                || arrowType instanceof ArrowType.LargeBinary) {
                return "PLAIN";
            }
        }
        return null;
    }

    /** Fields carrying a {@code codec} mapping parameter, with the parsed codec. */
    public static Map<String, FieldCodec> mappingCodecs(MapperService mapperService) {
        return FieldStorageParameters.codecs(mapperService);
    }

    /** Fields whose {@code bloom_filter} mapping parameter is {@code true}. */
    public static Set<String> mappingBloomFilterFields(MapperService mapperService) {
        return FieldStorageParameters.bloomFilterFields(mapperService);
    }

    /**
     * The codec tokens the Parquet writer can honour: every well-known encoding and compression token. The
     * vocabulary is open, so a token another format defines is rejected here by name rather than silently ignored.
     */
    public static final Set<String> SUPPORTED_TOKENS;
    static {
        Set<String> tokens = new java.util.HashSet<>(FieldCodec.ENCODINGS);
        tokens.addAll(FieldCodec.COMPRESSIONS);
        SUPPORTED_TOKENS = Collections.unmodifiableSet(tokens);
    }

    /**
     * Validates a parsed codec against the field content type it is declared on. Runs at mapping parse time, so a
     * token the Parquet writer does not support, or an encoding it cannot apply to that type, is rejected before the
     * mapping is accepted.
     *
     * @throws IllegalArgumentException if a token is unsupported or the encoding does not fit the field type
     */
    public static void validateForContentType(FieldCodec codec, String contentType) {
        codec.requireSupported(SUPPORTED_TOKENS);
        ArrowType arrowType = arrowTypeFor(contentType);
        if (arrowType == null) {
            throw new IllegalArgumentException("codec is not supported on fields of type [" + contentType + "]");
        }
        if (codec.encoding() == null) {
            return;
        }
        String encoding = physicalEncoding(codec, arrowType);
        if (encoding == null || ParquetSettings.isEncodingValidForArrowType(encoding, arrowType) == false) {
            throw new IllegalArgumentException(
                "codec encoding [" + codec.encoding() + "] is not supported for fields of type [" + contentType + "]"
            );
        }
    }

    /**
     * The Parquet encoding for a neutral encoding token on a column of the given Arrow type, or {@code null} when the
     * codec does not constrain encoding. {@code delta} is type-directed: integers and time types use
     * {@code DELTA_BINARY_PACKED}, strings and binary use {@code DELTA_BYTE_ARRAY}.
     */
    @Nullable
    static String physicalEncoding(FieldCodec codec, @Nullable ArrowType arrowType) {
        String encoding = codec.encoding();
        if (encoding == null) {
            return null;
        }
        switch (encoding) {
            case FieldCodec.PLAIN:
                return "PLAIN";
            case FieldCodec.DICTIONARY:
                return "RLE_DICTIONARY";
            case FieldCodec.RLE:
                return "RLE";
            case FieldCodec.BYTE_SPLIT:
                return "BYTE_STREAM_SPLIT";
            case FieldCodec.DELTA:
                if (arrowType instanceof ArrowType.Utf8
                    || arrowType instanceof ArrowType.LargeUtf8
                    || arrowType instanceof ArrowType.Binary
                    || arrowType instanceof ArrowType.LargeBinary) {
                    return "DELTA_BYTE_ARRAY";
                }
                return "DELTA_BINARY_PACKED";
            default:
                throw new IllegalArgumentException("unknown codec encoding [" + encoding + "]");
        }
    }

    /**
     * The Parquet compression for a neutral compression token, with the level inlined as {@code NAME(level)} when one
     * was given, or {@code null} when the codec does not constrain compression.
     */
    @Nullable
    static String physicalCompression(FieldCodec codec) {
        String compression = codec.compression();
        if (compression == null) {
            return null;
        }
        String physical;
        switch (compression) {
            case FieldCodec.ZSTD:
                physical = "ZSTD";
                break;
            case FieldCodec.LZ4:
                physical = "LZ4_RAW";
                break;
            case FieldCodec.SNAPPY:
                physical = "SNAPPY";
                break;
            case FieldCodec.GZIP:
                physical = "GZIP";
                break;
            case FieldCodec.NONE:
                physical = "UNCOMPRESSED";
                break;
            default:
                throw new IllegalArgumentException("unknown codec compression [" + compression + "]");
        }
        return codec.compressionLevel() == null ? physical : physical + "(" + codec.compressionLevel() + ")";
    }

    /** The Arrow type the Parquet plugin uses for a core field content type, or {@code null} if unsupported. */
    @Nullable
    static ArrowType arrowTypeFor(String contentType) {
        ParquetField field = ArrowFieldRegistry.getParquetField(contentType);
        return field == null ? null : field.getFieldType().getType();
    }
}

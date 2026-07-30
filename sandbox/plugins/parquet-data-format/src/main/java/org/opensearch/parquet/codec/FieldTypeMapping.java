/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.DocValuesType;

import java.util.Locale;
import java.util.Map;

/**
 * Deterministic mapping from OpenSearch field mapping types to the Lucene DocValues type and
 * the Parquet physical type the codec reads
 *
 * <p>Every Parquet column is written as {@code optional repeated <physical>}; whether a field
 * is single- or multi-valued is decided by the OpenSearch mapping, not by this table. The DV
 * type recorded here is the <em>single-valued</em> form; the matching repeated form
 * ({@code SORTED_NUMERIC} for numerics, {@code SORTED_SET} for keyword/ip) is selected by the
 * producer when a multi-valued iterator is requested.
 */
public final class FieldTypeMapping {

    /**
     * The resolved DocValues + physical type for one OpenSearch mapping type.
     *
     * @param singleValued Lucene DV type served when the field holds one value per document
     *                     (e.g. {@code NUMERIC} for {@code long}, {@code SORTED} for {@code keyword})
     * @param multiValued  Lucene DV type served when the field is an array
     *                     ({@code SORTED_NUMERIC} for numerics, {@code SORTED_SET} for keyword/ip;
     *                     {@code NONE} for types with no array form, e.g. {@code text})
     * @param physical     the Parquet physical type the column is stored as, read back by the codec
     *
     * <p>{@code boolean} maps to a numeric DV type ({@code NUMERIC} / {@code SORTED_NUMERIC}) because
     * Lucene has no boolean DV type; it is stored as {@code 0}/{@code 1}, matching how core
     * OpenSearch indexes boolean doc values.
     */
    public record Mapping(DocValuesType singleValued, DocValuesType multiValued, ParquetPhysicalType physical) {
    }

    // TODO(type-coverage): these writable OpenSearch types are not yet mapped, so the codec serves no
    // doc values for them: half_float, unsigned_long, scaled_float, token_count, version, wildcard,
    // constant_keyword, flat_object, match_only_text. Note scaled_float is stored as a raw scaled
    // long, so it is not a plain INT64 mapping. forType and validate throw for unmapped types.
    private static final Map<String, Mapping> BY_TYPE = Map.ofEntries(
        Map.entry("boolean", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.BOOL)),
        Map.entry("byte", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT32)),
        Map.entry("short", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT32)),
        Map.entry("integer", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT32)),
        Map.entry("long", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT64)),
        Map.entry("float", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.FLOAT)),
        Map.entry("double", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.DOUBLE)),
        Map.entry("date", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT64)),
        Map.entry("date_nanos", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT64)),
        Map.entry("keyword", new Mapping(DocValuesType.SORTED, DocValuesType.SORTED_SET, ParquetPhysicalType.BYTE_ARRAY)),
        Map.entry("ip", new Mapping(DocValuesType.SORTED, DocValuesType.SORTED_SET, ParquetPhysicalType.BYTE_ARRAY)),
        Map.entry("text", new Mapping(DocValuesType.BINARY, DocValuesType.NONE, ParquetPhysicalType.BYTE_ARRAY)),
        Map.entry("binary", new Mapping(DocValuesType.BINARY, DocValuesType.NONE, ParquetPhysicalType.BYTE_ARRAY))
    );

    private FieldTypeMapping() {}

    /** True if the codec has a Parquet DocValues mapping for the given OpenSearch mapping type. */
    public static boolean isSupported(String mappingType) {
        return BY_TYPE.containsKey(mappingType);
    }

    /**
     * Returns the mapping for {@code mappingType}.
     *
     * @throws IllegalArgumentException if the mapping type has no Parquet DocValues mapping
     */
    public static Mapping forType(String mappingType) {
        Mapping m = BY_TYPE.get(mappingType);
        if (m == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parquet DocValues codec has no mapping for OpenSearch type '%s'", mappingType)
            );
        }
        return m;
    }

    /**
     * Validates that the field's mapping type supports the requested Lucene DV type, throwing
     * {@link IllegalArgumentException} naming the field and mapping type when incompatible.
     *
     * <p>The requested type may be the single- or multi-valued form of the mapping's DV type
     * (e.g. requesting {@code SORTED_NUMERIC} for a {@code long} field, whose single-valued
     * form is {@code NUMERIC}, is valid).
     */
    public static void validate(String field, String mappingType, DocValuesType requested) {
        Mapping m = BY_TYPE.get(mappingType);
        if (m == null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "field '%s' has mapping type '%s', which the Parquet DocValues codec does not support",
                    field,
                    mappingType
                )
            );
        }
        if (requested != m.singleValued() && requested != m.multiValued()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "field '%s' (mapping type '%s') supports DocValues type %s/%s but %s was requested",
                    field,
                    mappingType,
                    m.singleValued(),
                    m.multiValued(),
                    requested
                )
            );
        }
    }
}

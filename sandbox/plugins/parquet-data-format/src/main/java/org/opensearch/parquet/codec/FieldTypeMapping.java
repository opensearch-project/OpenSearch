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
 * Maps an OpenSearch field mapping type to the Lucene DocValues type the codec serves for it.
 *
 * <p>The recorded DV type is the <em>single-valued</em> form; the matching repeated form
 * ({@code SORTED_NUMERIC} for numerics) is selected when a multi-valued iterator is requested.
 * The Parquet physical type is not modeled here: the native cursor reads it from the Parquet
 * schema at open time.
 *
 * <p>Only single-valued numeric fields are mapped today. Boolean, half_float, scaled_float, and
 * the binary/keyword family are intentionally absent and fall through as unsupported until their
 * read paths land.
 */
public final class FieldTypeMapping {

    // TODO: add half_float and scaled_float once their Parquet encoding and longValue() decoding
    // are verified end-to-end, and boolean once the zero-copy bit-packed borrow path lands.

    /** The resolved single- and multi-valued Lucene DV types for a mapping type. */
    public record Mapping(DocValuesType singleValued, DocValuesType multiValued) {
    }

    private static final Map<String, Mapping> BY_TYPE = Map.ofEntries(
        Map.entry("byte", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        Map.entry("short", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        Map.entry("integer", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        Map.entry("long", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        Map.entry("float", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        Map.entry("double", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        Map.entry("date", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        Map.entry("date_nanos", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC))
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

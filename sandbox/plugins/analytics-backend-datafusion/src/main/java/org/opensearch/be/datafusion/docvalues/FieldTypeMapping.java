/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

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
 * <p>Single-valued numeric fields (including half_float, scaled_float, and unsigned_long) and
 * boolean are mapped today. The binary/keyword/text/ip family is intentionally absent and falls
 * through as unsupported until a variable-width read path lands.
 */
public final class FieldTypeMapping {

    // Not readable here, and why. token_count is deliberately absent: its field type reports
    // typeName() "integer" (TokenCountFieldType extends NumberFieldType), so it already resolves
    // through the "integer" entry below and a "token_count" key would be dead code.
    //
    // The binary/keyword/text/ip family has no borrow path in the native cursor yet, so those stay
    // out until a variable-width read lands.

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
        Map.entry("date_nanos", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        // BooleanFieldMapper stores doc values as a SortedNumericDocValuesField holding 0 or 1, so a
        // boolean resolves to the same DV types as the numerics; the codec reads it bit-packed.
        Map.entry("boolean", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        // Arrow UInt64. NumberType.UNSIGNED_LONG stores doc values as BigInteger.longValue(), the same
        // raw 64-bit pattern the column holds, so the bits pass through unchanged.
        Map.entry("unsigned_long", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        // Written as a plain long holding the already-scaled value, which is exactly what
        // ScaledFloatFieldMapper puts in doc values; ScaledFloatLeafFieldData divides by the scaling
        // factor above this codec, so the raw scaled long is the correct thing to return.
        Map.entry("scaled_float", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC)),
        // Arrow Float16, re-encoded to Lucene's sortable short by DecodedBatch.KIND_HALF_FLOAT.
        Map.entry("half_float", new Mapping(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC))
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

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
import java.util.Set;

/**
 * Maps an OpenSearch field mapping type to the Lucene DocValues type the codec serves for it.
 *
 * <p>Every supported type is served as {@code SORTED_NUMERIC}, the one DV type OpenSearch's numeric
 * and boolean mappers index; a single-valued field is a singleton recovered by callers via
 * {@code DocValues.unwrapSingleton}. The Parquet physical type is not modeled here: the native
 * cursor reads it from the Parquet schema at open time.
 *
 * <p>Numeric fields (including half_float, scaled_float, and unsigned_long) and boolean are mapped
 * today. The binary/keyword/text/ip family is not supported in this numeric borrow path.
 */
public final class FieldTypeMapping {

    // token_count is deliberately absent: its field type reports typeName() "integer"
    // (TokenCountFieldType extends NumberFieldType), so it already resolves through the
    // "integer" entry below and a "token_count" key would be dead code.

    private static final Set<String> SUPPORTED = Set.of(
        "byte",
        "short",
        "integer",
        "long",
        "float",
        "double",
        "date",
        "date_nanos",
        // BooleanFieldMapper stores doc values as a SortedNumericDocValuesField holding 0 or 1, so a
        // boolean resolves like the numerics; the codec reads it bit-packed.
        "boolean",
        // Arrow UInt64. NumberType.UNSIGNED_LONG stores doc values as BigInteger.longValue(), the same
        // raw 64-bit pattern the column holds, so the bits pass through unchanged.
        "unsigned_long",
        // Written as a plain long holding the already-scaled value, which is exactly what
        // ScaledFloatFieldMapper puts in doc values; ScaledFloatLeafFieldData divides by the scaling
        // factor above this codec, so the raw scaled long is the correct thing to return.
        "scaled_float",
        // Arrow Float16, re-encoded to Lucene's sortable short by DecodedBatch.KIND_HALF_FLOAT.
        "half_float"
    );

    private FieldTypeMapping() {}

    /** True if the codec has a Parquet DocValues mapping for the given OpenSearch mapping type. */
    public static boolean isSupported(String mappingType) {
        return SUPPORTED.contains(mappingType);
    }

    /**
     * Returns the DV type the codec serves for {@code mappingType}: always
     * {@link DocValuesType#SORTED_NUMERIC}.
     *
     * @throws IllegalArgumentException if the mapping type has no Parquet DocValues mapping
     */
    public static DocValuesType forType(String mappingType) {
        if (SUPPORTED.contains(mappingType) == false) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parquet DocValues codec has no mapping for OpenSearch type '%s'", mappingType)
            );
        }
        return DocValuesType.SORTED_NUMERIC;
    }

    /**
     * Validates that the field's mapping type supports the requested Lucene DV type, throwing
     * {@link IllegalArgumentException} naming the field and mapping type when incompatible.
     */
    public static void validate(String field, String mappingType, DocValuesType requested) {
        if (SUPPORTED.contains(mappingType) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "field '%s' has mapping type '%s', which the Parquet DocValues codec does not support",
                    field,
                    mappingType
                )
            );
        }
        if (requested != DocValuesType.SORTED_NUMERIC) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "field '%s' (mapping type '%s') supports DocValues type %s but %s was requested",
                    field,
                    mappingType,
                    DocValuesType.SORTED_NUMERIC,
                    requested
                )
            );
        }
    }
}

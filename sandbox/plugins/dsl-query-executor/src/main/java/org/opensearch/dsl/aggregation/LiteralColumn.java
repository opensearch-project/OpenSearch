/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

/**
 * One literal-derived input column of the aggregate, materialized by the converter in the
 * pre-aggregate project. Three kinds:
 *
 * <ul>
 *   <li><b>Double constant</b>: a column that always holds {@code value}.</li>
 *   <li><b>Integer constant</b>: same, typed as an exact integer.</li>
 *   <li><b>Coalesced</b>: {@code COALESCE(field, value)}, implementing {@code missing}.</li>
 * </ul>
 *
 * <p>Record equality drives allocator dedup: equal columns share one projected column.
 *
 * @param kind which of the three column kinds this is
 * @param coalesceFieldIndex input index of the coalesced field, or {@code null} for constants
 * @param value the constant value, or the {@code missing} substitute for a coalesced column
 */
public record LiteralColumn(Kind kind, Integer coalesceFieldIndex, double value) {

    /** The three column kinds. */
    public enum Kind {
        /** Constant typed as DOUBLE. */
        DOUBLE_CONSTANT,
        /** Constant typed as an exact integer. */
        INTEGER_CONSTANT,
        /** {@code COALESCE(field, value)}. */
        COALESCED
    }

    /**
     * Creates a DOUBLE constant column.
     *
     * @param value the constant value
     */
    public static LiteralColumn constant(double value) {
        return new LiteralColumn(Kind.DOUBLE_CONSTANT, null, value);
    }

    /**
     * Creates an exact-integer constant column.
     *
     * @param value the constant value
     */
    public static LiteralColumn integerConstant(long value) {
        return new LiteralColumn(Kind.INTEGER_CONSTANT, null, value);
    }

    /**
     * Creates a coalesced column: {@code COALESCE(field, missingValue)}.
     *
     * @param fieldIndex input index of the field
     * @param missingValue substitute for SQL NULL
     */
    public static LiteralColumn coalesced(int fieldIndex, double missingValue) {
        return new LiteralColumn(Kind.COALESCED, fieldIndex, missingValue);
    }
}

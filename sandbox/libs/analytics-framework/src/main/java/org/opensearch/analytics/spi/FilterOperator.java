/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * All filter operations a backend may support, covering standard comparisons,
 * full-text search, and expression-based filtering.
 *
 * <p>Each operator carries a {@link Type} indicating its category and whether
 * it supports parameters (e.g., full-text operators accept analyzer, slop, etc.).
 *
 * @opensearch.internal
 */
public enum FilterOperator {

    // Standard comparison
    EQUALS(Type.STANDARD, SqlKind.EQUALS),
    NOT_EQUALS(Type.STANDARD, SqlKind.NOT_EQUALS),
    GREATER_THAN(Type.STANDARD, SqlKind.GREATER_THAN),
    GREATER_THAN_OR_EQUAL(Type.STANDARD, SqlKind.GREATER_THAN_OR_EQUAL),
    LESS_THAN(Type.STANDARD, SqlKind.LESS_THAN),
    LESS_THAN_OR_EQUAL(Type.STANDARD, SqlKind.LESS_THAN_OR_EQUAL),
    IS_NULL(Type.STANDARD, SqlKind.IS_NULL),
    IS_NOT_NULL(Type.STANDARD, SqlKind.IS_NOT_NULL),
    IN(Type.STANDARD, SqlKind.IN),
    LIKE(Type.STANDARD, SqlKind.LIKE),
    PREFIX(Type.STANDARD, SqlKind.OTHER),

    // Full-text search
    MATCH(Type.FULL_TEXT, SqlKind.OTHER),
    MATCH_PHRASE(Type.FULL_TEXT, SqlKind.OTHER),
    FUZZY(Type.FULL_TEXT, SqlKind.OTHER),
    WILDCARD(Type.FULL_TEXT, SqlKind.OTHER),
    REGEXP(Type.FULL_TEXT, SqlKind.OTHER),

    // Expression-based filtering (on derived columns, e.g., HAVING)
    EXPRESSION(Type.EXPRESSION, SqlKind.OTHER);

    /**
     * Category of filter operator. Declares whether the operator supports parameters.
     */
    public enum Type {
        STANDARD(false),
        FULL_TEXT(true),
        EXPRESSION(false);

        private final boolean supportsParams;

        Type(boolean supportsParams) {
            this.supportsParams = supportsParams;
        }

        public boolean supportsParams() {
            return supportsParams;
        }
    }

    private final Type type;
    private final SqlKind sqlKind;

    FilterOperator(Type type, SqlKind sqlKind) {
        this.type = type;
        this.sqlKind = sqlKind;
    }

    public Type getType() {
        return type;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /**
     * Returns a Calcite {@link SqlFunction} for this full-text operator.
     * Only valid for operators of type {@link Type#FULL_TEXT}.
     */
    public SqlFunction toSqlFunction() {
        return new SqlFunction(name(), SqlKind.OTHER_FUNCTION, ReturnTypes.BOOLEAN,
            null, OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    /** Maps a Calcite SqlKind to a standard FilterOperator, or null if not recognized. */
    public static FilterOperator fromSqlKind(SqlKind kind) {
        for (FilterOperator op : values()) {
            if (op.type == Type.STANDARD && op.sqlKind == kind && op.sqlKind != SqlKind.OTHER) {
                return op;
            }
        }
        return null;
    }

    /** Maps a Calcite SqlFunction to a FULL_TEXT FilterOperator by name, or null if not recognized. */
    public static FilterOperator fromSqlFunction(SqlFunction function) {
        try {
            FilterOperator op = FilterOperator.valueOf(function.getName().toUpperCase(java.util.Locale.ROOT));
            return op.type == Type.FULL_TEXT ? op : null;
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }
}

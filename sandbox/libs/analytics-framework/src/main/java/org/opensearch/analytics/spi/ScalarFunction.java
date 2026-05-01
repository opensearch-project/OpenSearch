/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.Locale;

/**
 * All scalar functions a backend may support — comparisons, full-text search,
 * math, string, conditional, date/time, and cast operations. Used across filter,
 * project, and aggregate expression capability declarations.
 *
 * <p>Each function carries a {@link Category} indicating its type and whether
 * it supports parameters (e.g., full-text operators accept analyzer, slop, etc.).
 *
 * @opensearch.internal
 */
public enum ScalarFunction {

    // ── Comparisons ──────────────────────────────────────────────────
    EQUALS(Category.COMPARISON, SqlKind.EQUALS),
    NOT_EQUALS(Category.COMPARISON, SqlKind.NOT_EQUALS),
    GREATER_THAN(Category.COMPARISON, SqlKind.GREATER_THAN),
    GREATER_THAN_OR_EQUAL(Category.COMPARISON, SqlKind.GREATER_THAN_OR_EQUAL),
    LESS_THAN(Category.COMPARISON, SqlKind.LESS_THAN),
    LESS_THAN_OR_EQUAL(Category.COMPARISON, SqlKind.LESS_THAN_OR_EQUAL),
    IS_NULL(Category.COMPARISON, SqlKind.IS_NULL),
    IS_NOT_NULL(Category.COMPARISON, SqlKind.IS_NOT_NULL),
    IN(Category.COMPARISON, SqlKind.IN),
    LIKE(Category.COMPARISON, SqlKind.LIKE),
    PREFIX(Category.COMPARISON, SqlKind.OTHER_FUNCTION),

    // ── Full-text search ─────────────────────────────────────────────
    MATCH(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    MATCH_PHRASE(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    FUZZY(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    WILDCARD(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    REGEXP(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),

    // ── String ───────────────────────────────────────────────────────
    UPPER(Category.STRING, SqlKind.OTHER_FUNCTION),
    LOWER(Category.STRING, SqlKind.OTHER_FUNCTION),
    TRIM(Category.STRING, SqlKind.TRIM),
    SUBSTRING(Category.STRING, SqlKind.OTHER_FUNCTION),
    CONCAT(Category.STRING, SqlKind.OTHER_FUNCTION),
    CHAR_LENGTH(Category.STRING, SqlKind.OTHER_FUNCTION),

    // ── Math ─────────────────────────────────────────────────────────
    PLUS(Category.MATH, SqlKind.PLUS),
    MINUS(Category.MATH, SqlKind.MINUS),
    TIMES(Category.MATH, SqlKind.TIMES),
    DIVIDE(Category.MATH, SqlKind.DIVIDE),
    MOD(Category.MATH, SqlKind.MOD),
    ABS(Category.MATH, SqlKind.OTHER_FUNCTION),
    SIN(Category.MATH, SqlKind.OTHER_FUNCTION),
    CEIL(Category.MATH, SqlKind.CEIL),
    FLOOR(Category.MATH, SqlKind.FLOOR),

    // ── Cast / type ──────────────────────────────────────────────────
    CAST(Category.SCALAR, SqlKind.CAST),

    // ── Conditional ──────────────────────────────────────────────────
    CASE(Category.SCALAR, SqlKind.CASE),
    COALESCE(Category.SCALAR, SqlKind.COALESCE),
    NULLIF(Category.SCALAR, SqlKind.NULLIF),

    EXTRACT(Category.SCALAR, SqlKind.EXTRACT),

    // ── Datetime ────────────────────────────────────────────────────
    TIMESTAMP(Category.SCALAR, SqlKind.OTHER_FUNCTION);

    /**
     * Category of scalar function.
     */
    public enum Category {
        COMPARISON,
        FULL_TEXT,
        STRING,
        MATH,
        /** Catch-all for functions that don't fit other categories (CAST, CASE, COALESCE, EXTRACT, etc.). */
        SCALAR
    }

    private final Category category;
    private final SqlKind sqlKind;

    ScalarFunction(Category category, SqlKind sqlKind) {
        this.category = category;
        this.sqlKind = sqlKind;
    }

    public Category getCategory() {
        return category;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /**
     * Maps a Calcite SqlKind to a ScalarFunction, or null if not recognized.
     * Skips OTHER_FUNCTION — multiple functions share this kind,
     * so they must be resolved by name via {@link #fromSqlFunction(SqlFunction)}.
     */
    public static ScalarFunction fromSqlKind(SqlKind kind) {
        for (ScalarFunction func : values()) {
            if (func.sqlKind == kind && func.sqlKind != SqlKind.OTHER_FUNCTION) {
                return func;
            }
        }
        return null;
    }

    /** Maps a Calcite SqlFunction to a ScalarFunction by name, or throws if not recognized. */
    public static ScalarFunction fromSqlFunction(SqlFunction function) {
        // TODO: Add an explicit functionName field per enum constant instead of relying on
        // valueOf(toUpperCase). This couples enum constant naming to SQL function naming convention.
        return ScalarFunction.valueOf(function.getName().toUpperCase(Locale.ROOT));
    }
}

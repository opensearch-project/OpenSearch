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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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
    /** Calcite's Sarg fold for IN / NOT IN / BETWEEN / range-union. Backends expand it before substrait. */
    SARG_PREDICATE(Category.SCALAR, SqlKind.SEARCH),

    // ── Full-text search ─────────────────────────────────────────────
    MATCH(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    MATCH_PHRASE(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    FUZZY(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    WILDCARD(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    REGEXP(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    REGEXP_CONTAINS(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),

    // ── String ───────────────────────────────────────────────────────
    UPPER(Category.STRING, SqlKind.OTHER_FUNCTION),
    LOWER(Category.STRING, SqlKind.OTHER_FUNCTION),
    TRIM(Category.STRING, SqlKind.TRIM),
    SUBSTRING(Category.STRING, SqlKind.OTHER_FUNCTION),
    /**
     * String concatenation. Calcite's {@code SqlStdOperatorTable.CONCAT} is a
     * {@link org.apache.calcite.sql.SqlBinaryOperator} named {@code "||"} (not {@code "CONCAT"})
     * with {@link SqlKind#OTHER}, so neither {@link #fromSqlKind(SqlKind)} nor identifier-name
     * {@link #valueOf(String)} resolves it. The {@code referenceOperator} hook below pins the
     * concrete Calcite operator constant so resolution is a singleton-identity match — a Calcite
     * rename surfaces as a compile error rather than as a silent string mismatch at runtime.
     */
    CONCAT(Category.STRING, SqlKind.OTHER_FUNCTION, SqlStdOperatorTable.CONCAT),
    CHAR_LENGTH(Category.STRING, SqlKind.OTHER_FUNCTION),

    // ── Math ─────────────────────────────────────────────────────────
    PLUS(Category.MATH, SqlKind.PLUS),
    MINUS(Category.MATH, SqlKind.MINUS),
    TIMES(Category.MATH, SqlKind.TIMES),
    DIVIDE(Category.MATH, SqlKind.DIVIDE),
    MOD(Category.MATH, SqlKind.MOD),
    ABS(Category.MATH, SqlKind.OTHER_FUNCTION),
    ACOS(Category.MATH, SqlKind.OTHER_FUNCTION),
    ASIN(Category.MATH, SqlKind.OTHER_FUNCTION),
    ATAN(Category.MATH, SqlKind.OTHER_FUNCTION),
    ATAN2(Category.MATH, SqlKind.OTHER_FUNCTION),
    CBRT(Category.MATH, SqlKind.OTHER_FUNCTION),
    CEIL(Category.MATH, SqlKind.CEIL),
    COS(Category.MATH, SqlKind.OTHER_FUNCTION),
    COSH(Category.MATH, SqlKind.OTHER_FUNCTION),
    COT(Category.MATH, SqlKind.OTHER_FUNCTION),
    DEGREES(Category.MATH, SqlKind.OTHER_FUNCTION),
    E(Category.MATH, SqlKind.OTHER_FUNCTION),
    EXP(Category.MATH, SqlKind.OTHER_FUNCTION),
    EXPM1(Category.MATH, SqlKind.OTHER_FUNCTION),
    FLOOR(Category.MATH, SqlKind.FLOOR),
    LN(Category.MATH, SqlKind.OTHER_FUNCTION),
    LOG(Category.MATH, SqlKind.OTHER_FUNCTION),
    LOG10(Category.MATH, SqlKind.OTHER_FUNCTION),
    LOG2(Category.MATH, SqlKind.OTHER_FUNCTION),
    PI(Category.MATH, SqlKind.OTHER_FUNCTION),
    POWER(Category.MATH, SqlKind.OTHER_FUNCTION),
    RADIANS(Category.MATH, SqlKind.OTHER_FUNCTION),
    RAND(Category.MATH, SqlKind.OTHER_FUNCTION),
    ROUND(Category.MATH, SqlKind.OTHER_FUNCTION),
    SCALAR_MAX(Category.MATH, SqlKind.OTHER_FUNCTION),
    SCALAR_MIN(Category.MATH, SqlKind.OTHER_FUNCTION),
    SIGN(Category.MATH, SqlKind.OTHER_FUNCTION),
    SIN(Category.MATH, SqlKind.OTHER_FUNCTION),
    SINH(Category.MATH, SqlKind.OTHER_FUNCTION),
    TAN(Category.MATH, SqlKind.OTHER_FUNCTION),
    TRUNCATE(Category.MATH, SqlKind.OTHER_FUNCTION),

    // ── Cast / type ──────────────────────────────────────────────────
    CAST(Category.SCALAR, SqlKind.CAST),
    /**
     * Calcite's {@code SAFE_CAST} — emitted by PPL's explicit {@code CAST(... AS ...)} when the
     * source value may be NULL or the conversion may fail; returns NULL on failure rather than
     * throwing. Resolves through {@link SqlKind#SAFE_CAST}, distinct from {@link #CAST} which
     * uses {@link SqlKind#CAST}. DataFusion's native cast already returns NULL on conversion
     * failure, so SAFE_CAST and CAST share the same backend semantics.
     */
    SAFE_CAST(Category.SCALAR, SqlKind.SAFE_CAST),

    // ── Conditional ──────────────────────────────────────────────────
    CASE(Category.SCALAR, SqlKind.CASE),
    COALESCE(Category.SCALAR, SqlKind.COALESCE),
    NULLIF(Category.SCALAR, SqlKind.NULLIF),

    EXTRACT(Category.SCALAR, SqlKind.EXTRACT),

    // ── Datetime ────────────────────────────────────────────────────
    TIMESTAMP(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    YEAR(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    CONVERT_TZ(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    UNIX_TIMESTAMP(Category.SCALAR, SqlKind.OTHER_FUNCTION);

    /**
     * Category of scalar function.
     */
    public enum Category {
        COMPARISON,
        FULL_TEXT,
        STRING,
        MATH,
        /**
         * Catch-all for functions that don't fit other categories (CAST, CASE, COALESCE, EXTRACT, etc.).
         */
        SCALAR
    }

    private final Category category;
    private final SqlKind sqlKind;
    /**
     * Optional Calcite operator that this constant maps to when the operator cannot be resolved
     * via {@link SqlKind} or via identifier-name {@link #valueOf(String)} — typically operators
     * whose {@code getName()} returns a non-identifier token (e.g. {@code SqlStdOperatorTable.CONCAT}
     * is named {@code "||"}). Null for the common case where SqlKind or name resolution suffices.
     * Stored as a reference (not a string) so a Calcite-side rename of the operator surfaces as a
     * compile error here.
     */
    private final SqlOperator referenceOperator;

    ScalarFunction(Category category, SqlKind sqlKind) {
        this(category, sqlKind, null);
    }

    ScalarFunction(Category category, SqlKind sqlKind, SqlOperator referenceOperator) {
        this.category = category;
        this.sqlKind = sqlKind;
        this.referenceOperator = referenceOperator;
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

    /**
     * Maps a Calcite SqlFunction to a ScalarFunction by name, or null if not recognized.
     */
    public static ScalarFunction fromSqlFunction(SqlFunction function) {
        // TODO: Add an explicit functionName field per enum constant instead of relying on
        // valueOf(toUpperCase). This couples enum constant naming to SQL function naming convention.
        return ScalarFunction.valueOf(function.getName().toUpperCase(Locale.ROOT));
    }

    /**
     * Reverse index from {@link #referenceOperator} to enum constant. Built from the enum itself
     * at class init — adding a new symbolic operator is a single-site change on the enum constant,
     * no separate map to maintain. Lookup is identity-keyed because Calcite's standard operators
     * are singletons (e.g. {@code SqlStdOperatorTable.CONCAT}). Empty in the common case (most
     * constants resolve by SqlKind or identifier-name valueOf).
     */
    private static final Map<SqlOperator, ScalarFunction> BY_REFERENCE_OPERATOR;

    static {
        Map<SqlOperator, ScalarFunction> byOperator = new HashMap<>();
        for (ScalarFunction func : values()) {
            if (func.referenceOperator != null) {
                byOperator.put(func.referenceOperator, func);
            }
        }
        // The HashMap is private static final and never exposed beyond the get() in the resolver
        // below — wrapping it in Map.copyOf adds an allocation without any external safety guarantee.
        BY_REFERENCE_OPERATOR = byOperator;
    }

    /**
     * Maps any Calcite {@link SqlOperator} to a {@link ScalarFunction}, or returns null if
     * unrecognized. Resolution order: {@link SqlKind} match, then {@link #referenceOperator}
     * identity match (handles {@code SqlStdOperatorTable.CONCAT} a.k.a. {@code ||}), then
     * identifier-name {@link #valueOf(String)} match.
     *
     * <p>Prefer this entry point over {@link #fromSqlKind(SqlKind)} /
     * {@link #fromSqlFunction(SqlFunction)} when resolving an arbitrary {@code RexCall}'s
     * operator: a {@code RexCall} may be backed by a {@code SqlBinaryOperator} (e.g. {@code ||})
     * which is neither covered by {@code OTHER} {@code SqlKind} nor by {@code SqlFunction}.
     */
    public static ScalarFunction fromSqlOperatorWithFallback(SqlOperator operator) {
        ScalarFunction byKind = fromSqlKind(operator.getKind());
        if (byKind != null) {
            return byKind;
        }
        ScalarFunction byReference = BY_REFERENCE_OPERATOR.get(operator);
        if (byReference != null) {
            return byReference;
        }
        try {
            return ScalarFunction.valueOf(operator.getName().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }
}

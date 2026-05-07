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

    // ── Logical connectives ─────────────────────────────────────────
    AND(Category.SCALAR, SqlKind.AND),
    OR(Category.SCALAR, SqlKind.OR),
    NOT(Category.SCALAR, SqlKind.NOT),
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
    SUBSTR(Category.STRING, SqlKind.OTHER_FUNCTION),
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
    CONCAT_WS(Category.STRING, SqlKind.OTHER_FUNCTION),
    CHAR_LENGTH(Category.STRING, SqlKind.OTHER_FUNCTION),
    REPLACE(Category.STRING, SqlKind.OTHER_FUNCTION),
    REGEXP_REPLACE(Category.STRING, SqlKind.OTHER_FUNCTION),
    ASCII(Category.STRING, SqlKind.OTHER_FUNCTION),
    LEFT(Category.STRING, SqlKind.OTHER_FUNCTION),
    LENGTH(Category.STRING, SqlKind.OTHER_FUNCTION),
    LOCATE(Category.STRING, SqlKind.OTHER_FUNCTION),
    POSITION(Category.STRING, SqlKind.POSITION),
    LTRIM(Category.STRING, SqlKind.OTHER_FUNCTION),
    RTRIM(Category.STRING, SqlKind.OTHER_FUNCTION),
    REVERSE(Category.STRING, SqlKind.OTHER_FUNCTION),
    RIGHT(Category.STRING, SqlKind.OTHER_FUNCTION),
    TOSTRING(Category.STRING, SqlKind.OTHER_FUNCTION),
    NUMBER_TO_STRING(Category.STRING, SqlKind.OTHER_FUNCTION), // Alias for TOSTRING
    TONUMBER(Category.STRING, SqlKind.OTHER_FUNCTION),
    STRCMP(Category.STRING, SqlKind.OTHER_FUNCTION),
    TRANSLATE(Category.STRING, SqlKind.OTHER_FUNCTION),
    REX_EXTRACT(Category.STRING, SqlKind.OTHER_FUNCTION),
    REX_EXTRACT_MULTI(Category.STRING, SqlKind.OTHER_FUNCTION),
    REX_OFFSET(Category.STRING, SqlKind.OTHER_FUNCTION),

    // ── Cryptographic hash ─────────────────────────────────────────────
    // md5(x), sha1(x), sha2(x, bitLen) with bitLen ∈ {224,256,384,512}, crc32(x).
    MD5(Category.STRING, SqlKind.OTHER_FUNCTION),
    SHA1(Category.STRING, SqlKind.OTHER_FUNCTION),
    SHA2(Category.STRING, SqlKind.OTHER_FUNCTION),
    CRC32(Category.STRING, SqlKind.OTHER_FUNCTION),

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
    // fromSqlFunction resolves via valueOf(name.toUpperCase()), so the enum name IS
    // the wire contract. Aliases each need their own entry; the adapter map points
    // them at one shared instance.
    TIMESTAMP(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    YEAR(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    QUARTER(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    MONTH(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    MONTH_OF_YEAR(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    DAY(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    DAYOFMONTH(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    DAYOFYEAR(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    DAY_OF_YEAR(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    HOUR(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    HOUR_OF_DAY(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    MINUTE(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    MINUTE_OF_HOUR(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    MICROSECOND(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    WEEK(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    WEEK_OF_YEAR(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    NOW(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    CURRENT_TIMESTAMP(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    CURRENT_DATE(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    CURDATE(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    CURRENT_TIME(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    CURTIME(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    CONVERT_TZ(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    UNIX_TIMESTAMP(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    STRFTIME(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    TIME(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    DATE(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    DATETIME(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    SYSDATE(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    DAYOFWEEK(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    DAY_OF_WEEK(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    SECOND(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    SECOND_OF_MINUTE(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    FROM_UNIXTIME(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    MAKETIME(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    MAKEDATE(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    DATE_FORMAT(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    TIME_FORMAT(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    STR_TO_DATE(Category.SCALAR, SqlKind.OTHER_FUNCTION),

    // ── JSON ────────────────────────────────────────────────────────
    JSON_APPEND(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    JSON_ARRAY_LENGTH(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    JSON_DELETE(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    JSON_EXTEND(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    JSON_EXTRACT(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    JSON_KEYS(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    JSON_SET(Category.SCALAR, SqlKind.OTHER_FUNCTION),

    // ── Array ────────────────────────────────────────────────────────
    /**
     * PPL {@code array(a, b, …)} constructor — resolves through the SQL plugin's
     * {@code ArrayFunctionImpl} UDF named {@code "array"}. DataFusion's native
     * equivalent is {@code make_array}, so a backend that supports this needs a
     * name-mapping adapter (see {@code MakeArrayAdapter} in the DataFusion backend).
     */
    ARRAY(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    ARRAY_LENGTH(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    ARRAY_SLICE(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    ARRAY_DISTINCT(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    /**
     * Calcite's {@code ARRAY_JOIN} — joins array elements with a separator. PPL
     * {@code mvjoin} is registered to this operator. DataFusion's native equivalent
     * is named {@code array_to_string}, so the DataFusion backend rewrites to that
     * via a name-mapping adapter.
     */
    ARRAY_JOIN(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    /**
     * Calcite's {@code SqlStdOperatorTable.ITEM} — element access ({@code arr[N]}).
     * PPL's {@code mvindex(arr, N)} single-element form lowers through
     * {@code MVIndexFunctionImp.resolveSingleElement} to ITEM with a 1-based index
     * (already converted from PPL's 0-based input). DataFusion's native equivalent
     * is {@code array_element}, also 1-based; the DataFusion backend renames via a
     * name-mapping adapter.
     */
    ITEM(Category.SCALAR, SqlKind.ITEM),
    /**
     * PPL {@code mvzip(left, right [, sep])} — element-wise zip of two arrays into an
     * array of strings, joined per pair by a separator (default {@code ","}). Resolves
     * through the SQL plugin's {@code MVZipFunctionImpl} UDF named {@code "mvzip"}.
     * No DataFusion stdlib equivalent — the analytics-backend-datafusion plugin ships
     * a custom Rust UDF (`udf::mvzip`) registered on its session context.
     */
    MVZIP(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    /**
     * PPL {@code mvfind(arr, regex)} — find the 0-based index of the first array
     * element matching a regex, or NULL if no match. Resolves through the SQL
     * plugin's {@code MVFindFunctionImpl} UDF named {@code "mvfind"}. No
     * DataFusion stdlib equivalent — the analytics-backend-datafusion plugin
     * ships a custom Rust UDF (`udf::mvfind`) registered on its session context.
     */
    MVFIND(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    /**
     * PPL {@code mvappend(arg1, arg2, …)} — flatten a mixed list of array and
     * scalar arguments into one array, dropping null args and null elements.
     * Resolves through the SQL plugin's {@code MVAppendFunctionImpl} UDF named
     * {@code "mvappend"}. DataFusion's {@code array_concat} only accepts arrays
     * and preserves nulls, so the analytics-backend-datafusion plugin ships a
     * custom Rust UDF ({@code udf::mvappend}) registered on its session context.
     */
    MVAPPEND(Category.SCALAR, SqlKind.OTHER_FUNCTION);

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

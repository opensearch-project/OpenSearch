/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Unit coverage for {@link ScalarFunction}'s three resolution paths used by the analytics-engine
 * planner ({@code OpenSearchProjectRule}, {@code OpenSearchFilterRule}, {@code BackendPlanAdapter}).
 *
 * <p>Each test pins one of the resolver's branches so a regression that drops a branch surfaces
 * here rather than in IT-level "No backend supports scalar function [null]" errors.
 */
public class ScalarFunctionTests extends OpenSearchTestCase {

    // ── fromSqlKind ─────────────────────────────────────────────────────────────

    public void testFromSqlKindResolvesDedicatedKind() {
        assertEquals(ScalarFunction.EQUALS, ScalarFunction.fromSqlKind(SqlKind.EQUALS));
        assertEquals(ScalarFunction.PLUS, ScalarFunction.fromSqlKind(SqlKind.PLUS));
        assertEquals(ScalarFunction.CAST, ScalarFunction.fromSqlKind(SqlKind.CAST));
        assertEquals(ScalarFunction.SAFE_CAST, ScalarFunction.fromSqlKind(SqlKind.SAFE_CAST));
        assertEquals(ScalarFunction.COALESCE, ScalarFunction.fromSqlKind(SqlKind.COALESCE));
    }

    public void testFromSqlKindReturnsNullForOtherKind() {
        // SqlKind.OTHER is shared by many SqlBinaryOperators — must NOT resolve via SqlKind.
        assertNull(ScalarFunction.fromSqlKind(SqlKind.OTHER));
    }

    public void testFromSqlKindReturnsNullForOtherFunctionKind() {
        // SqlKind.OTHER_FUNCTION is shared by many name-distinguished SqlFunctions — must NOT
        // resolve via SqlKind even though several enum entries declare it.
        assertNull(ScalarFunction.fromSqlKind(SqlKind.OTHER_FUNCTION));
    }

    /** Non-OTHER_FUNCTION SqlKinds must be unique: fromSqlKind picks the first match and would shadow later entries. */
    public void testNoDuplicateSqlKindBindings() {
        Map<SqlKind, ScalarFunction> claimedBy = new EnumMap<>(SqlKind.class);
        for (ScalarFunction func : ScalarFunction.values()) {
            SqlKind kind = func.getSqlKind();
            if (kind == SqlKind.OTHER_FUNCTION) {
                continue;
            }
            ScalarFunction existing = claimedBy.put(kind, func);
            if (existing != null) {
                fail("SqlKind." + kind + " claimed by both " + existing + " and " + func);
            }
        }
    }

    public void testSargPredicateIsBoundToSqlKindSearch() {
        assertSame(ScalarFunction.SARG_PREDICATE, ScalarFunction.fromSqlKind(SqlKind.SEARCH));
    }

    // ── fromSqlOperatorWithFallback: SqlKind branch ────────────────────────────────────────

    public void testFromSqlOperatorResolvesViaSqlKind() {
        // Calcite's CAST has a dedicated SqlKind.CAST — short-circuit before name lookup.
        assertEquals(ScalarFunction.CAST, ScalarFunction.fromSqlOperatorWithFallback(SqlStdOperatorTable.CAST));
        assertEquals(ScalarFunction.PLUS, ScalarFunction.fromSqlOperatorWithFallback(SqlStdOperatorTable.PLUS));
        assertEquals(ScalarFunction.GREATER_THAN, ScalarFunction.fromSqlOperatorWithFallback(SqlStdOperatorTable.GREATER_THAN));
        assertEquals(ScalarFunction.COALESCE, ScalarFunction.fromSqlOperatorWithFallback(SqlStdOperatorTable.COALESCE));
    }

    // ── fromSqlOperatorWithFallback: reference-operator branch ─────────────────────────────

    public void testFromSqlOperatorResolvesPipeConcatViaReferenceOperator() {
        // The original "no backend supports scalar function [null]" symptom for PPL string `+`.
        // SqlStdOperatorTable.CONCAT is a SqlBinaryOperator named "||" with SqlKind.OTHER —
        // neither fromSqlKind nor fromSqlFunction(SqlFunction) resolves it. CONCAT's
        // referenceOperator field points at the singleton, so the resolver matches by identity.
        assertEquals("||", SqlStdOperatorTable.CONCAT.getName());
        assertEquals(SqlKind.OTHER, SqlStdOperatorTable.CONCAT.getKind());
        assertEquals(ScalarFunction.CONCAT, ScalarFunction.fromSqlOperatorWithFallback(SqlStdOperatorTable.CONCAT));
    }

    // ── fromSqlOperatorWithFallback: identifier-name branch ────────────────────────────────

    public void testFromSqlOperatorResolvesViaIdentifierName() {
        // SqlStdOperatorTable.UPPER is a SqlFunction named "UPPER" with SqlKind.OTHER_FUNCTION;
        // resolves through the valueOf(name.toUpperCase()) fallback after SqlKind misses.
        assertEquals(ScalarFunction.UPPER, ScalarFunction.fromSqlOperatorWithFallback(SqlStdOperatorTable.UPPER));
        assertEquals(ScalarFunction.LOWER, ScalarFunction.fromSqlOperatorWithFallback(SqlStdOperatorTable.LOWER));
        assertEquals(ScalarFunction.ABS, ScalarFunction.fromSqlOperatorWithFallback(SqlStdOperatorTable.ABS));
    }

    public void testFromSqlOperatorReturnsNullForUnknownFunction() {
        // UNARY_MINUS has SqlKind.MINUS_PREFIX (no enum) and name "-" (not a valid valueOf input);
        // both resolution paths miss and the resolver returns null instead of throwing.
        assertNull(ScalarFunction.fromSqlOperatorWithFallback(SqlStdOperatorTable.UNARY_MINUS));
    }

    // ── Group G math functions: name-based lookup via fromSqlFunction ──────────
    // PPL emits these as Calcite SqlBasicFunction calls whose name matches the
    // enum constant. STANDARD_PROJECT_OPS registration (and adapter dispatch)
    // depends on fromSqlFunction resolving them by name, so guard every entry.

    public void testMathFunctionsResolveByName() {
        assertSame(ScalarFunction.ABS, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.ABS));
        assertSame(ScalarFunction.ACOS, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.ACOS));
        assertSame(ScalarFunction.ASIN, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.ASIN));
        assertSame(ScalarFunction.ATAN, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.ATAN));
        assertSame(ScalarFunction.ATAN2, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.ATAN2));
        assertSame(ScalarFunction.CBRT, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.CBRT));
        assertSame(ScalarFunction.COS, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.COS));
        assertSame(ScalarFunction.COT, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.COT));
        assertSame(ScalarFunction.DEGREES, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.DEGREES));
        assertSame(ScalarFunction.EXP, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.EXP));
        assertSame(ScalarFunction.LN, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.LN));
        // 2-arg log: PPL emits SqlLibraryOperators.LOG(x, base); 1-arg log(x) is pre-lowered to
        // LOG(x, e) by PPLFuncImpTable, so this single LOG entry covers both arities.
        assertSame(ScalarFunction.LOG, ScalarFunction.fromSqlFunction(SqlLibraryOperators.LOG));
        assertSame(ScalarFunction.LOG10, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.LOG10));
        assertSame(ScalarFunction.LOG2, ScalarFunction.fromSqlFunction(SqlLibraryOperators.LOG2));
        assertSame(ScalarFunction.PI, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.PI));
        assertSame(ScalarFunction.POWER, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.POWER));
        assertSame(ScalarFunction.RADIANS, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.RADIANS));
        assertSame(ScalarFunction.RAND, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.RAND));
        assertSame(ScalarFunction.ROUND, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.ROUND));
        assertSame(ScalarFunction.SIGN, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.SIGN));
        assertSame(ScalarFunction.TAN, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.TAN));
        assertSame(ScalarFunction.TRUNCATE, ScalarFunction.fromSqlFunction(SqlStdOperatorTable.TRUNCATE));
    }

    /** PPL's SCALAR_MAX / SCALAR_MIN UDFs resolve by the UDF's declared name — these are the
     *  PPLBuiltinOperators variants that DataFusionAnalyticsBackendPlugin binds to
     *  AbstractNameMappingAdapter instances targeting SqlLibraryOperators.GREATEST / LEAST. */
    public void testScalarMaxMinResolveByName() {
        assertSame(ScalarFunction.SCALAR_MAX, ScalarFunction.valueOf("SCALAR_MAX"));
        assertSame(ScalarFunction.SCALAR_MIN, ScalarFunction.valueOf("SCALAR_MIN"));
    }

    /**
     * Tier-2 adapter targets: enum entries exist for PPL UDFs even though the
     * upstream isthmus SCALAR_SIGS only recognises SqlLibraryOperators variants.
     * The DataFusion adapter rewrites the UDF call to the Calcite-library
     * operator before Substrait conversion, but the name-based lookup here
     * must still succeed so STANDARD_PROJECT_OPS and adapter dispatch can run.
     */
    public void testTier2AdapterTargetFunctionsExistByName() {
        // PPL's COSH/SINH UDFs have getName() = "COSH"/"SINH"; valueOf succeeds.
        assertSame(ScalarFunction.COSH, ScalarFunction.valueOf("COSH"));
        assertSame(ScalarFunction.SINH, ScalarFunction.valueOf("SINH"));
        // PPL's E() and EXPM1 UDFs likewise resolve by name.
        assertSame(ScalarFunction.E, ScalarFunction.valueOf("E"));
        assertSame(ScalarFunction.EXPM1, ScalarFunction.valueOf("EXPM1"));
    }

    /** Category hygiene: every math enum constant belongs to the MATH category. */
    public void testMathFunctionsHaveMathCategory() {
        List<ScalarFunction> mathFuncs = List.of(
            ScalarFunction.ABS,
            ScalarFunction.ACOS,
            ScalarFunction.ASIN,
            ScalarFunction.ATAN,
            ScalarFunction.ATAN2,
            ScalarFunction.CBRT,
            ScalarFunction.CEIL,
            ScalarFunction.COS,
            ScalarFunction.COSH,
            ScalarFunction.COT,
            ScalarFunction.DEGREES,
            ScalarFunction.E,
            ScalarFunction.EXP,
            ScalarFunction.EXPM1,
            ScalarFunction.FLOOR,
            ScalarFunction.LN,
            ScalarFunction.LOG,
            ScalarFunction.LOG10,
            ScalarFunction.LOG2,
            ScalarFunction.PI,
            ScalarFunction.POWER,
            ScalarFunction.RADIANS,
            ScalarFunction.RAND,
            ScalarFunction.ROUND,
            ScalarFunction.SCALAR_MAX,
            ScalarFunction.SCALAR_MIN,
            ScalarFunction.SIGN,
            ScalarFunction.SIN,
            ScalarFunction.SINH,
            ScalarFunction.TAN,
            ScalarFunction.TRUNCATE
        );
        for (ScalarFunction func : mathFuncs) {
            assertSame("expected MATH category for " + func, ScalarFunction.Category.MATH, func.getCategory());
        }
    }
}

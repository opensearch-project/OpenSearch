/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumMap;
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

    // ── fromSqlOperator: SqlKind branch ────────────────────────────────────────

    public void testFromSqlOperatorResolvesViaSqlKind() {
        // Calcite's CAST has a dedicated SqlKind.CAST — short-circuit before name lookup.
        assertEquals(ScalarFunction.CAST, ScalarFunction.fromSqlOperator(SqlStdOperatorTable.CAST));
        assertEquals(ScalarFunction.PLUS, ScalarFunction.fromSqlOperator(SqlStdOperatorTable.PLUS));
        assertEquals(ScalarFunction.GREATER_THAN, ScalarFunction.fromSqlOperator(SqlStdOperatorTable.GREATER_THAN));
        assertEquals(ScalarFunction.COALESCE, ScalarFunction.fromSqlOperator(SqlStdOperatorTable.COALESCE));
    }

    // ── fromSqlOperator: symbolic-name branch ──────────────────────────────────

    public void testFromSqlOperatorResolvesPipeConcatViaSymbolicName() {
        // The original "no backend supports scalar function [null]" symptom for PPL string `+`.
        // SqlStdOperatorTable.CONCAT is a SqlBinaryOperator named "||" with SqlKind.OTHER —
        // neither fromSqlKind nor fromSqlFunction(SqlFunction) resolves it.
        assertEquals("||", SqlStdOperatorTable.CONCAT.getName());
        assertEquals(SqlKind.OTHER, SqlStdOperatorTable.CONCAT.getKind());
        assertEquals(ScalarFunction.CONCAT, ScalarFunction.fromSqlOperator(SqlStdOperatorTable.CONCAT));
    }

    // ── fromSqlOperator: identifier-name branch ────────────────────────────────

    public void testFromSqlOperatorResolvesViaIdentifierName() {
        // SqlStdOperatorTable.UPPER is a SqlFunction named "UPPER" with SqlKind.OTHER_FUNCTION;
        // resolves through the valueOf(name.toUpperCase()) fallback after SqlKind misses.
        assertEquals(ScalarFunction.UPPER, ScalarFunction.fromSqlOperator(SqlStdOperatorTable.UPPER));
        assertEquals(ScalarFunction.LOWER, ScalarFunction.fromSqlOperator(SqlStdOperatorTable.LOWER));
        assertEquals(ScalarFunction.ABS, ScalarFunction.fromSqlOperator(SqlStdOperatorTable.ABS));
    }

    public void testFromSqlOperatorReturnsNullForUnknownFunction() {
        // UNARY_MINUS has SqlKind.MINUS_PREFIX (no enum) and name "-" (not a valid valueOf input);
        // both resolution paths miss and the resolver returns null instead of throwing.
        assertNull(ScalarFunction.fromSqlOperator(SqlStdOperatorTable.UNARY_MINUS));
    }
}

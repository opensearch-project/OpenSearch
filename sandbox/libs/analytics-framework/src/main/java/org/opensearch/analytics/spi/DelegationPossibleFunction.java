/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Marker function for predicates whose evaluation may be opportunistically delegated
 * to a peer backend at runtime.
 *
 * <p>Wraps the original predicate verbatim — the driving backend evaluates it natively
 * by default. If the driving backend's own pruning isn't selective enough for a row
 * group, it consults the peer (whose query is identified by {@code annotationId}) and
 * intersects the resulting bitset with its own candidates.
 *
 * <p>Plan shape after FragmentConversion (Performance branch):
 * <pre>
 *   delegation_possible(originalPredicate, annotationId)
 * </pre>
 * The original predicate survives the Substrait round-trip; the driving backend
 * unwraps it for its own evaluation, and uses the {@code annotationId} to look up
 * the peer-side compiled query when needed.
 *
 * @opensearch.internal
 */
public final class DelegationPossibleFunction {

    /** The function name used in Calcite plans and Substrait serialization. */
    public static final String NAME = "delegation_possible";

    /**
     * Singleton Calcite SqlFunction: {@code delegation_possible(BOOLEAN, INT) → BOOLEAN}.
     * arg0 = original predicate expression, arg1 = annotation id.
     */
    public static final SqlFunction FUNCTION = new SqlFunction(
        NAME,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.NUMERIC),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private DelegationPossibleFunction() {}

    /** Builds a {@code delegation_possible(original, annotationId)} RexCall. */
    public static RexNode makeCall(RexBuilder rexBuilder, RexNode original, int annotationId) {
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        return rexBuilder.makeCall(FUNCTION, original, rexBuilder.makeLiteral(annotationId, intType, false));
    }
}

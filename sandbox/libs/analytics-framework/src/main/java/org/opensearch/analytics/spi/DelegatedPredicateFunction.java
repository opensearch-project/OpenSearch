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
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Placeholder function for delegated predicates in the Calcite plan.
 *
 * <p>When a predicate is delegated to another backend (e.g., a MATCH_PHRASE predicate
 * delegated from DataFusion to Lucene), the original expression is serialized and sent
 * as opaque bytes. In the Calcite plan that goes to Isthmus/Substrait, the original
 * expression is replaced with {@code delegated_predicate(annotationId)} — a function
 * that always evaluates to TRUE and carries the annotation ID so the driving backend
 * can look up the delegated query at execution time.
 *
 * @opensearch.internal
 */
public final class DelegatedPredicateFunction {

    /** The function name used in Calcite plans and Substrait serialization. */
    public static final String NAME = "delegated_predicate";

    /** Singleton Calcite SqlFunction: {@code delegated_predicate(INT) → BOOLEAN}. */
    public static final SqlFunction FUNCTION = new SqlFunction(
        NAME,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private DelegatedPredicateFunction() {}

    /** Builds a {@code delegated_predicate(annotationId)} RexCall. */
    public static RexNode makeCall(RexBuilder rexBuilder, int annotationId) {
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        return rexBuilder.makeCall(FUNCTION, rexBuilder.makeLiteral(annotationId, intType, false));
    }
}

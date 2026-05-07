/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link LikeAdapter} — verifies the adapter drops Calcite's default
 * 3rd (escape) operand so the call shape matches Substrait's 2-arg {@code like} /
 * {@code ilike} signatures, while leaving the operator (LIKE vs ILIKE) unchanged.
 */
public class LikeAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    public void testIlikeWithEscapeDropsEscapeAndKeepsIlikeOperator() {
        RexNode field = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode pattern = rexBuilder.makeLiteral("%e%");
        RexNode escape = rexBuilder.makeLiteral("\\");
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.ILIKE, List.of(field, pattern, escape));

        RexCall adapted = (RexCall) new LikeAdapter().adapt(original, List.of(), cluster);

        assertSame(
            "ILIKE operator must be preserved so Isthmus can serialize it as ilike",
            SqlLibraryOperators.ILIKE,
            adapted.getOperator()
        );
        assertEquals("3rd (escape) operand must be dropped", 2, adapted.getOperands().size());
    }

    public void testLikeWithEscapeDropsEscapeAndKeepsLikeOperator() {
        RexNode field = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode pattern = rexBuilder.makeLiteral("%e%");
        RexNode escape = rexBuilder.makeLiteral("\\");
        RexCall original = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LIKE, List.of(field, pattern, escape));

        RexCall adapted = (RexCall) new LikeAdapter().adapt(original, List.of(), cluster);

        assertSame("LIKE operator must be preserved", SqlStdOperatorTable.LIKE, adapted.getOperator());
        assertEquals("3rd (escape) operand must be dropped", 2, adapted.getOperands().size());
    }

    public void testTwoArgLikeIsReturnedUnchanged() {
        RexNode field = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode pattern = rexBuilder.makeLiteral("%e%");
        RexCall original = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LIKE, List.of(field, pattern));

        RexNode adapted = new LikeAdapter().adapt(original, List.of(), cluster);

        assertSame("2-arg LIKE should pass through unchanged", original, adapted);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

public class CastToVarcharRewriterTests extends OpenSearchTestCase {

    /** CAST(boolean AS VARCHAR) → CASE WHEN val THEN 'TRUE' WHEN NOT val THEN 'FALSE' END. */
    public void testBooleanCastRewritesToCaseUpper() {
        Cluster c = newCluster();
        RexNode operand = c.inputRef(0, SqlTypeName.BOOLEAN);
        RexNode cast = c.castTo(operand, SqlTypeName.VARCHAR);

        RexCall out = (RexCall) cast.accept(CastToVarcharRewriter.newShuttle(c.rexBuilder));

        assertEquals(SqlKind.CASE, out.getKind());
        assertEquals("TRUE", ((RexLiteral) out.getOperands().get(1)).getValueAs(String.class));
        assertEquals("FALSE", ((RexLiteral) out.getOperands().get(3)).getValueAs(String.class));
    }

    /** Temporal casts pass through — handled by ArrowValues post-processing instead. */
    public void testTimestampCastPassesThrough() {
        Cluster c = newCluster();
        RexNode operand = c.inputRef(0, SqlTypeName.TIMESTAMP);
        RexNode cast = c.castTo(operand, SqlTypeName.VARCHAR);

        RexNode out = cast.accept(CastToVarcharRewriter.newShuttle(c.rexBuilder));

        assertEquals(SqlKind.CAST, out.getKind());
    }

    /** Unrelated casts (INT → VARCHAR) pass through. */
    public void testIntegerCastPassesThrough() {
        Cluster c = newCluster();
        RexNode operand = c.inputRef(0, SqlTypeName.INTEGER);
        RexNode cast = c.castTo(operand, SqlTypeName.VARCHAR);

        RexNode out = cast.accept(CastToVarcharRewriter.newShuttle(c.rexBuilder));

        assertEquals(SqlKind.CAST, out.getKind());
    }

    /** Non-cast calls pass through. */
    public void testNonCastCallPassesThrough() {
        Cluster c = newCluster();
        RexNode left = c.inputRef(0, SqlTypeName.INTEGER);
        RexNode right = c.inputRef(1, SqlTypeName.INTEGER);
        RexNode plus = c.rexBuilder.makeCall(SqlStdOperatorTable.PLUS, left, right);

        RexNode out = plus.accept(CastToVarcharRewriter.newShuttle(c.rexBuilder));

        assertSame(plus, out);
    }

    private static Cluster newCluster() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        return new Cluster(typeFactory, rexBuilder);
    }

    private static final class Cluster {
        final RelDataTypeFactory typeFactory;
        final RexBuilder rexBuilder;

        Cluster(RelDataTypeFactory typeFactory, RexBuilder rexBuilder) {
            this.typeFactory = typeFactory;
            this.rexBuilder = rexBuilder;
        }

        RexNode inputRef(int index, SqlTypeName type) {
            return rexBuilder.makeInputRef(typeFactory.createTypeWithNullability(typeFactory.createSqlType(type), true), index);
        }

        RexNode castTo(RexNode operand, SqlTypeName target) {
            RelDataType targetType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(target), true);
            return rexBuilder.makeCast(targetType, operand);
        }
    }
}

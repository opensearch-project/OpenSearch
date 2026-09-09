/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class MinusAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private MinusAdapter adapter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
        adapter = new MinusAdapter();
    }

    public void testTimestampMinusTimestampRewritesToUnixSecondsDiff() {
        RexCall call = minusOf(SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP);
        RexNode adapted = adapter.adapt(call, List.of(), cluster);

        // Expect: CAST(from_unixtime(CAST(MINUS(to_unixtime(t1), to_unixtime(t2)) AS DOUBLE)) AS TIMESTAMP).
        RexCall fromUnixtime = (RexCall) unwrapCast(adapted);
        assertSame(RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, fromUnixtime.getOperator());

        RexCall minus = (RexCall) unwrapCast(fromUnixtime.getOperands().get(0));
        assertSame(SqlStdOperatorTable.MINUS, minus.getOperator());
        assertSame(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, ((RexCall) minus.getOperands().get(0)).getOperator());
        assertSame(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, ((RexCall) minus.getOperands().get(1)).getOperator());
    }

    public void testDateMinusDateReturnsBigIntDayCount() {
        RexCall call = minusOf(SqlTypeName.DATE, SqlTypeName.DATE);
        RexNode adapted = adapter.adapt(call, List.of(), cluster);

        // DATE-DATE → integer day-count: DIVIDE(MINUS(to_unixtime(L), to_unixtime(R)), 86400).
        RexCall divide = (RexCall) adapted;
        assertSame(SqlStdOperatorTable.DIVIDE, divide.getOperator());
        assertSame(SqlTypeName.BIGINT, divide.getType().getSqlTypeName());

        RexCall minus = (RexCall) divide.getOperands().get(0);
        assertSame(SqlStdOperatorTable.MINUS, minus.getOperator());
        assertSame(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, ((RexCall) minus.getOperands().get(0)).getOperator());
        assertSame(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, ((RexCall) minus.getOperands().get(1)).getOperator());
    }

    public void testNumericMinusPassesThroughUnchanged() {
        RexCall call = minusOf(SqlTypeName.INTEGER, SqlTypeName.INTEGER);
        assertSame("numeric subtract must be returned untouched", call, adapter.adapt(call, List.of(), cluster));
    }

    public void testNonMinusOperatorPassesThrough() {
        RelDataType ts = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RexCall plus = (RexCall) rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(ts, 0),
            rexBuilder.makeInputRef(ts, 1)
        );
        assertSame(plus, adapter.adapt(plus, List.of(), cluster));
    }

    public void testUnaryMinusPassesThrough() {
        RelDataType i32 = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexCall unary = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, rexBuilder.makeInputRef(i32, 0));
        assertSame(unary, adapter.adapt(unary, List.of(), cluster));
    }

    public void testMinusOfWindowAggsPassesThrough() {
        // WidthBucketAdapter pattern-matches MINUS(MAX OVER (), MIN OVER ()) for `bin <ts> bins=N`.
        // MinusAdapter must leave it intact so the downstream adapter still sees SqlKind.MINUS.
        RelDataType ts = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RexNode tsCol = rexBuilder.makeInputRef(ts, 0);
        RexNode maxOver = makeOverEmpty(SqlStdOperatorTable.MAX, tsCol, ts);
        RexNode minOver = makeOverEmpty(SqlStdOperatorTable.MIN, tsCol, ts);
        RexCall minus = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.MINUS, maxOver, minOver);
        assertSame(minus, adapter.adapt(minus, List.of(), cluster));
    }

    private RexNode makeOverEmpty(SqlAggFunction agg, RexNode arg, RelDataType returnType) {
        return rexBuilder.makeOver(
            returnType,
            agg,
            List.of(arg),
            List.of(),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            true,
            true,
            false,
            false,
            false
        );
    }

    private RexCall minusOf(SqlTypeName left, SqlTypeName right) {
        RexNode l = rexBuilder.makeInputRef(typeFactory.createSqlType(left), 0);
        RexNode r = rexBuilder.makeInputRef(typeFactory.createSqlType(right), 1);
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.MINUS, l, r);
    }

    private static RexNode unwrapCast(RexNode node) {
        while (node instanceof RexCall call && call.getKind() == SqlKind.CAST) {
            node = call.getOperands().get(0);
        }
        return node;
    }
}

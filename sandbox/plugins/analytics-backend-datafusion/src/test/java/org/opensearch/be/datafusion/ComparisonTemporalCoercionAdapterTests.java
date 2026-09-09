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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/** Unit tests for {@link ComparisonTemporalCoercionAdapter}. */
public class ComparisonTemporalCoercionAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    private final ComparisonTemporalCoercionAdapter adapter = new ComparisonTemporalCoercionAdapter();

    private RexNode field(SqlTypeName name) {
        return rexBuilder.makeInputRef(typeFactory.createSqlType(name), 0);
    }

    private RexCall eq(RexNode l, RexNode r) {
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(l, r));
    }

    /** TIME vs TIMESTAMP: TIME side rewritten to today-anchored TIMESTAMP, TIMESTAMP side untouched. */
    public void testTimeVsTimestampCoercesTimeSide() {
        RexNode time = field(SqlTypeName.TIME);
        RexNode ts = field(SqlTypeName.TIMESTAMP);
        RexCall original = eq(time, ts);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame(original, adapted);
        RexNode newLeft = adapted.getOperands().get(0);
        assertEquals(SqlKind.CAST, newLeft.getKind());
        assertSame(SqlTypeName.TIMESTAMP, newLeft.getType().getSqlTypeName());
        assertSame("TIMESTAMP side must not be rewritten", ts, adapted.getOperands().get(1));
    }

    /** TIMESTAMP vs TIME: symmetric case, only TIME side is rewritten. */
    public void testTimestampVsTimeCoercesTimeSide() {
        RexNode ts = field(SqlTypeName.TIMESTAMP);
        RexNode time = field(SqlTypeName.TIME);
        RexCall original = eq(ts, time);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame(original, adapted);
        assertSame("TIMESTAMP side must not be rewritten", ts, adapted.getOperands().get(0));
        RexNode newRight = adapted.getOperands().get(1);
        assertEquals(SqlKind.CAST, newRight.getKind());
        assertSame(SqlTypeName.TIMESTAMP, newRight.getType().getSqlTypeName());
    }

    /** TIME vs DATE: same path — TIME rewritten, DATE untouched. */
    public void testTimeVsDateCoercesTimeSide() {
        RexNode time = field(SqlTypeName.TIME);
        RexNode date = field(SqlTypeName.DATE);
        RexCall original = eq(time, date);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame(original, adapted);
        assertSame(SqlTypeName.TIMESTAMP, adapted.getOperands().get(0).getType().getSqlTypeName());
        assertSame(date, adapted.getOperands().get(1));
    }

    /** TIME vs TIME — load-bearing guard preserves the native comparison. */
    public void testTimeVsTimePassesThrough() {
        RexCall original = eq(field(SqlTypeName.TIME), field(SqlTypeName.TIME));

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertSame(original, adapted);
    }

    /** DATE vs TIMESTAMP — Substrait already binds; passthrough. */
    public void testDateVsTimestampPassesThrough() {
        RexCall original = eq(field(SqlTypeName.DATE), field(SqlTypeName.TIMESTAMP));

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertSame(original, adapted);
    }

    /** VARCHAR vs TIMESTAMP — pre-existing char-vs-temporal branch still fires after the TIME branch addition. */
    public void testVarcharVsTimestampStillCoerced() {
        RexNode varchar = field(SqlTypeName.VARCHAR);
        RexNode ts = field(SqlTypeName.TIMESTAMP);
        RexCall original = eq(varchar, ts);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame(original, adapted);
        assertSame(SqlTypeName.TIMESTAMP, adapted.getOperands().get(0).getType().getSqlTypeName());
        assertSame(ts, adapted.getOperands().get(1));
    }

    /** Numeric-vs-numeric — adapter is registered against comparison ops generically; non-temporal calls pass through. */
    public void testNumericComparisonPassesThrough() {
        RexCall original = eq(field(SqlTypeName.INTEGER), field(SqlTypeName.INTEGER));

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertSame(original, adapted);
    }

    /**
     * Calcite's binary-comparison type-coercion wraps the TIME side with
     * {@code to_timestamp(time)} to align with a DATE peer — that's the actual production input
     * shape (not a raw TIME ref). The adapter must unwrap to find the inner TIME and rewrite it.
     */
    public void testToTimestampWrappedTimeVsDateCoerces() {
        RexNode timeRef = field(SqlTypeName.TIME);
        RexNode wrappedTime = rexBuilder.makeCall(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
            DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP,
            List.of(timeRef)
        );
        RexNode date = field(SqlTypeName.DATE);
        RexCall original = eq(wrappedTime, date);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame("wrapped TIME must trigger the coercion path", original, adapted);
        assertSame(SqlTypeName.TIMESTAMP, adapted.getOperands().get(0).getType().getSqlTypeName());
        assertSame("DATE peer must not be rewritten", date, adapted.getOperands().get(1));
    }

    /**
     * {@code CAST(time AS TIMESTAMP)} is the other wrapped form Calcite's coercion may emit. The
     * adapter's {@code unwrapToTime} also recognises CAST as a single-operand wrapper.
     */
    public void testCastWrappedTimeVsTimestampCoerces() {
        RexNode timeRef = field(SqlTypeName.TIME);
        RexNode castTime = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), timeRef);
        RexNode ts = field(SqlTypeName.TIMESTAMP);
        RexCall original = eq(castTime, ts);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame("CAST-wrapped TIME must trigger the coercion path", original, adapted);
        assertSame(SqlTypeName.TIMESTAMP, adapted.getOperands().get(0).getType().getSqlTypeName());
        assertSame(ts, adapted.getOperands().get(1));
    }

    /**
     * The DATE/TIMESTAMP peer can also arrive wrapped (e.g. {@code to_timestamp(date)}); the adapter
     * still has to recognise it as a temporal peer to fire the TIME-side rewrite.
     */
    public void testTimeVsToTimestampWrappedDateCoerces() {
        RexNode time = field(SqlTypeName.TIME);
        RexNode dateRef = field(SqlTypeName.DATE);
        RexNode wrappedDate = rexBuilder.makeCall(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
            DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP,
            List.of(dateRef)
        );
        RexCall original = eq(time, wrappedDate);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame("wrapped DATE peer must still trigger TIME-side coercion", original, adapted);
        assertSame(SqlTypeName.TIMESTAMP, adapted.getOperands().get(0).getType().getSqlTypeName());
        assertSame("wrapped DATE peer must not be re-rewritten", wrappedDate, adapted.getOperands().get(1));
    }
}

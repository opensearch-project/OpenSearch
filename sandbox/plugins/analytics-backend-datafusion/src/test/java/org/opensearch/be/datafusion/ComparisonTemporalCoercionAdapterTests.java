/* SPDX-License-Identifier: Apache-2.0 */
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
import org.opensearch.analytics.schema.IpType;
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

    private RexNode ipField(boolean nullable) {
        return rexBuilder.makeInputRef(new IpType(nullable), 0);
    }

    private RexNode varbinaryField(boolean nullable) {
        return rexBuilder.makeInputRef(
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARBINARY), nullable),
            0
        );
    }

    private RexCall eq(RexNode l, RexNode r) {
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(l, r));
    }

    // ── IpType comparison tests ─────────────────────────────────────────────

    /** IpType vs IpType: both operands must be cast to plain VARBINARY. */
    public void testIpTypeVsIpTypeCastToVarbinary() {
        RexCall original = eq(ipField(true), ipField(true));

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame(original, adapted);
        for (int i = 0; i < 2; i++) {
            RexNode operand = adapted.getOperands().get(i);
            assertEquals(SqlKind.CAST, operand.getKind());
            assertSame(SqlTypeName.VARBINARY, operand.getType().getSqlTypeName());
        }
    }

    /** IpType vs VARBINARY: only the IpType side is cast; VARBINARY side passes through. */
    public void testIpTypeVsVarbinaryCastsIpSideOnly() {
        RexCall original = eq(ipField(true), varbinaryField(true));

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame(original, adapted);
        // Left (IpType) must be cast to VARBINARY
        assertEquals(SqlKind.CAST, adapted.getOperands().get(0).getKind());
        assertSame(SqlTypeName.VARBINARY, adapted.getOperands().get(0).getType().getSqlTypeName());
        // Right (VARBINARY) must pass through unchanged
        assertSame(original.getOperands().get(1), adapted.getOperands().get(1));
    }

    /** VARBINARY vs IpType: symmetric — only the IpType side is cast. */
    public void testVarbinaryVsIpTypeCastsIpSideOnly() {
        RexCall original = eq(varbinaryField(true), ipField(true));

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame(original, adapted);
        assertSame(original.getOperands().get(0), adapted.getOperands().get(0));
        assertEquals(SqlKind.CAST, adapted.getOperands().get(1).getKind());
        assertSame(SqlTypeName.VARBINARY, adapted.getOperands().get(1).getType().getSqlTypeName());
    }

    /** IpType vs VARCHAR: IpType is cast to VARBINARY; VARCHAR remains unchanged (temporal coercion may handle it separately). */
    public void testIpTypeVsVarchar() {
        RexCall original = eq(ipField(true), field(SqlTypeName.VARCHAR));

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame(original, adapted);
        assertEquals(SqlKind.CAST, adapted.getOperands().get(0).getKind());
        assertSame(SqlTypeName.VARBINARY, adapted.getOperands().get(0).getType().getSqlTypeName());
        assertSame(original.getOperands().get(1), adapted.getOperands().get(1));
    }

    /** Plain VARBINARY vs VARBINARY (no IpType involved) must pass through unchanged. */
    public void testPlainVarbinaryComparisonPassesThrough() {
        RexCall original = eq(varbinaryField(true), varbinaryField(true));

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertSame(original, adapted);
    }

    // ── Original temporal coercion tests ─────────────────────────────────────

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

    /** Calcite's binary-comparison type-coercion wraps the TIME side with to_timestamp(time). */
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

    /** CAST(time AS TIMESTAMP) wrapped form. */
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

    /** TIME vs to_timestamp-wrapped DATE. */
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

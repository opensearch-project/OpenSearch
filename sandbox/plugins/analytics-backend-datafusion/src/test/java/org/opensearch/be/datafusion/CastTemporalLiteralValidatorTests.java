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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class CastTemporalLiteralValidatorTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
    }

    /** CAST('2025-13-02' AS DATE) → IllegalArgumentException with date format-hint. */
    public void testInvalidDateLiteralCastRejected() {
        RexNode cast = castStringLiteralTo("2025-13-02", SqlTypeName.DATE);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cast.accept(CastTemporalLiteralValidator.newShuttle())
        );
        assertEquals("date:2025-13-02 in unsupported format, please use 'yyyy-MM-dd'", e.getMessage());
    }

    /** CAST('09:07:42' AS DATE) — bare time string is not a valid date literal (cluster G1). */
    public void testTimeLiteralCastToDateRejected() {
        RexNode cast = castStringLiteralTo("09:07:42", SqlTypeName.DATE);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cast.accept(CastTemporalLiteralValidator.newShuttle())
        );
        assertEquals("date:09:07:42 in unsupported format, please use 'yyyy-MM-dd'", e.getMessage());
    }

    /** CAST('1984-04-12' AS TIME) → IllegalArgumentException with time format-hint. */
    public void testDateLiteralCastToTimeRejected() {
        RexNode cast = castStringLiteralTo("1984-04-12", SqlTypeName.TIME);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cast.accept(CastTemporalLiteralValidator.newShuttle())
        );
        assertEquals("time:1984-04-12 in unsupported format, please use 'HH:mm:ss[.SSSSSSSSS]'", e.getMessage());
    }

    /** CAST('09:07:42' AS TIMESTAMP) rejected — bare time is not a valid timestamp literal. */
    public void testBareTimeCastToTimestampRejected() {
        RexNode cast = castStringLiteralTo("09:07:42", SqlTypeName.TIMESTAMP);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cast.accept(CastTemporalLiteralValidator.newShuttle())
        );
        assertEquals("timestamp:09:07:42 in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'", e.getMessage());
    }

    /** CAST('1984-04-12' AS TIMESTAMP) accepted — bare date is valid timestamp input. */
    public void testBareDateCastToTimestampAccepted() {
        RexNode cast = castStringLiteralTo("1984-04-12", SqlTypeName.TIMESTAMP);
        cast.accept(CastTemporalLiteralValidator.newShuttle());  // no throw
    }

    /** CAST('garbage' AS TIMESTAMP) → IllegalArgumentException with timestamp format-hint. */
    public void testGarbageStringCastToTimestampRejected() {
        RexNode cast = castStringLiteralTo("not-a-timestamp", SqlTypeName.TIMESTAMP);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cast.accept(CastTemporalLiteralValidator.newShuttle())
        );
        assertEquals("timestamp:not-a-timestamp in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'", e.getMessage());
    }

    /** Non-string-literal cast (column ref → DATE) passes through unchanged. */
    public void testColumnRefCastNotValidated() {
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode columnRef = rexBuilder.makeInputRef(varcharType, 0);
        RelDataType dateType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);
        RexNode cast = rexBuilder.makeCast(dateType, columnRef);
        cast.accept(CastTemporalLiteralValidator.newShuttle());  // no throw
    }

    /** TIMESTAMPADD with bad string literal arg — format-hint exception (substrait would otherwise reject opaquely). */
    public void testTimestampAddBadLiteralRejected() {
        RexNode call = buildTimestampAddDiff(
            "TIMESTAMPADD",
            List.of(makeStringLiteral("HOUR"), makeIntLiteral(1), makeStringLiteral("16:00:61"))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> call.accept(CastTemporalLiteralValidator.newShuttle())
        );
        assertEquals("timestamp:16:00:61 in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'", e.getMessage());
    }

    /** TIMESTAMPDIFF with bad string literal arg — format-hint exception on the first bad arg. */
    public void testTimestampDiffBadLiteralRejected() {
        RexNode call = buildTimestampAddDiff(
            "TIMESTAMPDIFF",
            List.of(makeStringLiteral("HOUR"), makeStringLiteral("2025-13-02"), makeStringLiteral("2025-13-02"))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> call.accept(CastTemporalLiteralValidator.newShuttle())
        );
        assertEquals("timestamp:2025-13-02 in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'", e.getMessage());
    }

    private RexNode buildTimestampAddDiff(String name, List<RexNode> operands) {
        SqlOperator op = new SqlFunction(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT_NULLABLE,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.TIMEDATE
        );
        return rexBuilder.makeCall(typeFactory.createSqlType(SqlTypeName.BIGINT), op, operands);
    }

    private RexLiteral makeStringLiteral(String value) {
        RelDataType nonNullVarchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        return (RexLiteral) rexBuilder.makeLiteral(value, nonNullVarchar, false);
    }

    private RexLiteral makeIntLiteral(int value) {
        return (RexLiteral) rexBuilder.makeLiteral(
            java.math.BigDecimal.valueOf(value),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            false
        );
    }

    private RexNode castStringLiteralTo(String value, SqlTypeName target) {
        RelDataType nonNullVarchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexLiteral literal = (RexLiteral) rexBuilder.makeLiteral(value, nonNullVarchar, false);
        RelDataType targetType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(target), true);
        return rexBuilder.makeCast(targetType, literal);
    }
}

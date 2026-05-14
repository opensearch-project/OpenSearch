/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Date-part extractor adapters — rewrite {@code FN(ts)} to {@code date_part('<unit>', ts)}.
 * Alias pairs (e.g. MONTH_OF_YEAR → MONTH) share an adapter instance at registration.
 *
 * <p>TIME operand handling: isthmus emits Calcite's TIME as Substrait
 * {@code precision_time<P>?}, which binds to no {@code date_part} sig in our
 * yaml (declaring that sig directly triggers a runtime
 * {@code ParameterizedTypeThrowsVisitor} error on every call). Pre-cast TIME
 * to VARCHAR so the recursive {@code DatetimeOperandCoercer} then re-casts to
 * TIMESTAMP, landing on the yaml's {@code precision_timestamp<P>} sig.
 *
 * @opensearch.internal
 */
class DatePartAdapters implements ScalarFunctionAdapter {

    private final String unit;

    DatePartAdapters(String unit) {
        this.unit = unit;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType varcharType = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode partLiteral = rexBuilder.makeLiteral(unit, varcharType, true);
        RexNode operand = original.getOperands().get(0);
        if (operand.getType().getSqlTypeName() == SqlTypeName.TIME) {
            RexNode synthesized = DatetimeLiteralHelper.unwrapTimeLiteralToTimestamp(operand, rexBuilder);
            if (synthesized != null) {
                operand = synthesized;
            } else {
                RelDataType nullableVarchar = cluster.getTypeFactory()
                    .createTypeWithNullability(varcharType, operand.getType().isNullable());
                operand = rexBuilder.makeCast(nullableVarchar, operand);
            }
        }
        return rexBuilder.makeCall(original.getType(), SqlLibraryOperators.DATE_PART, List.of(partLiteral, operand));
    }

    static DatePartAdapters year() {
        return new DatePartAdapters("year");
    }

    static DatePartAdapters quarter() {
        return new DatePartAdapters("quarter");
    }

    static DatePartAdapters month() {
        return new DatePartAdapters("month");
    }

    static DatePartAdapters day() {
        return new DatePartAdapters("day");
    }

    static DatePartAdapters dayOfYear() {
        return new DatePartAdapters("doy");
    }

    static DatePartAdapters hour() {
        return new DatePartAdapters("hour");
    }

    static DatePartAdapters minute() {
        return new DatePartAdapters("minute");
    }

    static DatePartAdapters microsecond() {
        return new DatePartAdapters("microsecond");
    }

    static DatePartAdapters week() {
        return new DatePartAdapters("week");
    }
}

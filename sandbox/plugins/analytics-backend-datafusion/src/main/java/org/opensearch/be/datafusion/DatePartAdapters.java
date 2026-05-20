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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Date-part extractor adapters — rewrite {@code FN(ts)} to {@code date_part('<unit>', ts)}.
 * Alias pairs (e.g. MONTH_OF_YEAR → MONTH) share an adapter instance at registration.
 *
 * <p>Operand coercion: PPL allows bare string-literal datetime arguments
 * (e.g. {@code DAY('2020-09-16')}). Calcite types those operands as
 * {@code VARCHAR}, but {@code opensearch_scalar_functions.yaml} declares
 * {@code date_part} only for {@code (string, precision_timestamp<P>)} and
 * {@code (string, date)}. Without coercion, isthmus' signature matcher fails
 * with {@code "Unable to convert call DATE_PART(string, string)"}. This adapter
 * wraps any character-family operand in {@code CAST(operand AS TIMESTAMP)} so
 * the call resolves to the precision_timestamp impl. DataFusion's CAST kernel
 * parses ISO-8601 datetime strings; malformed inputs surface as a runtime
 * Arrow cast error from the engine.
 *
 * @opensearch.internal
 */
final class DatePartAdapters extends AbstractNameMappingAdapter {

    private final String unit;

    DatePartAdapters(String unit) {
        super(SqlLibraryOperators.DATE_PART, List.of(unit), List.of());
        this.unit = unit;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().stream().noneMatch(DatePartAdapters::isCharacterOperand)) {
            return super.adapt(original, fieldStorage, cluster);
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> coerced = new ArrayList<>(original.getOperands().size());
        for (RexNode operand : original.getOperands()) {
            coerced.add(isCharacterOperand(operand) ? castToTimestamp(operand, cluster) : operand);
        }
        RelDataType unitType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        List<RexNode> args = new ArrayList<>(coerced.size() + 1);
        args.add(rexBuilder.makeLiteral(unit, unitType, true));
        args.addAll(coerced);
        return rexBuilder.makeCall(original.getType(), SqlLibraryOperators.DATE_PART, args);
    }

    private static boolean isCharacterOperand(RexNode operand) {
        return SqlTypeFamily.CHARACTER.contains(operand.getType());
    }

    private static RexNode castToTimestamp(RexNode operand, RelOptCluster cluster) {
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType timestampType = factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.TIMESTAMP),
            operand.getType().isNullable()
        );
        return cluster.getRexBuilder().makeCast(timestampType, operand);
    }

    /**
     * Shared coercion helper for sibling adapters ({@link DayOfWeekAdapter}, {@link SecondAdapter})
     * that build {@code date_part('<unit>', operand)} calls directly. Wraps a character-family
     * operand in {@code CAST(_ AS TIMESTAMP)}; non-character operands are returned unchanged.
     */
    static RexNode coerceCharacterOperandToTimestamp(RexNode operand, RelOptCluster cluster) {
        return isCharacterOperand(operand) ? castToTimestamp(operand, cluster) : operand;
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

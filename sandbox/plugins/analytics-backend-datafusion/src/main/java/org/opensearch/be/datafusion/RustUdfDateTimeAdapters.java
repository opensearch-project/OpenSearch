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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.NumericToDoubleAdapter;

import java.util.List;

/**
 * Adapters for PPL datetime functions routed to Rust UDFs. Each {@code LOCAL_*_OP}
 * names a Calcite {@link SqlFunction} matching a UDF in {@code rust/src/udf/mod.rs};
 * Substrait sigs live in {@code opensearch_scalar_functions.yaml} +
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS}.
 *
 * @opensearch.internal
 */
final class RustUdfDateTimeAdapters {

    private RustUdfDateTimeAdapters() {}

    private static SqlOperator udf(String name, SqlReturnTypeInference ret, SqlOperandTypeChecker operands) {
        return new SqlFunction(name, SqlKind.OTHER_FUNCTION, ret, null, operands, SqlFunctionCategory.TIMEDATE);
    }

    static final SqlOperator LOCAL_EXTRACT_OP = udf("opensearch_extract", ReturnTypes.BIGINT_NULLABLE, OperandTypes.ANY_ANY);
    static final SqlOperator LOCAL_FROM_UNIXTIME_OP = udf("from_unixtime", ReturnTypes.TIMESTAMP_NULLABLE, OperandTypes.ANY);
    static final SqlOperator LOCAL_MAKETIME_OP = udf(
        "maketime",
        ReturnTypes.TIME_NULLABLE,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY)
    );
    static final SqlOperator LOCAL_MAKEDATE_OP = udf("makedate", ReturnTypes.DATE_NULLABLE, OperandTypes.ANY_ANY);
    static final SqlOperator LOCAL_DATE_FORMAT_OP = udf("date_format", ReturnTypes.VARCHAR_NULLABLE, OperandTypes.ANY_ANY);
    static final SqlOperator LOCAL_TIME_FORMAT_OP = udf("time_format", ReturnTypes.VARCHAR_NULLABLE, OperandTypes.ANY_ANY);
    static final SqlOperator LOCAL_STR_TO_DATE_OP = udf("str_to_date", ReturnTypes.TIMESTAMP_NULLABLE, OperandTypes.ANY_ANY);

    /**
     * Adapter for PPL {@code extract(<unit> FROM <expr>)}.
     * <p>For TIME(p) operands, CASTs to VARCHAR so the call binds to the {@code (string, string)}
     * yaml overload — substrait-java 0.89.1 can't emit a {@code precision_time<P>} signature.
     * Other operand types (DATE / TIMESTAMP / VARCHAR) pass through unchanged.
     */
    static final class ExtractAdapter extends AbstractNameMappingAdapter {

        ExtractAdapter() {
            super(LOCAL_EXTRACT_OP, List.of(), List.of());
        }

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexCall toAdapt = isTimeValueOperand(original) ? castValueOperandToVarchar(original, cluster) : original;
            return super.adapt(toAdapt, fieldStorage, cluster);
        }

        private static boolean isTimeValueOperand(RexCall call) {
            return call.getOperands().size() == 2 && call.getOperands().get(1).getType().getSqlTypeName() == SqlTypeName.TIME;
        }

        private static RexCall castValueOperandToVarchar(RexCall original, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode value = original.getOperands().get(1);
            RelDataType varcharType = rexBuilder.getTypeFactory()
                .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), value.getType().isNullable());
            RexNode castedValue = rexBuilder.makeCast(varcharType, value);
            return (RexCall) rexBuilder.makeCall(
                original.getType(),
                original.getOperator(),
                List.of(original.getOperands().get(0), castedValue)
            );
        }
    }

    static final class DateFormatAdapter extends AbstractNameMappingAdapter {
        DateFormatAdapter() {
            super(LOCAL_DATE_FORMAT_OP, List.of(), List.of());
        }
    }

    static final class TimeFormatAdapter extends AbstractNameMappingAdapter {
        TimeFormatAdapter() {
            super(LOCAL_TIME_FORMAT_OP, List.of(), List.of());
        }
    }

    static final class StrToDateAdapter extends AbstractNameMappingAdapter {
        StrToDateAdapter() {
            super(LOCAL_STR_TO_DATE_OP, List.of(), List.of());
        }
    }

    static final class FromUnixtimeAdapter extends NumericToDoubleAdapter {
        FromUnixtimeAdapter() {
            super(LOCAL_FROM_UNIXTIME_OP);
        }
    }

    static final class MaketimeAdapter extends NumericToDoubleAdapter {
        MaketimeAdapter() {
            super(LOCAL_MAKETIME_OP);
        }
    }

    static final class MakedateAdapter extends NumericToDoubleAdapter {
        MakedateAdapter() {
            super(LOCAL_MAKEDATE_OP);
        }
    }
}

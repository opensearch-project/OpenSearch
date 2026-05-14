/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapters for PPL datetime functions that map 1:1 to a DataFusion builtin; signatures
 * registered in {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS}.
 *
 * @opensearch.internal
 */
final class DateTimeAdapters {

    private DateTimeAdapters() {}

    static final SqlOperator LOCAL_NOW_OP = new SqlFunction(
        "now",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE
    );

    static final SqlOperator LOCAL_CURRENT_DATE_OP = new SqlFunction(
        "current_date",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE,
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE
    );

    static final SqlOperator LOCAL_CURRENT_TIME_OP = new SqlFunction(
        "current_time",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIME,
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE
    );

    static final SqlOperator LOCAL_TIME_OP = new SqlFunction(
        "to_time",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIME_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    static final SqlOperator LOCAL_DATE_OP = new SqlFunction(
        "to_date",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    // 1-arg timestamp(expr) remains on the legacy engine — the TIMESTAMP enum slot is already
    // bound to TimestampFunctionAdapter for VARCHAR-literal folding.
    static final SqlOperator LOCAL_TO_TIMESTAMP_OP = new SqlFunction(
        "to_timestamp",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    /**
     * Rewrites PPL {@code now()} / {@code current_timestamp()} / {@code sysdate()} /
     * {@code utc_timestamp()} to {@code date_format(now(), '%Y-%m-%d %H:%i:%S')}.
     *
     * <p>Rationale: the legacy engine and every PPL IT parse timestamp results with
     * {@code LocalDateTime.parse(value, "uuuu-MM-dd HH:mm:ss")} — a SPACE between date
     * and time. DataFusion's Arrow {@code CAST(Timestamp AS Utf8)} emits ISO-8601 with
     * a 'T' separator, which the PPL result layer's outer {@code CAST($0):VARCHAR}
     * invokes on every TIMESTAMP-returning projection. Folding the rewrite into this
     * adapter short-circuits that path: the inner expression is already VARCHAR in
     * PPL canonical form, so the outer CAST becomes a pass-through.
     *
     * <p>Issue #5420 tracks the underlying ISO-T gap. Fixing it at the adapter is
     * narrower than a RelNode-wide rewrite of every {@code CAST(ts AS varchar)},
     * which would also affect filter subexpressions where ISO-T is the correct
     * DataFusion semantic.
     */
    static final class NowAdapter implements ScalarFunctionAdapter {
        // `%Y-%m-%d %H:%i:%S` (MySQL tokens consumed by the date_format Rust UDF)
        // matches reference PPL's second-precision canonical form; fractional seconds
        // are dropped intentionally because the reference PPL `now()` formatter also
        // yields second precision when nanos==0 and the NowLike IT asserts against a
        // `uuuu-MM-dd HH:mm:ss` parser.
        private static final String PPL_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%i:%S";

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            // 3-arg makeLiteral matches AbstractNameMappingAdapter — 1-arg infers
            // CHAR<N> (fixed-width) which Substrait rejects against date_format's
            // `string` format-arg signature (`Unable to convert call
            // date_format(precision_timestamp<9>?, char<N>)`).
            RexNode format = rexBuilder.makeLiteral(
                PPL_TIMESTAMP_FORMAT,
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                true
            );
            RexNode now = rexBuilder.makeCall(original.getType(), LOCAL_NOW_OP, List.of());
            return rexBuilder.makeCall(original.getType(), RustUdfDateTimeAdapters.LOCAL_DATE_FORMAT_OP, List.of(now, format));
        }
    }

    static final class CurrentDateAdapter extends AbstractNameMappingAdapter {
        CurrentDateAdapter() {
            super(LOCAL_CURRENT_DATE_OP, List.of(), List.of());
        }
    }

    static final class CurrentTimeAdapter extends AbstractNameMappingAdapter {
        CurrentTimeAdapter() {
            super(LOCAL_CURRENT_TIME_OP, List.of(), List.of());
        }
    }

    static final class TimeAdapter extends AbstractNameMappingAdapter {
        TimeAdapter() {
            super(LOCAL_TIME_OP, List.of(), List.of());
        }
    }

    static final class DateAdapter extends AbstractNameMappingAdapter {
        DateAdapter() {
            super(LOCAL_DATE_OP, List.of(), List.of());
        }
    }

    static final class DatetimeAdapter extends AbstractNameMappingAdapter {
        DatetimeAdapter() {
            super(LOCAL_TO_TIMESTAMP_OP, List.of(), List.of());
        }
    }
}

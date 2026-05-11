/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;

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

    static final class NowAdapter extends AbstractNameMappingAdapter {
        NowAdapter() {
            super(LOCAL_NOW_OP, List.of(), List.of());
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

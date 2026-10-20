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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;

import java.util.List;

/**
 * Cat-4 rename adapter for PPL's {@code RANGE_BUCKET(value, data_min,
 * data_max, start_param, end_param)}. Rewrites to a locally-declared
 * {@link SqlFunction} whose
 * {@link io.substrait.isthmus.expression.FunctionMappings.Sig} lives in
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} and resolves
 * to the {@code range_bucket} Rust UDF (see
 * {@code rust/src/udf/range_bucket.rs}).
 *
 * <p><b>Window-dependency note:</b> the PPL frontend's {@code bin start=...
 * end=...} command wraps {@code data_min} / {@code data_max} in
 * {@code MIN(field) OVER ()} / {@code MAX(field) OVER ()} empty-partition
 * window aggregates. The adapter itself is window-agnostic — it handles
 * whatever RexNode the frontend produces for each slot — but end-to-end
 * pushdown via the PPL {@code bin} command requires that the analytics-
 * engine backend advertises the window-over-empty-partition capability.
 * Calling {@code range_bucket(...)} directly via {@code eval} with literal
 * bounds sidesteps the window dependency and does exercise this adapter
 * end-to-end.
 *
 * <p>Pattern-match with Group C's other adapters: pure rename via
 * {@link AbstractNameMappingAdapter}, preserves operand order (including
 * null literals in the optional start_param / end_param slots) and the
 * original call's {@link org.apache.calcite.rel.type.RelDataType}.
 *
 * @opensearch.internal
 */
class RangeBucketAdapter extends AbstractNameMappingAdapter {

    /**
     * Locally-declared target operator. {@link SqlKind#OTHER_FUNCTION} to
     * avoid Calcite built-in collisions. Slots 3 and 4 use
     * {@link SqlTypeFamily#ANY} because PPL may pass a null literal for
     * the optional start/end params (Calcite types null literals as NULL,
     * which isn't in {@link SqlTypeFamily#NUMERIC}).
     */
    static final SqlOperator LOCAL_RANGE_BUCKET_OP = new SqlFunction(
        "range_bucket",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    RangeBucketAdapter() {
        super(LOCAL_RANGE_BUCKET_OP, List.of(), List.of());
    }
}

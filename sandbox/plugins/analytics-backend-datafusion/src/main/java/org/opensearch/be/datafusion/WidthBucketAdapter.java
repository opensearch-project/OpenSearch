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
 * Cat-4 rename adapter for PPL's {@code WIDTH_BUCKET(value, num_bins,
 * data_range, max_value)}. Rewrites to a locally-declared {@link SqlFunction}
 * whose {@link io.substrait.isthmus.expression.FunctionMappings.Sig} lives in
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} and resolves to
 * the {@code width_bucket} Rust UDF (see
 * {@code rust/src/udf/width_bucket.rs} and the YAML signature in
 * {@code extensions/opensearch_scalar.yaml}).
 *
 * <p><b>Name collision:</b> PPL's {@code WIDTH_BUCKET} is NOT the ISO-SQL
 * {@code WIDTH_BUCKET(value, min, max, count) → INT}; it's a bespoke
 * OpenSearch-SQL UDF that returns a VARCHAR bucket label like {@code "0-100"}
 * via a nice-number magnitude-based algorithm (see
 * {@code sql/core/.../WidthBucketFunction.java}). A future implementation of
 * real ISO-SQL width_bucket (DataFusion has it natively) must use a distinct
 * enum entry / sig name to avoid dispatch ambiguity.
 *
 * <p>Pattern match with {@link SpanBucketAdapter}: pure rename via
 * {@link AbstractNameMappingAdapter}, no literal injection, preserves
 * operand order and the original call's {@link org.apache.calcite.rel.type.RelDataType}.
 *
 * @opensearch.internal
 */
class WidthBucketAdapter extends AbstractNameMappingAdapter {

    /**
     * Locally-declared target operator for the rewrite. {@link SqlKind#OTHER_FUNCTION}
     * to avoid colliding with any Calcite built-in (notably Calcite's own
     * {@code WIDTH_BUCKET} constant, which is the ISO-SQL variant — wrong
     * semantics). Operand-type checking is permissive (4 numerics); real
     * argument vetting happens in the UDF's {@code coerce_types} and
     * {@code invoke_with_args}.
     */
    static final SqlOperator LOCAL_WIDTH_BUCKET_OP = new SqlFunction(
        "width_bucket",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    WidthBucketAdapter() {
        super(LOCAL_WIDTH_BUCKET_OP, List.of(), List.of());
    }
}

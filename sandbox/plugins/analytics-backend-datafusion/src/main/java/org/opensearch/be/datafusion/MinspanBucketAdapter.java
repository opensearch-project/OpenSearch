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
 * Cat-4 rename adapter for PPL's {@code MINSPAN_BUCKET(value, min_span,
 * data_range, max_value)}. Rewrites to a locally-declared {@link SqlFunction}
 * whose {@link io.substrait.isthmus.expression.FunctionMappings.Sig} lives in
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} and resolves to
 * the {@code minspan_bucket} Rust UDF (see
 * {@code rust/src/udf/minspan_bucket.rs} and the YAML signature in
 * {@code extensions/opensearch_scalar.yaml}).
 *
 * <p>Same adapter-pattern as {@link SpanBucketAdapter} and
 * {@link WidthBucketAdapter}: pure rename, preserves operand order and the
 * original call's {@link org.apache.calcite.rel.type.RelDataType}.
 *
 * <p>PPL's {@code MINSPAN_BUCKET} takes a 4-tuple {@code (value, min_span,
 * data_range, max_value)} where {@code max_value} is carried through for
 * call-shape parity but unused by the algorithm (explicit no-op in the Java
 * reference — see {@code MinspanBucketFunction}'s Javadoc).
 *
 * @opensearch.internal
 */
class MinspanBucketAdapter extends AbstractNameMappingAdapter {

    /**
     * Locally-declared target operator for the rewrite.
     * {@link SqlKind#OTHER_FUNCTION} prevents collision with Calcite built-ins.
     * Permissive 4-numeric operand checker; real argument vetting in the
     * UDF's {@code coerce_types} and {@code invoke_with_args}.
     */
    static final SqlOperator LOCAL_MINSPAN_BUCKET_OP = new SqlFunction(
        "minspan_bucket",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    MinspanBucketAdapter() {
        super(LOCAL_MINSPAN_BUCKET_OP, List.of(), List.of());
    }
}

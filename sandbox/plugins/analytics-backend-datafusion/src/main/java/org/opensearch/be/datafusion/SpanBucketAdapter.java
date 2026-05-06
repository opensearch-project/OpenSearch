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
 * Cat-4 rename adapter for PPL's {@code SPAN_BUCKET(value, span)}. Rewrites to
 * a locally-declared {@link SqlFunction} whose
 * {@link io.substrait.isthmus.expression.FunctionMappings.Sig} lives in
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} and resolves to
 * the {@code span_bucket} Rust UDF (see {@code rust/src/udf/span_bucket.rs}
 * and the YAML signature in {@code extensions/opensearch_scalar.yaml}).
 *
 * <p><b>Why a locally-declared operator?</b> PPL's {@code SPAN_BUCKET} is a
 * bespoke {@code SqlUserDefinedFunction} defined in {@code sql/core}'s
 * {@code PPLBuiltinOperators}. Isthmus's {@code ScalarFunctionConverter} binds
 * Sigs by operator <em>instance</em>, so the Sig's target must be an operator
 * we can import. Adding a {@code sql/core} dependency on this module is
 * undesirable; instead we declare a minimal {@link SqlFunction} here whose
 * sole purpose is to be the referent of our Sig. Same pattern as
 * {@link ConvertTzAdapter}.
 *
 * <p>Note on naming: this is PPL's {@code SPAN_BUCKET} (returns a VARCHAR
 * bucket label like {@code "10-20"} via the OpenSearch nice-number algorithm),
 * not ISO SQL {@code WIDTH_BUCKET} (returns an integer bucket index).
 *
 * @opensearch.internal
 */
class SpanBucketAdapter extends AbstractNameMappingAdapter {

    /**
     * Locally-declared target operator for the rewrite. Uses
     * {@link SqlKind#OTHER_FUNCTION} so it cannot collide with any Calcite
     * built-in. Operand-type checking is permissive ({@link OperandTypes#NUMERIC_NUMERIC});
     * real argument vetting happens inside the UDF's {@code coerce_types}
     * and {@code invoke_with_args}.
     *
     * <p>Return type inference is a placeholder; the adapter clones with
     * {@code original.getType()} via {@link AbstractNameMappingAdapter}, so
     * the adapted call's reported type equals PPL's
     * {@code VARCHAR(2000) FORCE_NULLABLE}.
     */
    static final SqlOperator LOCAL_SPAN_BUCKET_OP = new SqlFunction(
        "span_bucket",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.NUMERIC_NUMERIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    SpanBucketAdapter() {
        super(LOCAL_SPAN_BUCKET_OP, List.of(), List.of());
    }
}

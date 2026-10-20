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
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rename adapter for PPL's {@code mvzip(left, right [, sep])} — rewrites the
 * Calcite UDF call (PPL's {@code MVZipFunctionImpl} registered under the
 * function name {@code "mvzip"}) to a locally-declared {@link SqlFunction}
 * also named {@code mvzip}. The locally-declared op is the referent of the
 * {@link io.substrait.isthmus.expression.FunctionMappings.Sig} entry in
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS}, so isthmus
 * emits a Substrait scalar function call with that exact name. The
 * analytics-backend-datafusion plugin's Rust crate (`udf::mvzip`) registers a
 * matching ScalarUDF on the DataFusion session context with the same name,
 * which the substrait consumer resolves natively.
 *
 * <p>The PPL UDF's Calcite-side return type is already
 * {@code ARRAY&lt;VARCHAR&gt;} (set by {@code MVZipFunctionImpl.getReturnTypeInference}),
 * matching the {@code list&lt;string?&gt;} declared in
 * {@code opensearch_array_functions.yaml}. No operand widening is needed —
 * mvzip accepts any pair of array element types and emits strings.
 *
 * @opensearch.internal
 */
class MvzipAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator. Name matches the Rust UDF
     * {@code MvzipUdf::name()}. Return-type inference here is a placeholder —
     * the call's original return type ({@code ARRAY&lt;VARCHAR&gt;}) is carried
     * forward explicitly in {@link #adapt}.
     */
    static final SqlOperator LOCAL_MVZIP_OP = new SqlFunction(
        "mvzip",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        return rexBuilder.makeCall(original.getType(), LOCAL_MVZIP_OP, original.getOperands());
    }
}

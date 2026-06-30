/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rewrites a one-arg scalar UDF call to use a target Calcite {@link SqlOperator}.
 *
 * <p>Used for PPL hyperbolic functions ({@code SINH}, {@code COSH}): PPL emits
 * them as {@link org.apache.calcite.sql.validate.SqlUserDefinedFunction} UDFs,
 * but isthmus's {@code FunctionMappings.SCALAR_SIGS} only maps the variants in
 * {@link org.apache.calcite.sql.fun.SqlLibraryOperators} to their Substrait
 * canonical names ({@code sinh}, {@code cosh}). This adapter swaps the operator
 * reference while preserving the operand so the subsequent Substrait visitor
 * produces the standard function call DataFusion's substrait consumer evaluates
 * natively.
 *
 * <p>Input shape: {@code UDF(arg)}. Output shape: {@code targetOperator(arg)}.
 * Preserves the Calcite row type of the call.
 *
 * @opensearch.internal
 */
class HyperbolicOperatorAdapter implements ScalarFunctionAdapter {

    private final SqlOperator targetOperator;

    HyperbolicOperatorAdapter(SqlOperator targetOperator) {
        this.targetOperator = targetOperator;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        // Idempotency — if the plan already carries the target operator, leave it.
        if (original.getOperator() == targetOperator) {
            return original;
        }
        // Defensive: the adapter is only registered against the ScalarFunction whose
        // name matches the target operator, so any other call shape is a programming
        // error upstream. Rather than silently rewriting (which would corrupt unrelated
        // math functions like ABS if the adapter were mis-registered), only rewrite
        // when the operator name matches.
        if (!original.getOperator().getName().equalsIgnoreCase(targetOperator.getName())) {
            return original;
        }
        if (original.getOperands().size() != 1) {
            return original;
        }
        // Swap the operator but keep the operand and the Calcite-inferred type.
        return cluster.getRexBuilder().makeCall(original.getType(), targetOperator, original.getOperands());
    }
}

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
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrites the zero-arg PPL {@code E()} UDF call to a {@code DOUBLE} literal
 * equal to {@link Math#E}. DataFusion's substrait consumer has no {@code e}
 * scalar function, but constant-folding the call on the coordinator side
 * before Substrait serialisation produces a literal expression the downstream
 * plan handles trivially.
 *
 * @opensearch.internal
 */
class EConstantAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        // Only rewrite the zero-arg E() UDF. Defensive guard against accidental
        // registration — any call with operands, or one whose operator isn't named
        // "E", passes through unchanged so it can be surfaced as a planner error
        // further down the pipeline instead of being silently dropped.
        if (!original.getOperator().getName().equalsIgnoreCase("E")) {
            return original;
        }
        if (!original.getOperands().isEmpty()) {
            return original;
        }
        return cluster.getRexBuilder().makeApproxLiteral(BigDecimal.valueOf(Math.E));
    }
}

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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapter for PPL {@code RAND([N])}. Niladic {@code rand()} maps to {@code SqlStdOperatorTable.RAND},
 * whose {@code FunctionMappings.Sig} in {@link DataFusionFragmentConvertor} resolves to DataFusion's
 * niladic {@code random()} UDF.
 *
 * <p><b>Seeded form is rejected, not silently degraded.</b> PPL's reference {@code rand(N)} seeds the
 * generator so identical {@code N} yields identical values — it is observably <em>deterministic</em>
 * (see {@code MathematicalFunctions.rand}, which uses {@code new Random(N).nextFloat()}). DataFusion's
 * {@code random()} is niladic and unseeded, so there is no faithful mapping. Dropping the seed would
 * turn a deterministic call into a non-deterministic one — a silent semantic change. Until a seeded
 * {@code random} is available on the DataFusion side, {@code rand(seed)} fails fast here with a clear,
 * actionable error rather than returning subtly wrong (non-reproducible) results.
 *
 * @opensearch.internal
 */
class RandSeedAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.isEmpty()) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            return rexBuilder.makeCall(original.getType(), SqlStdOperatorTable.RAND, List.of());
        }
        if (operands.size() == 1) {
            // Seeded rand(N): no faithful niladic mapping (would lose determinism). Fail clearly.
            throw new IllegalArgumentException(
                "Seeded RAND(seed) is not supported on the analytics-engine route yet; use RAND() without a seed"
            );
        }
        // Unexpected arity — leave the call untouched so the converter surfaces the original shape
        // rather than this adapter inventing a valid one.
        return original;
    }
}

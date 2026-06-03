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
 * Adapter for PPL {@code RAND([N])}. Maps to {@code SqlStdOperatorTable.RAND}, whose
 * {@code FunctionMappings.Sig} in {@link DataFusionFragmentConvertor} resolves to DataFusion's
 * niladic {@code random()} UDF — dropping the optional integer seed operand.
 *
 * <p><b>Why drop the seed.</b> DataFusion's {@code random()} takes no arguments, so a seeded
 * {@code RAND(N)} call (e.g. {@code rand(5)}) has no Substrait mapping and fails fragment
 * conversion with {@code IllegalArgumentException: Unable to convert call RAND(i32)}.
 *
 * <p><b>Semantics note.</b> PPL's reference {@code rand(N)} seeds the generator so identical
 * {@code N} yields identical values (deterministic). Dropping the seed loses that determinism —
 * {@code rand(5)} becomes equivalent to {@code rand()}. This is acceptable for the current test
 * surface: the only IT exercising {@code rand(5)}
 * ({@code MathematicalFunctionIT.testRand}) asserts only the result <em>schema</em> (a DOUBLE
 * column), not the value. If strict seeded reproducibility is required later, this must instead
 * route to a seeded {@code random} variant on the DataFusion side rather than dropping the seed.
 *
 * @opensearch.internal
 */
class RandSeedAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        // Emit the niladic RAND form regardless of whether a seed operand was supplied; the
        // SqlStdOperatorTable.RAND -> "random" sig handles the conversion to DataFusion.
        return rexBuilder.makeCall(original.getType(), SqlStdOperatorTable.RAND, List.of());
    }
}

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
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapter for PPL {@code NOW([fsp])} (and its {@code CURRENT_TIMESTAMP} / {@code LOCALTIMESTAMP}
 * synonyms). Maps to DataFusion's niladic {@code now()} via {@link DateTimeAdapters#LOCAL_NOW_OP},
 * dropping the optional fractional-seconds-precision argument.
 *
 * <p><b>Why drop the {@code fsp} arg.</b> DataFusion's {@code now()} takes no arguments, so a
 * {@code now(i32)} call has no Substrait mapping and fails fragment conversion with
 * {@code IllegalArgumentException: Unable to convert call now(i32)}. The {@code fsp} argument is
 * intentionally ignored to match the SQL-plugin reference, which removed {@code fsp} support
 * specifically "to avoid bug where {@code now()}, {@code now(x)} and {@code now(y)} return
 * different values" (see {@code DateTimeFunctions.now}). So {@code now(3)} ≡ {@code now()} —
 * dropping the precision operand is the correct, reference-matching semantics.
 *
 * <p>The 0-arg {@code now()} form already routes correctly through
 * {@link DateTimeAdapters.NowAdapter}; this adapter only differs by stripping the operand,
 * so it subsumes that case (a no-op when there are no operands).
 *
 * @opensearch.internal
 */
class NowFspAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> operands = original.getOperands();
        if (operands.isEmpty()) {
            // No fsp arg — plain now(). Still route to the substrait-mapped local operator.
            return rexBuilder.makeCall(original.getType(), DateTimeAdapters.LOCAL_NOW_OP, List.of());
        }
        // Drop the fsp/precision operand(s); DataFusion now() is niladic and the value is
        // intentionally ignored per the reference implementation.
        return rexBuilder.makeCall(original.getType(), DateTimeAdapters.LOCAL_NOW_OP, List.of());
    }
}

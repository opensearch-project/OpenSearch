/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.WindowFunctionAdapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * DataFusion-side {@link WindowFunctionAdapter}s rewriting PPL-form window aggregates:
 * <ul>
 *   <li>{@link #argMin()} / {@link #argMax()} — {@code ARG_MIN/MAX(value, ts)} →
 *       {@code FIRST_VALUE/LAST_VALUE(value) ORDER BY ts ASC} (no native arg_min/max UDAF in DataFusion 53.x).</li>
 *   <li>{@link #distinctCountApprox()} — PPL {@code DISTINCT_COUNT_APPROX(x)} → Calcite
 *       {@code APPROX_COUNT_DISTINCT(x)}, which the convertor renames to substrait {@code approx_distinct}
 *       (DataFusion's built-in HLL UDAF).</li>
 *   <li>{@link #countDistinctExact()} — {@code COUNT(DISTINCT x) OVER(...)} → sandbox
 *       {@code os_count_distinct(x)}. DataFusion 54.x's substrait window consumer drops the
 *       {@code AggregationInvocation::DISTINCT} bit, so a vanilla COUNT-with-distinct lowers to
 *       a plain non-distinct COUNT. Encoding DISTINCT in the operator name sidesteps the bug.</li>
 * </ul>
 *
 * @opensearch.internal
 */
final class WindowFunctionAdapters {

    private WindowFunctionAdapters() {}

    static WindowFunctionAdapter argMin() {
        return new ArgFunctionAdapter(SqlStdOperatorTable.FIRST_VALUE);
    }

    static WindowFunctionAdapter argMax() {
        return new ArgFunctionAdapter(SqlStdOperatorTable.LAST_VALUE);
    }

    static WindowFunctionAdapter distinctCountApprox() {
        return (over, operands, partitions, orderKeys, cluster) -> rebuild(
            over,
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            operands,
            partitions,
            orderKeys,
            over.isDistinct(),
            cluster
        );
    }

    /**
     * Replaces {@code COUNT(DISTINCT x) OVER(...)} with {@code os_count_distinct(x) OVER(...)}.
     * Non-distinct {@code COUNT(x) OVER(...)} passes through unchanged. The distinct flag is
     * cleared on the rebuilt RexOver because it's already encoded in the operator name —
     * leaving it set would emit a substrait operator named {@code os_count_distinct} carrying a
     * redundant {@code AggregationInvocation::DISTINCT} bit.
     */
    static WindowFunctionAdapter countDistinctExact() {
        return (over, operands, partitions, orderKeys, cluster) -> {
            if (!over.isDistinct()) {
                return rebuild(over, over.getAggOperator(), operands, partitions, orderKeys, /* distinct */ false, cluster);
            }
            return rebuild(
                over,
                DataFusionFragmentConvertor.LOCAL_OS_COUNT_DISTINCT_OP,
                operands,
                partitions,
                orderKeys,
                /* distinct */ false,
                cluster
            );
        };
    }

    /** Rewrites {@code ARG_MIN(value, ts)} / {@code ARG_MAX(value, ts)} to
     *  {@code FIRST_VALUE(value)} / {@code LAST_VALUE(value)} with {@code ts} appended as an
     *  ORDER BY key. Falls back to a passthrough rebuild when the operand shape is unexpected. */
    private record ArgFunctionAdapter(SqlAggFunction target) implements WindowFunctionAdapter {
        @Override
        public RexNode adapt(
            RexOver original,
            List<RexNode> operands,
            List<RexNode> partitions,
            List<RexFieldCollation> orderKeys,
            RelOptCluster cluster
        ) {
            if (operands.size() != 2) {
                // Unexpected shape — preserve original behavior as best as possible.
                return rebuild(original, original.getAggOperator(), operands, partitions, orderKeys, original.isDistinct(), cluster);
            }
            return rebuild(
                original,
                target,
                List.of(operands.get(0)),
                partitions,
                appendAsc(orderKeys, operands.get(1)),
                original.isDistinct(),
                cluster
            );
        }
    }

    /** Builds a fresh window {@link RexNode} preserving frame bounds, exclude, rows-vs-range,
     *  and ignore-nulls flags from {@code original}. */
    private static RexNode rebuild(
        RexOver original,
        SqlAggFunction operator,
        List<RexNode> operands,
        List<RexNode> partitions,
        List<RexFieldCollation> orderKeys,
        boolean distinct,
        RelOptCluster cluster
    ) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexWindow window = original.getWindow();
        return rexBuilder.makeOver(
            original.getType(),
            operator,
            operands,
            partitions,
            ImmutableList.copyOf(orderKeys),
            window.getLowerBound(),
            window.getUpperBound(),
            window.getExclude(),
            window.isRows(),
            true,
            false,
            distinct,
            original.ignoreNulls()
        );
    }

    /** Returns a new order-key list with {@code key ASC NULLS_LAST} appended. Empty SqlKind set
     *  is the Calcite encoding of "default direction" — RexFieldCollation.getDirection() reads it
     *  back as {@link org.apache.calcite.rel.RelFieldCollation.Direction#ASCENDING}. */
    private static List<RexFieldCollation> appendAsc(List<RexFieldCollation> existing, RexNode key) {
        List<RexFieldCollation> out = new ArrayList<>(existing.size() + 1);
        out.addAll(existing);
        out.add(new RexFieldCollation(key, Collections.emptySet()));
        return out;
    }
}

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
 * DataFusion-side {@link WindowFunctionAdapter} implementations that rewrite PPL-form
 * window aggregates into the shapes DataFusion's substrait consumer understands.
 *
 * <ul>
 *   <li>{@link #argMin()} — {@code ARG_MIN(value, ts)} → {@code FIRST_VALUE(value) ORDER BY ts ASC}.
 *       DataFusion 53.x has no built-in {@code arg_min} UDAF; under PPL's default
 *       UNBOUNDED PRECEDING / UNBOUNDED FOLLOWING frame, FIRST_VALUE with ORDER BY ts is exactly
 *       equivalent.</li>
 *   <li>{@link #argMax()} — {@code ARG_MAX(value, ts)} → {@code LAST_VALUE(value) ORDER BY ts ASC}.
 *       Same reasoning; LAST_VALUE on the full partition under the unbounded frame picks the
 *       row whose ORDER BY key is largest.</li>
 *   <li>{@link #distinctCountApprox()} — {@code DISTINCT_COUNT_APPROX(x)}
 *       → {@code APPROX_COUNT_DISTINCT(x)}.
 *       sql-plugin's PPL maps {@code dc()}/{@code distinct_count()}/{@code distinct_count_approx()}
 *       all to a UDAF named {@code DISTINCT_COUNT_APPROX} (HyperLogLog++ via OpenSearch cardinality
 *       on the V3 path, i.e. always approximate). DataFusion exposes the same family of HLL
 *       semantics under {@code APPROX_COUNT_DISTINCT}; this adapter rebinds the operator so
 *       isthmus emits the substrait function name DataFusion's substrait consumer recognizes
 *       (after the wrapper UDAF in {@code rust/src/udaf/approx_count_distinct.rs} aliases it
 *       to DataFusion's built-in {@code approx_distinct}).
 *
 *       <p>We do not rewrite to {@code COUNT(DISTINCT x)} because DataFusion's substrait
 *       consumer drops the {@code AggregationInvocation.DISTINCT} flag on window functions
 *       (see {@code datafusion-substrait/.../window_function.rs} hard-coding
 *       {@code distinct: false}).</li>
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

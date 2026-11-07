/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pre-marking rule: hoists a constant out of an aggregate's argument so the arithmetic runs once on the
 * aggregated value instead of once per row, and every term over the same column shares one set of
 * accumulators. For an integer column {@code x} and an integer literal {@code k}:
 *
 * <pre>
 *   SUM(x ± k)   → SUM(x) ± k * COUNT(x)
 *   COUNT(x ± k) → COUNT(x)
 *   MIN(x ± k)   → MIN(x) ± k          MAX(x ± k) → MAX(x) ± k
 *
 *   stats sum(x), sum(x + 1), sum(x + 2), min(x + 3), count(x + 4)
 *
 *   Aggregate(SUM($0), SUM($1), SUM($2), MIN($3), COUNT($4))     ← 5 accumulators
 *     Project(x, x + 1, x + 2, x + 3, x + 4)                       ← 4 additions per row
 *
 *   Project($0, $0 + $1, $0 + 2 * $1, $2 + 3, $1)                  ← arithmetic once, on aggregated values
 *     Aggregate(SUM($0), COUNT($0), MIN($0))                       ← 3 accumulators
 *       Project(x)                                                 ← 1 column read
 * </pre>
 *
 * <p>Scope: the one Project directly under the Aggregate. Nothing here rejects a Join, Union, Sort or Filter
 * below that Project; {@code Aggregate(Project(Join))} with the arithmetic in that Project is rewritten. When an
 * earlier {@code eval} computed {@code x ± k} below such an operator, the adjacent Project holds a bare input
 * reference, so there is nothing to hoist and the rule declines (pinned by the {@code testShiftBelow*} tests).
 *
 * <p>Runs in {@code aggregate-decompose}, before marking and before the PARTIAL / FINAL split, so both halves
 * of a distributed aggregate see the reduced call list. It cannot be left to a backend: by the time a
 * fragment is handed over, the argument is an opaque projected column, not an expression. Shares a rule
 * collection with the AVG / STDDEV reduce rule so {@code AVG(x ± k)}, reduced to {@code SUM(x ± k) /
 * COUNT(x ± k)}, collapses in the same fixpoint loop.
 *
 * <p>{@code COUNT(x)}, not {@code COUNT(*)}, keeps the identities exact with NULLs and empty groups.
 * Integer-only: float addition is not associative. {@code x} and {@code CAST(x):BIGINT} count as the same
 * column (the widest variant is aggregated, results are cast back), which is what lets {@code sum(x)} and
 * {@code sum(x + 1)} share an accumulator. Output names and types are preserved. Fires only when at least one
 * call is a supported function over {@code x ± k} with a non-zero integer {@code k}; DISTINCT, FILTER,
 * approximate and multi-argument calls never take part.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateConstantShiftRule extends RelOptRule {

    // Aggregates for which fn(x ± k) can be computed from fn(x) (plus COUNT(x) for SUM).
    private static final Set<SqlAggFunction> SUPPORTED_FUNCTIONS = Set.of(
        SqlStdOperatorTable.SUM,
        SqlStdOperatorTable.COUNT,
        SqlStdOperatorTable.MIN,
        SqlStdOperatorTable.MAX
    );

    public OpenSearchAggregateConstantShiftRule() {
        super(operand(LogicalAggregate.class, operand(LogicalProject.class, any())), "OpenSearchAggregateConstantShiftRule");
    }

    /**
     * Cheap structural precheck. Kept separate from {@link #onMatch} on purpose: Calcite records a rule attempt
     * (and {@code RuleProfilingListener} reports it) only when this returns true, so aggregates with nothing
     * to hoist must be declined here, not inside {@code onMatch}.
     */
    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalAggregate aggregate = call.rel(0);
        LogicalProject project = call.rel(1);

        // Grouping sets would need one groupKey per set; PPL never produces them.
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            return false;
        }
        // rexList = pre-operands, e.g. LITERAL_AGG(1): AggregateCall.equals ignores them (LITERAL_AGG(1) equals LITERAL_AGG(2)),
        // so the dedup below would be at the mercy of hash collisions. Declined and pinned by testLiteralAggSiblingIsNotRewritten.
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            if (!aggCall.rexList.isEmpty()) {
                return false;
            }
        }
        // At least one supported call over x ± k with k != 0. Also what stops the rule from re-firing on its
        // own output, whose calls are all shift 0.
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            AggregateTerm term = AggregateTerm.of(aggCall, project);
            if (term != null && term.isShifted()) {
                return true;
            }
        }
        return false;
    }

    /** {@code Project(recomposed outputs) / Aggregate(shared accumulators) / Project(original columns + aggregated columns)}. */
    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggregate = call.rel(0);
        LogicalProject project = call.rel(1);
        RelBuilder relBuilder = call.builder();

        AggregateTerm[] terms = termsOf(aggregate, project);
        Map<String, Integer> slotByColumnDigest = pushProjectWithSharedColumns(relBuilder, project, sharedColumns(terms));
        SharedAccumulators accumulators = new SharedAccumulators(
            aggregate.getCluster().getRexBuilder(),
            relBuilder.peek(),
            aggregate.getGroupCount()
        );
        List<RexNode> outputs = rebuildOutputExprs(aggregate, terms, slotByColumnDigest, accumulators);

        call.transformTo(
            relBuilder.aggregate(relBuilder.groupKey(aggregate.getGroupSet()), accumulators.calls())
                .project(outputs, aggregate.getRowType().getFieldNames(), true)
                .build()
        );
    }

    // ---- Rewrite steps ----

    /** One term per call, in call order; {@code null} where a call does not take part (kept as-is, remapped by RelBuilder). */
    private static AggregateTerm[] termsOf(LogicalAggregate aggregate, LogicalProject project) {
        List<AggregateCall> calls = aggregate.getAggCallList();
        AggregateTerm[] terms = new AggregateTerm[calls.size()];
        for (int i = 0; i < calls.size(); i++) {
            terms[i] = AggregateTerm.of(calls.get(i), project);
        }
        return terms;
    }

    /**
     * {@code x}, {@code CAST(x):BIGINT} and {@code CAST(x):INTEGER} are one column; keep the variant the others widen into.
     * Insertion-ordered so the added columns land in the order the query first mentions them.
     */
    private static Map<String, RexNode> sharedColumns(AggregateTerm[] terms) {
        Map<String, RexNode> columnByDigest = new LinkedHashMap<>();
        for (AggregateTerm term : terms) {
            if (term != null) {
                columnByDigest.merge(columnDigest(term.column()), term.column(), OpenSearchAggregateConstantShiftRule::widerOf);
            }
        }
        return columnByDigest;
    }

    /**
     * Pushes the rewritten input Project (the original columns plus one per aggregated column) onto the builder — the caller reads
     * it back with {@link RelBuilder#peek()} — and returns the slot each aggregated column landed in, keyed by {@link #columnDigest}.
     * {@link RelBuilder#aggregate} later prunes the {@code x ± k} columns nobody reads any more, so this does not have to.
     */
    private static Map<String, Integer> pushProjectWithSharedColumns(
        RelBuilder relBuilder,
        LogicalProject project,
        Map<String, RexNode> columnByDigest
    ) {
        List<RexNode> exprs = new ArrayList<>(project.getProjects());
        List<String> names = new ArrayList<>(project.getRowType().getFieldNames());
        Map<String, Integer> slotByColumnDigest = new HashMap<>();
        for (Map.Entry<String, RexNode> column : columnByDigest.entrySet()) {
            int slot = exprs.indexOf(column.getValue());
            if (slot < 0) {
                slot = exprs.size();
                exprs.add(column.getValue());
                names.add(null);
            }
            slotByColumnDigest.put(column.getKey(), slot);
        }
        relBuilder.push(project.getInput()).project(exprs, names, true);
        return slotByColumnDigest;
    }

    /**
     * One expression per original output column: group keys pass through; aggregates are recomposed from the
     * shared accumulators or, when they do not take part, re-registered unchanged. Output types are restored either way.
     */
    private static List<RexNode> rebuildOutputExprs(
        LogicalAggregate aggregate,
        AggregateTerm[] terms,
        Map<String, Integer> slotByColumnDigest,
        SharedAccumulators accumulators
    ) {
        int groupCount = aggregate.getGroupCount();
        List<RexNode> outputs = new ArrayList<>();
        for (int key = 0; key < groupCount; key++) {
            outputs.add(accumulators.rexBuilder().makeInputRef(outputType(aggregate, key), key));
        }
        List<AggregateCall> originals = aggregate.getAggCallList();
        for (int i = 0; i < originals.size(); i++) {
            AggregateTerm term = terms[i];
            RexNode value = term == null
                ? accumulators.share(originals.get(i))
                : unshift(term, slotByColumnDigest.get(columnDigest(term.column())), accumulators);
            outputs.add(castTo(outputType(aggregate, groupCount + i), value, accumulators.rexBuilder()));
        }
        return outputs;
    }

    /**
     * {@code fn(column ± k)} from the shared {@code fn(column)}: SUM shifts by {@code k} per counted row, MIN / MAX by
     * {@code k} once, COUNT is shift-invariant. An unshifted term is the shared accumulator itself.
     */
    private static RexNode unshift(AggregateTerm term, int slot, SharedAccumulators accumulators) {
        RexNode aggregated = accumulators.accumulator(term.function(), slot);
        if (!term.isShifted() || term.function() == SqlStdOperatorTable.COUNT) {
            return aggregated;
        }
        BigDecimal magnitude = term.offset().abs();
        RexNode correction = term.function() == SqlStdOperatorTable.SUM
            ? sumCorrection(magnitude, slot, accumulators)
            : accumulators.rexBuilder().makeExactLiteral(magnitude);
        return accumulators.rexBuilder()
            .makeCall(term.offset().signum() > 0 ? SqlStdOperatorTable.PLUS : SqlStdOperatorTable.MINUS, aggregated, correction);
    }

    /** {@code magnitude * COUNT(column)}, or just {@code COUNT(column)} when {@code magnitude == 1}. */
    private static RexNode sumCorrection(BigDecimal magnitude, int slot, SharedAccumulators accumulators) {
        RexNode count = accumulators.accumulator(SqlStdOperatorTable.COUNT, slot);
        return magnitude.compareTo(BigDecimal.ONE) == 0
            ? count
            : accumulators.rexBuilder()
                .makeCall(SqlStdOperatorTable.MULTIPLY, accumulators.rexBuilder().makeExactLiteral(magnitude), count);
    }

    /** The recomposition may widen (BIGINT sum over a SMALLINT column); restore the declared output type. */
    private static RexNode castTo(RelDataType type, RexNode value, RexBuilder rexBuilder) {
        return value.getType().equals(type) ? value : rexBuilder.makeCast(type, value, true);
    }

    private static RelDataType outputType(LogicalAggregate aggregate, int ordinal) {
        return aggregate.getRowType().getFieldList().get(ordinal).getType();
    }

    /**
     * The rewritten aggregate's call list, deduplicated: {@link RexBuilder#addAggCall} adds a call once and hands back
     * a reference to its output column, so N terms over one column cost one accumulator per function.
     */
    private static final class SharedAccumulators {
        private final RexBuilder rexBuilder;
        private final RelNode input;
        private final int groupCount;
        private final List<AggregateCall> calls = new ArrayList<>();
        private final Map<AggregateCall, RexNode> outputRefs = new HashMap<>();

        SharedAccumulators(RexBuilder rexBuilder, RelNode input, int groupCount) {
            this.rexBuilder = rexBuilder;
            this.input = input;
            this.groupCount = groupCount;
        }

        /** Adds {@code call} unless an identical one is already present; returns the reference to its output column. */
        RexNode share(AggregateCall call) {
            return rexBuilder.addAggCall(call, groupCount, calls, outputRefs, input::fieldIsNullable);
        }

        /** The shared plain {@code fn(input column slot)}: no DISTINCT / FILTER / collation, type inferred from {@code input}. */
        RexNode accumulator(SqlAggFunction fn, int slot) {
            return share(
                AggregateCall.create(
                    fn,
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(slot),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    groupCount,
                    input,
                    null,
                    null
                )
            );
        }

        List<AggregateCall> calls() {
            return calls;
        }

        RexBuilder rexBuilder() {
            return rexBuilder;
        }
    }

    // ---- Call classification ----

    /**
     * One aggregate call that takes part in the rewrite, seen as {@code function(column ± offset)}: an integer
     * expression moved by an integer literal. A plain {@code function(column)} is a term with {@code offset == 0}
     * ({@link #isShifted()} is false) so it can share the column's accumulators with its shifted siblings.
     */
    private record AggregateTerm(SqlAggFunction function, RexNode column, BigDecimal offset) {

        /** Whether the argument really is {@code column ± k} with {@code k != 0}; false for a plain {@code function(column)}. */
        boolean isShifted() {
            return offset.signum() != 0;
        }

        /** Recognizes {@code fn(x + k)}, {@code fn(k + x)}, {@code fn(x - k)} and plain integer {@code fn(x)}; null otherwise. */
        static AggregateTerm of(AggregateCall call, LogicalProject project) {
            if (!SUPPORTED_FUNCTIONS.contains(call.getAggregation())
                || call.isDistinct()
                || call.distinctKeys != null
                || call.isApproximate()
                || call.hasFilter()
                || call.getArgList().size() != 1) {
                return null;
            }
            SqlAggFunction fn = call.getAggregation();
            RexNode arg = project.getProjects().get(call.getArgList().get(0));
            // Columns are unified by expression text, which is only sound for deterministic expressions: two
            // independent RAND() calls must stay two draws, not one shared column.
            if (!RexUtil.isDeterministic(arg)) {
                return null;
            }
            if (!(arg instanceof RexCall arithmetic)
                || arithmetic.getOperands().size() != 2
                || (arithmetic.getKind() != SqlKind.PLUS && arithmetic.getKind() != SqlKind.MINUS)) {
                // Not an arithmetic argument: a plain integer column takes part with offset 0.
                return isIntegerType(arg.getType()) ? new AggregateTerm(fn, arg, BigDecimal.ZERO) : null;
            }
            RexNode left = arithmetic.getOperands().get(0);
            RexNode right = arithmetic.getOperands().get(1);
            boolean minus = arithmetic.getKind() == SqlKind.MINUS;
            AggregateTerm term = null;
            if (isIntegerLiteral(right) && isIntegerType(left.getType())) {
                BigDecimal k = literalValue(right);
                term = new AggregateTerm(fn, left, minus ? k.negate() : k);
            } else if (!minus && isIntegerLiteral(left) && isIntegerType(right.getType())) {
                term = new AggregateTerm(fn, right, literalValue(left));
            }
            // k - x (needs -x, a scale), x + y (no literal), x + 1.5 (not integer): leave as written.
            if (term == null) {
                return null;
            }
            // Per-row x ± k wraps silently on overflow. SUM and COUNT survive that (wrapping addition commutes with
            // the identity), MIN / MAX do not: the order of wrapped values is not the order of the originals. Hoist
            // MIN / MAX only when no row can overflow, which the column type's range guarantees for small enough k.
            if ((fn == SqlStdOperatorTable.MIN || fn == SqlStdOperatorTable.MAX)
                && !shiftCannotOverflowLong(term.column(), term.offset())) {
                return null;
            }
            return term;
        }
    }

    // ---- Column unification ----

    /** {@code node} with lossless casts stripped, so {@code CAST(x):BIGINT} and {@code x} become the same expression. */
    private static RexNode stripLosslessCasts(RexNode node) {
        while (node.getKind() == SqlKind.CAST && RexUtil.isLosslessCast(node)) {
            node = ((RexCall) node).getOperands().get(0);
        }
        return node;
    }

    /** Identity of a column with lossless casts stripped, so {@code x} and {@code CAST(x):BIGINT} map to one key. */
    private static String columnDigest(RexNode column) {
        return stripLosslessCasts(column).toString();
    }

    /** Of two variants of one column, the one the other converts into without loss. */
    private static RexNode widerOf(RexNode current, RexNode candidate) {
        return RexUtil.isLosslessCast(current.getType(), candidate.getType()) ? candidate : current;
    }

    // ---- Small helpers ----

    /** A non-null integer literal, possibly wrapped in casts that keep it integer. */
    private static boolean isIntegerLiteral(RexNode node) {
        if (!RexUtil.isLiteral(node, true) || !isIntegerType(node.getType())) {
            return false;
        }
        RexLiteral literal = (RexLiteral) RexUtil.removeCast(node);
        return !literal.isNull() && isIntegerType(literal.getType());
    }

    private static BigDecimal literalValue(RexNode node) {
        return ((RexLiteral) RexUtil.removeCast(node)).getValueAs(BigDecimal.class);
    }

    private static boolean isIntegerType(RelDataType type) {
        return SqlTypeName.INT_TYPES.contains(type.getSqlTypeName());
    }

    /**
     * Whether {@code column ± offset} stays inside the 64-bit range for every possible value of {@code column}, judged
     * from the column's own type (lossless casts stripped): a SMALLINT or INTEGER column has room for any practical
     * {@code k}, a BIGINT column only for {@code k == 0}.
     */
    private static boolean shiftCannotOverflowLong(RexNode column, BigDecimal offset) {
        BigDecimal bound = switch (stripLosslessCasts(column).getType().getSqlTypeName()) {
            case TINYINT -> BigDecimal.valueOf(Byte.MAX_VALUE + 1L);
            case SMALLINT -> BigDecimal.valueOf(Short.MAX_VALUE + 1L);
            case INTEGER -> BigDecimal.valueOf(Integer.MAX_VALUE + 1L);
            default -> BigDecimal.valueOf(Long.MAX_VALUE);
        };
        // |column| <= bound, so |column ± k| <= bound + |k| must not exceed Long.MAX_VALUE.
        return bound.add(offset.abs()).compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0;
    }
}

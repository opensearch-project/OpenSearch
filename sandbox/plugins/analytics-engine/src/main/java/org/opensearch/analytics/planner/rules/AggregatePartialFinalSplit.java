/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.DistributedAggregateRewriter.FinalAggCallBuilder;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Builds {@code FINAL(gather(PARTIAL(partitionedInput)))} from a single-stage aggregate. The ONE place that
 * knows how a two-phase aggregate is assembled, so the two callers that decide WHEN to split cannot drift:
 * <ul>
 *   <li>{@link OpenSearchAggregateSplitRule} — during CBO, gathering via {@code convert()} so Volcano owns
 *       the exchange;</li>
 *   <li>{@link OpenSearchPartialAggregatePushdownRewriter} — post-CBO, gathering via
 *       {@code buildReducer()} because the satisfies-gated enforcer would insert nothing over an input whose
 *       trait set still carries CBO's {@code coordSingleton}.</li>
 * </ul>
 * That difference is the only thing the callers vary, so it arrives as the {@code gather} function; every
 * other decision — which agg calls the PARTIAL runs, which the FINAL re-merges, the literal args that must
 * survive the split, the row-type repair — is identical and lives here.
 *
 * <p>Splitting is only CORRECT for some aggregates; that gate is
 * {@link OpenSearchAggregateSplitRule#shouldSkipPartialFinalSplit}, and both callers check it before asking
 * for a split. This class assumes the decision has been made.
 *
 * @opensearch.internal
 */
final class AggregatePartialFinalSplit {

    private AggregatePartialFinalSplit() {}

    /**
     * @param agg              the single-stage aggregate to split
     * @param partitionedInput the input the PARTIAL will run over, per partition
     * @param traitDef         the per-query distribution trait def
     * @param gather           how to place the exchange above the PARTIAL — {@code convert()} during CBO,
     *                         {@code buildReducer()} post-CBO
     * @return the FINAL aggregate, wrapped in a cast Project when the split changed a nullability
     */
    static RelNode split(
        OpenSearchAggregate agg,
        RelNode partitionedInput,
        OpenSearchDistributionTraitDef traitDef,
        Function<OpenSearchAggregate, RelNode> gather
    ) {
        List<AggregateCall> partialCalls = repairLossyReturnTypes(agg.getAggCallList(), partitionedInput);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            agg.getCluster(),
            partitionedInput.getTraitSet().replace(OpenSearchConvention.INSTANCE),
            partitionedInput,
            agg.getGroupSet(),
            agg.getGroupSets(),
            partialCalls,
            AggregateMode.PARTIAL,
            agg.getViableBackends(),
            agg.getCallAnnotations()
        );
        RelNode gathered = gather.apply(partial);

        Map<Integer, List<RexLiteral>> finalExtraLiterals = captureLiteralArgsForFinal(agg.getAggCallList(), partitionedInput);
        // Classify ORIGINAL aggCalls once and stash on FINAL for post-Volcano transformers.
        List<IntermediateField> intermediateFields = FinalAggCallBuilder.classify(agg.getAggCallList());
        // Build FINAL's aggCalls against gathered's row type so typeMatchesInferred passes.
        List<AggregateCall> finalCalls = FinalAggCallBuilder.buildFinalCalls(
            agg.getAggCallList(),
            intermediateFields,
            agg.getGroupSet().cardinality(),
            gathered,
            agg.getGroupSet().isEmpty()
        );
        OpenSearchAggregate finalAgg = new OpenSearchAggregate(
            agg.getCluster(),
            gathered.getTraitSet().replace(traitDef.coordSingleton()),
            gathered,
            agg.getGroupSet(),
            agg.getGroupSets(),
            finalCalls,
            AggregateMode.FINAL,
            agg.getViableBackends(),
            agg.getCallAnnotations(),
            finalExtraLiterals,
            intermediateFields
        );
        // Empty-group nullability gap (COUNT→SUM swap): wrap FINAL so its row type matches SINGLE's.
        return wrapWithCastIfNeeded(finalAgg, agg);
    }

    /**
     * An empty-group COUNT becomes a SUM on the FINAL, which is NULLABLE where COUNT was not (no rows → no
     * partial → null). Volcano rejects an alternative whose row type differs from the original's, so cast
     * the differing columns back. Returns {@code finalAggregate} unchanged when every type already matches.
     */
    private static RelNode wrapWithCastIfNeeded(OpenSearchAggregate finalAggregate, OpenSearchAggregate expected) {
        RelDataType actualType = finalAggregate.getRowType();
        RelDataType expectedType = expected.getRowType();
        RexBuilder rexBuilder = finalAggregate.getCluster().getRexBuilder();

        List<RexNode> projects = new ArrayList<>(actualType.getFieldCount());
        boolean anyTypeDiffers = false;
        for (int idx = 0; idx < actualType.getFieldCount(); idx++) {
            RelDataType columnType = actualType.getFieldList().get(idx).getType();
            RelDataType targetType = expectedType.getFieldList().get(idx).getType();
            RexNode ref = new RexInputRef(idx, columnType);
            if (columnType.equals(targetType)) {
                projects.add(ref);
            } else {
                projects.add(rexBuilder.makeCast(targetType, ref));
                anyTypeDiffers = true;
            }
        }
        if (!anyTypeDiffers) return finalAggregate;

        return new OpenSearchProject(
            finalAggregate.getCluster(),
            finalAggregate.getTraitSet(),
            finalAggregate,
            projects,
            expectedType,
            finalAggregate.getViableBackends()
        );
    }

    /**
     * {@code LIST}/{@code VALUES} declare an ARRAY return type inferred from the ORIGINAL input. The PARTIAL
     * runs over the same columns, so re-derive the element type from {@code input} — otherwise the PARTIAL
     * carries a stale array type and Calcite's row-type equivalence check rejects the alternative.
     */
    private static List<AggregateCall> repairLossyReturnTypes(List<AggregateCall> aggCalls, RelNode input) {
        List<AggregateCall> rebuilt = null;
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall call = aggCalls.get(i);
            String name = call.getAggregation().getName();
            if (!"LIST".equalsIgnoreCase(name) && !"VALUES".equalsIgnoreCase(name)) continue;
            if (call.getArgList().isEmpty()) continue;
            RelDataType arg0Type = input.getRowType().getFieldList().get(call.getArgList().get(0)).getType();
            RelDataType repaired = input.getCluster().getTypeFactory().createArrayType(arg0Type, -1);
            if (repaired.equals(call.getType())) continue;
            if (rebuilt == null) rebuilt = new ArrayList<>(aggCalls);
            rebuilt.set(
                i,
                AggregateCall.create(
                    call.getAggregation(),
                    call.isDistinct(),
                    call.isApproximate(),
                    call.ignoreNulls(),
                    call.rexList,
                    call.getArgList(),
                    call.filterArg,
                    call.distinctKeys,
                    call.collation,
                    repaired,
                    call.getName()
                )
            );
        }
        return rebuilt != null ? rebuilt : aggCalls;
    }

    /**
     * A STATE_EXPANDING aggregate's trailing args are literals the FINAL still needs (percentile's percent
     * flag is the live case), but the PARTIAL's output carries opaque state columns instead of those literal
     * columns. Capture them off the Project below while it is still in reach, so FINAL can re-create them.
     */
    private static Map<Integer, List<RexLiteral>> captureLiteralArgsForFinal(List<AggregateCall> aggCalls, RelNode child) {
        if (!(RelNodeUtils.unwrapHep(child) instanceof Project project)) {
            return Map.of();
        }
        List<RexNode> projects = project.getProjects();
        Map<Integer, List<RexLiteral>> captured = new LinkedHashMap<>();
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall call = aggCalls.get(i);
            AggregateFunction fn = AggregateFunction.fromSqlAggFunction(call.getAggregation());
            if (fn == null || fn.getType() != AggregateFunction.Type.STATE_EXPANDING) continue;
            List<Integer> args = call.getArgList();
            if (args.size() < 2) continue;
            List<RexLiteral> literals = new ArrayList<>(args.size() - 1);
            boolean allLiteral = true;
            for (int a = 1; a < args.size(); a++) {
                int colIdx = args.get(a);
                if (colIdx < 0 || colIdx >= projects.size() || !(projects.get(colIdx) instanceof RexLiteral lit)) {
                    allLiteral = false;
                    break;
                }
                literals.add(lit);
            }
            if (allLiteral && !literals.isEmpty()) {
                captured.put(i, List.copyOf(literals));
            }
        }
        return captured;
    }
}

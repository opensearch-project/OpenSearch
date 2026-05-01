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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.spi.AggregateDecomposition;
import org.opensearch.analytics.spi.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Volcano CBO rule that splits an {@link OpenSearchAggregate} into
 * PARTIAL + FINAL when the input is partitioned.
 *
 * <p>Requests SINGLETON distribution on the partial output, letting Volcano's
 * trait enforcement (via {@code ExpandConversionRule} + {@code OpenSearchDistributionTraitDef})
 * automatically insert an {@code OpenSearchExchangeReducer}.
 *
 * <p>TODO (plan forking): aggregate decomposition is intentionally deferred to plan forking
 * resolution, after a single backend has been chosen per alternative. Decomposition is
 * backend-specific — different backends may emit different partial state schemas for the
 * same function (e.g. standard SUM+COUNT for AVG vs a backend's native running state).
 * Applying decomposition here would force a single schema before backends are resolved,
 * which breaks the multi-alternative model.
 *
 * <p>During plan forking resolution, for each PARTIAL+FINAL pair in a chosen-backend alternative:
 * <ol>
 *   <li>Look up {@link org.opensearch.analytics.spi.AggregateCapability#decomposition()} for
 *       each AggregateCall using the chosen backend.</li>
 *   <li>If null: apply Calcite's {@code AggregateReduceFunctionsRule} to rewrite
 *       AVG → SUM/COUNT, STDDEV → SUM(x²)+SUM(x)+COUNT, etc.</li>
 *   <li>If non-null: use {@code AggregateDecomposition.partialCalls(AggregateCall)}
 *       to rewrite PARTIAL's aggCalls and output row type, and
 *       {@code AggregateDecomposition.finalExpression()} to
 *       rewrite FINAL's aggCalls. Both must be updated together — the exchange row type
 *       between them must be consistent within the same plan alternative.</li>
 * </ol>
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateSplitRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchAggregateSplitRule(PlannerContext context) {
        super(operand(OpenSearchAggregate.class, operand(RelNode.class, any())), "OpenSearchAggregateSplitRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        return aggregate.getMode() == AggregateMode.SINGLE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        RelNode child = call.rel(1);
        RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

        // Determine the chosen backend (first viable — all support the same decompositions)
        String backend = aggregate.getViableBackends().getFirst();

        // Build PARTIAL aggCalls, expanding any decomposed functions.
        // partialCallMap[i] = list of partial AggregateCall indices for original call i
        List<AggregateCall> partialCalls = new ArrayList<>();
        List<AggregateDecomposition> decompositions = new ArrayList<>(); // null = no decomposition
        List<Integer> partialStartIndex = new ArrayList<>(); // start index in partialCalls for each original call

        for (AggregateCall origCall : aggregate.getAggCallList()) {
            AggregateFunction func = AggregateFunction.fromSqlKind(origCall.getAggregation().getKind());
            if (func == null) func = AggregateFunction.fromNameOrError(origCall.getAggregation().getName());
            AggregateDecomposition decomp = context.getCapabilityRegistry().getDecomposition(backend, func);
            partialStartIndex.add(partialCalls.size());
            if (decomp != null) {
                decompositions.add(decomp);
                partialCalls.addAll(decomp.partialCalls(origCall, child));
            } else {
                decompositions.add(null);
                partialCalls.add(origCall);
            }
        }

        // Partial aggregate: runs on each shard
        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(), partialTraits, child,
            aggregate.getGroupSet(), aggregate.getGroupSets(),
            partialCalls, AggregateMode.PARTIAL, aggregate.getViableBackends()
        );

        // Request SINGLETON distribution — Volcano inserts Exchange automatically
        RelTraitSet singletonTraits = partial.getTraitSet().replace(context.getDistributionTraitDef().singleton());
        RelNode gathered = convert(partial, singletonTraits);

        int groupCount = aggregate.getGroupSet().cardinality();

        // Check if any call has a decomposition — if so, we need a Project on top of FINAL
        boolean hasDecomposition = decompositions.stream().anyMatch(d -> d != null);

        if (!hasDecomposition) {
            // No decomposition: standard remap (same as before)
            List<AggregateCall> finalCalls = new ArrayList<>();
            for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
                AggregateCall origCall = aggregate.getAggCallList().get(i);
                finalCalls.add(origCall.adaptTo(gathered, List.of(groupCount + i), origCall.filterArg, groupCount, aggregate.getGroupCount()));
            }
            OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
                aggregate.getCluster(), singletonTraits, gathered,
                aggregate.getGroupSet(), aggregate.getGroupSets(),
                finalCalls, AggregateMode.FINAL, aggregate.getViableBackends()
            );
            call.transformTo(finalAggregate);
            return;
        }

        // With decomposition: FINAL aggregate has the partial calls (SUM+COUNT etc.),
        // then a Project applies finalExpression() to produce the original output columns.
        //
        // FINAL aggCalls: remap each partial call to reference its column in gathered output
        int partialOutputCount = groupCount + partialCalls.size();
        List<AggregateCall> finalAggCalls = new ArrayList<>();
        for (int pi = 0; pi < partialCalls.size(); pi++) {
            AggregateCall pc = partialCalls.get(pi);
            finalAggCalls.add(pc.adaptTo(gathered, List.of(groupCount + pi), pc.filterArg, groupCount, aggregate.getGroupCount()));
        }
        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(), singletonTraits, gathered,
            aggregate.getGroupSet(), aggregate.getGroupSets(),
            finalAggCalls, AggregateMode.FINAL, aggregate.getViableBackends()
        );

        // Project: group-by columns pass through, then finalExpression() for each original call
        List<RexNode> projectExprs = new ArrayList<>();
        List<String> projectNames = new ArrayList<>();
        // Group-by columns
        for (int i = 0; i < groupCount; i++) {
            projectExprs.add(rexBuilder.makeInputRef(finalAggregate, i));
            projectNames.add(finalAggregate.getRowType().getFieldList().get(i).getName());
        }
        // Aggregate output columns
        for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
            AggregateCall origCall = aggregate.getAggCallList().get(i);
            AggregateDecomposition decomp = decompositions.get(i);
            int startIdx = partialStartIndex.get(i);
            int endIdx = (i + 1 < partialStartIndex.size()) ? partialStartIndex.get(i + 1) : partialCalls.size();
            if (decomp != null) {
                // Build refs to the partial output columns in the FINAL aggregate's output
                List<RexNode> partialRefs = new ArrayList<>();
                for (int pi = startIdx; pi < endIdx; pi++) {
                    partialRefs.add(rexBuilder.makeInputRef(finalAggregate, groupCount + pi));
                }
                projectExprs.add(decomp.finalExpression(rexBuilder, partialRefs));
            } else {
                projectExprs.add(rexBuilder.makeInputRef(finalAggregate, groupCount + startIdx));
            }
            projectNames.add(origCall.name != null ? origCall.name : "expr$" + i);
        }

        RelNode project = new OpenSearchProject(
            aggregate.getCluster(),
            singletonTraits,
            finalAggregate,
            projectExprs,
            aggregate.getCluster().getTypeFactory().createStructType(
                projectExprs.stream().map(RexNode::getType).toList(), projectNames
            ),
            aggregate.getViableBackends()
        );

        call.transformTo(project);
    }
}

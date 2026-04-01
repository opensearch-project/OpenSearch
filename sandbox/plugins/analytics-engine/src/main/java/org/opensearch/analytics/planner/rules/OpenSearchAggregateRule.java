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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateCallAnnotation;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.OperatorCapability;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Converts {@link Aggregate} → {@link OpenSearchAggregate}.
 *
 * <p>Annotates each {@link AggregateCall} with viable backends by embedding
 * an {@link AggregateCallAnnotation} in its rexList. Computes operator-level
 * viable backends as the intersection of per-call viable backends.
 *
 * <p>The split into PARTIAL + FINAL is NOT done here. It happens via
 * {@link OpenSearchAggregateSplitRule} which fires when Volcano detects
 * a distribution trait mismatch (RANDOM input needing SINGLETON output).
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateRule extends RelOptRule {

    private static final Logger logger = LogManager.getLogger(OpenSearchAggregateRule.class);

    private final PlannerContext context;

    public OpenSearchAggregateRule(PlannerContext context) {
        super(operand(Aggregate.class, operand(RelNode.class, any())), "OpenSearchAggregateRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);
        RelNode child = call.rel(1);

        if (aggregate instanceof OpenSearchAggregate) {
            return;
        }

        if (!(child instanceof OpenSearchRelNode openSearchChild)) {
            throw new IllegalStateException(
                "Aggregate rule encountered unmarked child [" + child.getClass().getSimpleName() + "]");
        }

        String childBackend = openSearchChild.getBackend();

        // Annotate each AggregateCall with per-call viable backends
        List<AggregateCall> annotatedCalls = new ArrayList<>();
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            List<String> callViable = resolveViableBackendsForCall(aggCall);
            if (callViable.isEmpty()) {
                throw new IllegalStateException(
                    "No backend supports aggregate function [" + aggCall.getAggregation().getName() + "]");
            }
            annotatedCalls.add(AggregateCallAnnotation.annotate(aggCall, callViable));
        }

        // Compute operator-level viable backends considering delegation
        List<String> viableBackends = computeAggregateViableBackends(annotatedCalls, childBackend);

        if (viableBackends.isEmpty()) {
            List<String> funcNames = aggregate.getAggCallList().stream()
                .map(aggCall -> aggCall.getAggregation().getName())
                .toList();
            throw new IllegalStateException(
                "No backend can execute aggregate: functions " + funcNames
                    + " are split across backends and no delegation path exists");
        }

        String backend = viableBackends.contains(childBackend)
            ? childBackend
            : viableBackends.getFirst();

        RelTraitSet aggregateTraits = child.getTraitSet();
        if (aggregateTraits.size() > 0) {
            aggregateTraits = aggregateTraits.replace(context.getDistributionTraitDef().singleton());
        }

        call.transformTo(new OpenSearchAggregate(
            aggregate.getCluster(),
            aggregateTraits,
            RelNodeUtils.unwrapHep(aggregate.getInput()),
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            annotatedCalls,
            AggregateMode.SINGLE,
            backend,
            viableBackends
        ));
    }

    private List<String> resolveViableBackendsForCall(AggregateCall aggCall) {
        AggregateFunction func = AggregateFunction.fromSqlKind(aggCall.getAggregation().getKind());
        if (func == null) {
            func = AggregateFunction.fromNameOrError(aggCall.getAggregation().getName());
        }

        List<String> viable = new ArrayList<>();
        for (AnalyticsSearchBackendPlugin plugin : context.getBackends().values()) {
            if (plugin.supportedOperators().contains(OperatorCapability.AGGREGATE)
                && plugin.supportedAggregateFunctions().contains(func)) {
                viable.add(plugin.name());
            }
        }
        return viable;
    }

    /**
     * Computes which backends can execute this aggregate, considering delegation.
     * A backend is viable if for every agg call it can handle natively OR delegate
     * (supports AGGREGATE delegation AND some other backend accepts it for that call).
     */
    private List<String> computeAggregateViableBackends(List<AggregateCall> annotatedCalls,
                                                        String childBackend) {
        if (annotatedCalls.isEmpty()) {
            return new ArrayList<>(context.getBackends().keySet());
        }

        List<String> viable = new ArrayList<>();
        for (AnalyticsSearchBackendPlugin candidate : context.getBackends().values()) {
            if (!candidate.supportedOperators().contains(OperatorCapability.AGGREGATE)) {
                continue;
            }

            boolean canHandleAll = true;
            for (AggregateCall call : annotatedCalls) {
                AggregateCallAnnotation annotation = AggregateCallAnnotation.find(call);
                if (annotation == null) {
                    canHandleAll = false;
                    break;
                }
                List<String> callViable = annotation.getViableBackends();
                if (callViable.contains(candidate.name())) {
                    continue;
                }
                // Check if candidate can delegate this call
                if (candidate.supportedDelegations().contains(DelegationType.AGGREGATE)) {
                    boolean someoneAccepts = callViable.stream().anyMatch(backendName -> {
                        AnalyticsSearchBackendPlugin other = context.getBackends().get(backendName);
                        return other != null && other.acceptedDelegations().contains(DelegationType.AGGREGATE);
                    });
                    if (someoneAccepts) {
                        continue;
                    }
                }
                canHandleAll = false;
                break;
            }
            if (canHandleAll) {
                viable.add(candidate.name());
            }
        }
        return viable;
    }
}

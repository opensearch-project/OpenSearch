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
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageInfo;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateCallAnnotation;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts {@link Aggregate} → {@link OpenSearchAggregate}.
 *
 * <p>Annotates each {@link AggregateCall} with viable backends in
 * {@link OpenSearchAggregate#getCallAnnotations()} (a side map keyed by call
 * index) — NOT in the call's {@code rexList}, which would shift positional
 * argument inference. Computes operator-level viable backends as the
 * intersection of per-call viable backends.
 *
 * <p>The split into PARTIAL + FINAL is NOT done here. It happens via
 * {@link OpenSearchAggregateSplitRule} which fires when Volcano detects
 * a distribution trait mismatch (RANDOM input needing SINGLETON output).
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateRule extends RelOptRule {

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchAggregateRule.class);

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
            throw new IllegalStateException("Aggregate rule encountered unmarked child [" + child.getClass().getSimpleName() + "]");
        }

        List<String> childViableBackends = openSearchChild.getViableBackends();
        List<FieldStorageInfo> childFieldStorage = openSearchChild.getOutputFieldStorage();

        // Build per-call annotations into a side map keyed by call index.
        Map<Integer, AggregateCallAnnotation> annotations = new LinkedHashMap<>();
        List<AggregateCall> aggCalls = aggregate.getAggCallList();
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall aggCall = aggCalls.get(i);
            List<String> callViable = resolveViableBackendsForCall(aggCall, childFieldStorage);
            if (callViable.isEmpty()) {
                throw new IllegalStateException(
                    "No backend supports aggregate function [" + aggCall.getAggregation().getName() + "]"
                );
            }
            annotations.put(i, new AggregateCallAnnotation(callViable, context.nextAnnotationId()));
        }

        // Compute operator-level viable backends: must be viable for child AND handle agg calls
        List<String> viableBackends = computeAggregateViableBackends(annotations, childViableBackends);

        if (viableBackends.isEmpty()) {
            List<String> funcNames = aggCalls.stream().map(c -> c.getAggregation().getName()).toList();
            throw new IllegalStateException(
                "No backend can execute aggregate: functions "
                    + funcNames
                    + " not supported by any viable backend among "
                    + childViableBackends
            );
        }

        LOGGER.debug("Aggregate viable backends: {} (child viable: {})", viableBackends, childViableBackends);

        RelTraitSet aggregateTraits = child.getTraitSet().replace(context.getDistributionTraitDef().singleton());

        call.transformTo(
            new OpenSearchAggregate(
                aggregate.getCluster(),
                aggregateTraits,
                RelNodeUtils.unwrapHep(aggregate.getInput()),
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggCalls,
                AggregateMode.SINGLE,
                viableBackends,
                annotations
            )
        );
    }

    private List<String> resolveViableBackendsForCall(AggregateCall aggCall, List<FieldStorageInfo> childFieldStorageInfos) {
        AggregateFunction func = AggregateFunction.fromSqlKind(aggCall.getAggregation().getKind());
        if (func == null) {
            func = AggregateFunction.fromNameOrError(aggCall.getAggregation().getName());
        }

        CapabilityRegistry registry = context.getCapabilityRegistry();

        if (aggCall.getArgList().isEmpty()) {
            return new ArrayList<>(registry.aggregateCapableBackends());
        }

        List<String> callViable = null;
        for (int fieldIndex : aggCall.getArgList()) {
            FieldStorageInfo storageInfo = FieldStorageInfo.resolve(childFieldStorageInfos, fieldIndex);
            FieldType fieldType = storageInfo.getFieldType();

            Set<String> perFieldBackends = new HashSet<>();
            if (storageInfo.isDerived()) {
                perFieldBackends.addAll(registry.aggregateBackendsAnyFormat(func, fieldType));
            } else {
                // Format-aware: backends that can read this field's doc values and aggregate
                perFieldBackends.addAll(registry.aggregateBackendsForField(func, storageInfo));
                // Delegation targets: backends that declared acceptedDelegations(AGGREGATE) and
                // can aggregate this function — they receive data via Arrow batch, not field storage.
                // TODO: once DelegationType split (NATIVE_INDEX vs ARROW_BATCH) is designed,
                // restrict this to ARROW_BATCH delegation acceptors only.
                for (String name : registry.aggregateBackendsAnyFormat(func, fieldType)) {
                    if (registry.delegationAcceptors(DelegationType.AGGREGATE).contains(name)) {
                        perFieldBackends.add(name);
                    }
                }
            }

            if (callViable == null) {
                callViable = new ArrayList<>(perFieldBackends);
            } else {
                callViable.retainAll(perFieldBackends);
            }
        }

        return callViable != null ? callViable : new ArrayList<>(registry.aggregateCapableBackends());
    }

    private List<String> computeAggregateViableBackends(
        Map<Integer, AggregateCallAnnotation> annotations,
        List<String> childViableBackends
    ) {
        if (annotations.isEmpty()) {
            return new ArrayList<>(childViableBackends);
        }

        CapabilityRegistry registry = context.getCapabilityRegistry();

        List<String> viable = new ArrayList<>();
        for (String candidateName : childViableBackends) {
            if (!registry.aggregateCapableBackends().contains(candidateName)) {
                continue;
            }

            boolean canHandleAll = true;
            for (AggregateCallAnnotation annotation : annotations.values()) {
                if (!registry.canHandle(candidateName, annotation.getViableBackends(), DelegationType.AGGREGATE)) {
                    canHandleAll = false;
                    break;
                }
            }
            if (canHandleAll) {
                viable.add(candidateName);
            }
        }
        return viable;
    }
}

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
import org.opensearch.analytics.spi.OperatorCapability;

import java.util.ArrayList;
import java.util.List;

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
            throw new IllegalStateException("Aggregate rule encountered unmarked child [" + child.getClass().getSimpleName() + "]");
        }

        List<String> childViableBackends = openSearchChild.getViableBackends();
        List<FieldStorageInfo> childFieldStorage = openSearchChild.getOutputFieldStorage();

        // Annotate each AggregateCall with per-call viable backends
        List<AggregateCall> annotatedCalls = new ArrayList<>();
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            List<String> callViable = resolveViableBackendsForCall(aggCall, childFieldStorage);
            if (callViable.isEmpty()) {
                throw new IllegalStateException("No backend supports aggregate function [" + aggCall.getAggregation().getName() + "]");
            }
            annotatedCalls.add(AggregateCallAnnotation.annotate(aggCall, callViable, context.nextAnnotationId()));
        }

        // Compute operator-level viable backends: must be viable for child AND handle agg calls
        List<String> viableBackends = computeAggregateViableBackends(annotatedCalls, childViableBackends);

        if (viableBackends.isEmpty()) {
            List<String> funcNames = aggregate.getAggCallList().stream().map(aggCall -> aggCall.getAggregation().getName()).toList();
            throw new IllegalStateException(
                "No backend can execute aggregate: functions "
                    + funcNames
                    + " not supported by any viable backend among "
                    + childViableBackends
            );
        }

        logger.debug("Aggregate viable backends: {} (child viable: {})", viableBackends, childViableBackends);

        RelTraitSet aggregateTraits = child.getTraitSet();
        if (aggregateTraits.size() > 0) {
            aggregateTraits = aggregateTraits.replace(context.getDistributionTraitDef().singleton());
        }

        call.transformTo(
            new OpenSearchAggregate(
                aggregate.getCluster(),
                aggregateTraits,
                RelNodeUtils.unwrapHep(aggregate.getInput()),
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                annotatedCalls,
                AggregateMode.SINGLE,
                viableBackends
            )
        );
    }

    private List<String> resolveViableBackendsForCall(AggregateCall aggCall, List<FieldStorageInfo> childFieldStorage) {
        AggregateFunction func = AggregateFunction.fromSqlKind(aggCall.getAggregation().getKind());
        if (func == null) {
            func = AggregateFunction.fromNameOrError(aggCall.getAggregation().getName());
        }

        CapabilityRegistry registry = context.getCapabilityRegistry();

        if (aggCall.getArgList().isEmpty()) {
            return new ArrayList<>(registry.operatorBackends(OperatorCapability.AGGREGATE));
        }

        List<String> callViable = null;
        for (int fieldIndex : aggCall.getArgList()) {
            if (fieldIndex >= childFieldStorage.size()) {
                continue;
            }
            FieldStorageInfo storageInfo = childFieldStorage.get(fieldIndex);
            FieldType fieldType = storageInfo.getFieldType();
            if (fieldType == null) {
                throw new IllegalStateException(
                    "Unrecognized field type [" + storageInfo.getMappingType() + "] for field [" + storageInfo.getFieldName() + "]"
                );
            }

            List<String> perFieldBackends = new ArrayList<>();
            if (storageInfo.isDerived()) {
                perFieldBackends.addAll(registry.aggregateBackendsAnyFormat(func, fieldType));
            } else {
                // Format-aware: backends that can read the data and compute
                for (String format : storageInfo.getDocValueFormats()) {
                    for (String name : registry.aggregateBackends(func, fieldType, format)) {
                        if (!perFieldBackends.contains(name)) {
                            perFieldBackends.add(name);
                        }
                    }
                }
                // Format-agnostic: delegation targets that can compute but don't need data access
                for (String name : registry.aggregateBackendsAnyFormat(func, fieldType)) {
                    if (!perFieldBackends.contains(name)) {
                        perFieldBackends.add(name);
                    }
                }
            }

            if (callViable == null) {
                callViable = perFieldBackends;
            } else {
                callViable.retainAll(perFieldBackends);
            }
        }

        return callViable != null ? callViable : new ArrayList<>(registry.operatorBackends(OperatorCapability.AGGREGATE));
    }

    private List<String> computeAggregateViableBackends(List<AggregateCall> annotatedCalls, List<String> childViableBackends) {
        if (annotatedCalls.isEmpty()) {
            return new ArrayList<>(childViableBackends);
        }

        CapabilityRegistry registry = context.getCapabilityRegistry();
        List<String> delegationSupporters = registry.delegationSupporters(DelegationType.AGGREGATE);
        List<String> delegationAcceptors = registry.delegationAcceptors(DelegationType.AGGREGATE);

        List<String> viable = new ArrayList<>();
        for (String candidateName : childViableBackends) {
            if (!registry.operatorBackends(OperatorCapability.AGGREGATE).contains(candidateName)) {
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
                if (callViable.contains(candidateName)) {
                    continue;
                }
                if (delegationSupporters.contains(candidateName) && callViable.stream().anyMatch(delegationAcceptors::contains)) {
                    continue;
                }
                canHandleAll = false;
                break;
            }
            if (canHandleAll) {
                viable.add(candidateName);
            }
        }
        return viable;
    }
}

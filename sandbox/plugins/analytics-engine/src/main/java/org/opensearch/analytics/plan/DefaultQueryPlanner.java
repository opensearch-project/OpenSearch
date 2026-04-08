/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.opensearch.analytics.plan.operators.AggMode;
import org.opensearch.analytics.plan.operators.BackendSpecificRexNode;
import org.opensearch.analytics.plan.operators.BackendTagged;
import org.opensearch.analytics.plan.operators.OpenSearchAggregate;
import org.opensearch.analytics.plan.operators.OpenSearchFilter;
import org.opensearch.analytics.plan.operators.OpenSearchHybridFilter;
import org.opensearch.analytics.plan.operators.UnresolvedRexNode;
import org.opensearch.analytics.plan.registry.BackendCapabilityRegistry;
import org.opensearch.analytics.plan.rules.AggSplitRule;
import org.opensearch.analytics.plan.rules.OperatorWrapperVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link QueryPlanner}.
 *
 * <p>Executes a five-phase pipeline:
 * <ol>
 *   <li>Validate — check all operators are supported</li>
 *   <li>Optimize — HepPlanner + RelFieldTrimmer</li>
 *   <li>Wrap — convert Logical* to OpenSearch* operators</li>
 *   <li>AggSplit — split UNRESOLVED aggregates into PARTIAL/FINAL (skipped for single-shard)</li>
 *   <li>Resolve — assign backend tags bottom-up</li>
 * </ol>
 *
 * <p>Requirements: 1.1, 2.1, 3.1, 4.1, 5.1, 6.1
 */
public final class DefaultQueryPlanner implements QueryPlanner {

    private static final Logger logger = LogManager.getLogger(DefaultQueryPlanner.class);

    private final BackendCapabilityRegistry registry;
    private final RelOptCluster cluster;
    private final FieldCapabilityResolver fieldCapabilityResolver;

    public DefaultQueryPlanner(BackendCapabilityRegistry registry,
                               RelOptCluster cluster,
                               FieldCapabilityResolver fieldCapabilityResolver) {
        this.registry = registry;
        this.cluster = cluster;
        this.fieldCapabilityResolver = fieldCapabilityResolver;
    }

    @Override
    public ResolvedPlan plan(RelNode logicalPlan, int shardCount) {
        logger.info("[QueryPlanner] Input plan:\n{}", logicalPlan.explain());
        validate(logicalPlan);
        RelNode optimized = optimize(logicalPlan);
        logger.info("[QueryPlanner] After optimize:\n{}", optimized.explain());
        RelNode wrapped = wrap(optimized);
        logger.info("[QueryPlanner] After wrap:\n{}", wrapped.explain());
        RelNode split = shardCount == 1 ? markAggsFinal(wrapped) : aggSplit(wrapped);
        if (shardCount != 1) {
            logger.info("[QueryPlanner] After aggSplit:\n{}", split.explain());
        }
        ResolvedPlan result = resolve(split);
        logger.info("[QueryPlanner] After resolve (backend={}): \n{}", result.getBackendName(), result.getRoot().explain());
        return result;
    }

    // -----------------------------------------------------------------------
    // Phase 1 — Validate
    // -----------------------------------------------------------------------

    private void validate(RelNode root) {
        List<String> errors = new ArrayList<>();
        String tableName = extractTableName(root);

        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                // Check 1: operator class supported by at least one backend
                if (registry.backendsForOperator(node.getClass()).isEmpty()) {
                    errors.add("No backend supports operator: " + node.getClass().getSimpleName());
                }

                // Check 2: UnresolvedRexNode payloads accepted by at least one backend
                collectUnresolvedRexNodes(node).forEach(unresolved -> {
                    if (registry.backendForUnresolvedPredicate(unresolved.getPayload()).isEmpty()) {
                        errors.add("UnresolvedRexNode payload not accepted by any backend: " + unresolved);
                    }
                });

                // Check 3: full-text predicates on non-indexed fields
                if (node instanceof LogicalFilter && tableName != null) {
                    validateFilterPredicates(((LogicalFilter) node).getCondition(), tableName, errors);
                }

                super.visit(node, ordinal, parent);
            }
        }.go(root);

        if (!errors.isEmpty()) {
            throw new QueryPlanningException(errors);
        }
    }

    private List<UnresolvedRexNode> collectUnresolvedRexNodes(RelNode node) {
        List<UnresolvedRexNode> result = new ArrayList<>();
        if (node instanceof Filter) {
            collectUnresolvedFromRex(((Filter) node).getCondition(), result);
        }
        return result;
    }

    private void collectUnresolvedFromRex(RexNode rex, List<UnresolvedRexNode> result) {
        if (rex instanceof UnresolvedRexNode) {
            result.add((UnresolvedRexNode) rex);
        } else if (rex instanceof org.apache.calcite.rex.RexCall) {
            for (RexNode operand : ((org.apache.calcite.rex.RexCall) rex).getOperands()) {
                collectUnresolvedFromRex(operand, result);
            }
        }
    }

    private void validateFilterPredicates(RexNode condition, String tableName, List<String> errors) {
        // Walk RexNode tree looking for field references in LIKE patterns on non-text-indexed fields
        // Simplified: full implementation would inspect specific predicate patterns
    }

    private String extractTableName(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.TableScan) {
            List<String> names = node.getTable().getQualifiedName();
            return names.get(names.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // Phase 2 — Optimize
    // -----------------------------------------------------------------------

    private RelNode optimize(RelNode root) {
        HepProgram program = new HepProgramBuilder()
            .addMatchOrder(HepMatchOrder.BOTTOM_UP)
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .build();
        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(root);
        RelNode optimized = planner.findBestExp();

        RelBuilder relBuilder = RelBuilder.proto(Contexts.EMPTY_CONTEXT)
            .create(root.getCluster(), null);
        return new RelFieldTrimmer(null, relBuilder).trim(optimized);
    }

    // -----------------------------------------------------------------------
    // Phase 3 — Wrap
    // -----------------------------------------------------------------------

    private RelNode wrap(RelNode root) {
        return root.accept(new OperatorWrapperVisitor());
    }

    // -----------------------------------------------------------------------
    // Phase 4 — AggSplit
    // -----------------------------------------------------------------------

    private RelNode aggSplit(RelNode root) {
        HepProgram program = new HepProgramBuilder()
            .addMatchOrder(HepMatchOrder.BOTTOM_UP)
            .addRuleInstance(AggSplitRule.INSTANCE)
            .build();
        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(root);
        return planner.findBestExp();
    }

    /**
     * For single-shard queries, marks all UNRESOLVED aggregates as FINAL
     * since no partial/final split is needed.
     */
    private RelNode markAggsFinal(RelNode node) {
        List<RelNode> newInputs = node.getInputs().stream()
            .map(this::markAggsFinal)
            .collect(Collectors.toList());
        RelNode updated = node.copy(node.getTraitSet(), newInputs);
        if (updated instanceof OpenSearchAggregate) {
            OpenSearchAggregate agg = (OpenSearchAggregate) updated;
            if (agg.getMode() == AggMode.UNRESOLVED) {
                return new OpenSearchAggregate(agg.getCluster(), agg.getTraitSet(),
                    agg.getInput(), agg.getGroupSet(), agg.getGroupSets(),
                    agg.getAggCallList(), agg.getBackendTag(), AggMode.FINAL);
            }
        }
        return updated;
    }

    // -----------------------------------------------------------------------
    // Phase 5 — Resolve
    // -----------------------------------------------------------------------

    private ResolvedPlan resolve(RelNode root) {
        RelNode resolvedRoot = resolveNode(root);
        String backendName = ((BackendTagged) resolvedRoot).getBackendTag();
        if ("unresolved".equals(backendName)) {
            throw new QueryPlanningException(List.of(
                "Backend resolution incomplete: root operator still unresolved"));
        }
        return new ResolvedPlan(resolvedRoot, backendName);
    }

    private RelNode resolveNode(RelNode node) {
        // Recurse into inputs first (bottom-up)
        List<RelNode> resolvedInputs = node.getInputs().stream()
            .map(this::resolveNode)
            .collect(Collectors.toList());
        RelNode withResolvedInputs = node.copy(node.getTraitSet(), resolvedInputs);

        if (!(withResolvedInputs instanceof BackendTagged)) {
            throw new QueryPlanningException(List.of(
                "Non-wrapped operator encountered in resolution phase: "
                + withResolvedInputs.getClass().getSimpleName()
                + ". Ensure OperatorWrapperVisitor handles all operator types."));
        }

        // Handle UnresolvedRexNode in filter conditions
        if (withResolvedInputs instanceof OpenSearchFilter) {
            withResolvedInputs = resolveFilterRexNodes((OpenSearchFilter) withResolvedInputs);
        }

        // Assign backend tag — validation (Phase 1) already confirmed operator support,
        // so just pick the first backend from the registry using the Calcite base class.
        final RelNode resolved = withResolvedInputs;
        List<String> backends = registry.backendsForOperator(resolved.getClass());
        String tag = backends.isEmpty()
            ? ((BackendTagged) resolved).getBackendTag()
            : backends.get(0);

        return ((BackendTagged) resolved).withBackendTag(tag);
    }

    private RelNode resolveFilterRexNodes(OpenSearchFilter filter) {
        RexNode condition = filter.getCondition();
        List<UnresolvedRexNode> unresolved = new ArrayList<>();
        collectUnresolvedFromRex(condition, unresolved);

        if (unresolved.isEmpty()) return filter;

        for (UnresolvedRexNode u : unresolved) {
            registry.backendForUnresolvedPredicate(u.getPayload())
                .orElseThrow(() -> new QueryPlanningException(List.of(
                    "UnresolvedRexNode payload not accepted by any backend: " + u)));
        }
        return filter;
    }
}

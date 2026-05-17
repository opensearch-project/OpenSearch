/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateReduceRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchDistributionDeriveRule;
import org.opensearch.analytics.planner.rules.OpenSearchFilterRule;
import org.opensearch.analytics.planner.rules.OpenSearchJoinRule;
import org.opensearch.analytics.planner.rules.OpenSearchJoinSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchProjectRule;
import org.opensearch.analytics.planner.rules.OpenSearchSortRule;
import org.opensearch.analytics.planner.rules.OpenSearchSortSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchTableScanRule;
import org.opensearch.analytics.planner.rules.OpenSearchUnionRule;
import org.opensearch.analytics.planner.rules.OpenSearchUnionSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchValuesRule;

import java.util.List;

/**
 * Central planner for the Analytics Plugin.
 *
 * <p>Two phases:
 * <ol>
 *   <li>HepPlanner (RBO): converts LogicalXxx → OpenSearchXxx with backend
 *       assignment, predicate annotation, and distribution traits.</li>
 *   <li>VolcanoPlanner (CBO): requests SINGLETON at root (coordinator must
 *       gather all results). Split rule fires on aggregates, Volcano inserts
 *       exchanges via trait enforcement where distribution mismatches.</li>
 * </ol>
 *
 * <p>Each optimization phase lives in its own private method so a single
 * {@link RuleProfilingListener} (when profiling is enabled on
 * {@link PlannerContext}) can be threaded through every planner created here.
 *
 * <p>TODO: eliminate copyToCluster — have frontends create RelNodes with Volcano cluster.
 * <p>TODO: DAG construction (cut at exchange boundaries, build stage tree)
 * <p>TODO: Per-stage plan forking (multiple plan generation)
 * <p>TODO: Fragment conversion (backend.getFragmentConvertor())
 * <p>TODO: Join strategy selection, sort removal via CBO
 *
 * @opensearch.internal
 */
public class PlannerImpl {

    private static final Logger LOGGER = LogManager.getLogger(PlannerImpl.class);

    public static RelNode createPlan(RelNode rawRelNode, PlannerContext context) {
        return runAllOptimizations(rawRelNode, context);
    }

    /**
     * Phase 1 (RBO marking) + Phase 2 (CBO exchange insertion).
     * Package-private so planner rule tests can inspect the marked+optimized tree.
     */
    public static RelNode runAllOptimizations(RelNode rawRelNode, PlannerContext context) {
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(rawRelNode));

        RuleProfilingListener listener = context.isProfilingEnabled() ? new RuleProfilingListener() : null;

        RelNode modifiedRelNode = rawRelNode;
        modifiedRelNode = reduceExpressions(modifiedRelNode, listener);
        modifiedRelNode = pushdownRules(modifiedRelNode, listener);
        modifiedRelNode = decomposeAggregates(modifiedRelNode, listener);
        modifiedRelNode = mark(modifiedRelNode, context, listener);
        LOGGER.info("After marking:\n{}", RelOptUtil.toString(modifiedRelNode));
        // TODO(combine-delegated-predicates): a post-marking HEP rule should fuse same-backend
        // AND-sibling AnnotatedPredicates into one combined predicate per group, collapsing N
        // FFM round-trips per RG into one. Blocked on two open design points:
        // 1. Substrait wire representation for a fused {original = AND(call1, call2, ...)}
        // leaf — today the resolver requires a SqlFunction operator on the original.
        // 2. Receiving-backend (Lucene) needs a way to turn the combined payload back into
        // a single BooleanQuery / Weight without polluting ScalarFunction with AND.
        // Revisit once those are designed. The rule would also strip performance peers from
        // AnnotatedPredicates under OR/NOT (Lucene call buys nothing in those positions).
        modifiedRelNode = cbo(modifiedRelNode, rawRelNode, context, listener);
        LOGGER.info("After CBO:\n{}", RelOptUtil.toString(modifiedRelNode));

        if (listener != null) {
            RuleProfilingListener.PlannerProfile profile = listener.snapshot();
            context.recordProfilingResults(profile);
            LOGGER.info("Planner profile:\n{}", profile.format());
        }
        return modifiedRelNode;
    }

    /**
     * Phase 1a: constant-expression reduction on Filter and Project predicates. Kept in
     * its own phase so {@code ProjectReduceExpressionsRule} cannot use a downstream
     * Filter's predicates (introduced by {@link #pushdownRules} in Phase 1b) to rewrite
     * Project expressions. Co-locating the reducer with the transposes lets Calcite
     * simplify e.g. {@code m = (int0 <= 4)} into {@code m = IS NOT NULL(int0)} once the
     * Filter sits below the Project — semantically correct but emits operators the
     * Project may not have backend support for.
     */
    private static RelNode reduceExpressions(RelNode input, RuleProfilingListener listener) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addRuleCollection(
            List.of(
                new ReduceExpressionsRule.FilterReduceExpressionsRule(Filter.class, RelBuilder.proto(Contexts.empty())),
                new ReduceExpressionsRule.ProjectReduceExpressionsRule(Project.class, RelBuilder.proto(Contexts.empty()))
            )
        );
        HepPlanner planner = new HepPlanner(builder.build());
        if (listener != null) {
            planner.addListener(listener);
            listener.beginPhase("reduce-expressions");
        }
        try {
            planner.setRoot(input);
            return planner.findBestExp();
        } finally {
            if (listener != null) listener.endPhase("reduce-expressions");
        }
    }

    /**
     * Phase 1b: Filter pushdown past Project / Aggregate / Join via Calcite's transpose rules.
     * Pre-marking placement keeps marking single-pass — transposes emit plain {@code Logical*}
     * nodes, then marking lowers the canonical post-pushdown shape in one go.
     */
    private static RelNode pushdownRules(RelNode input, RuleProfilingListener listener) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addRuleCollection(
            List.of(CoreRules.FILTER_PROJECT_TRANSPOSE, CoreRules.FILTER_AGGREGATE_TRANSPOSE, CoreRules.FILTER_INTO_JOIN)
        );
        HepPlanner planner = new HepPlanner(builder.build());
        if (listener != null) {
            planner.addListener(listener);
            listener.beginPhase("pushdown-rules");
        }
        try {
            planner.setRoot(input);
            return planner.findBestExp();
        } finally {
            if (listener != null) listener.endPhase("pushdown-rules");
        }
    }

    /**
     * Phase 1b: decompose AVG / STDDEV / VAR into primitive SUM/COUNT (+ SUM_SQ for variance) plus a
     * scalar LogicalProject computing the quotient. Runs as its own HEP pass on plain LogicalAggregate
     * so Calcite's type inference is clean — no AGG_CALL_ANNOTATION wrappers in aggCall.rexList to
     * propagate AVG's DOUBLE return type to the derived primitive calls. Downstream the marking phase,
     * the Volcano split rule, and the AggregateDecompositionResolver see correctly-typed primitives.
     */
    private static RelNode decomposeAggregates(RelNode input, RuleProfilingListener listener) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addRuleInstance(new OpenSearchAggregateReduceRule());
        HepPlanner planner = new HepPlanner(builder.build());
        if (listener != null) {
            planner.addListener(listener);
            listener.beginPhase("aggregate-decompose");
        }
        try {
            planner.setRoot(input);
            return planner.findBestExp();
        } finally {
            if (listener != null) listener.endPhase("aggregate-decompose");
        }
    }

    /**
     * Phase 1c: marking — convert LogicalXxx → OpenSearchXxx bottom-up.
     *
     * <p>TODO: migrate rules from deprecated RelOptRule to RelRule&lt;Config&gt; once the planner
     * moves to its own Gradle module. The OpenSearch monorepo injects -proc:none globally, blocking
     * the Immutables annotation processor required by RelRule.Config sub-interfaces.
     * <p>TODO: add SortPushdown rule here — pushes Sort below Exchange to data nodes for top-K
     * optimization.
     */
    private static RelNode mark(RelNode input, PlannerContext context, RuleProfilingListener listener) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addRuleCollection(
            List.of(
                new OpenSearchTableScanRule(context),
                new OpenSearchFilterRule(context),
                new OpenSearchProjectRule(context),
                new OpenSearchAggregateRule(context),
                new OpenSearchJoinRule(context),
                new OpenSearchSortRule(context),
                new OpenSearchUnionRule(context),
                new OpenSearchValuesRule(context)
            )
        );
        HepPlanner planner = new HepPlanner(builder.build());
        if (listener != null) {
            planner.addListener(listener);
            listener.beginPhase("marking");
        }
        try {
            planner.setRoot(input);
            return planner.findBestExp();
        } finally {
            if (listener != null) listener.endPhase("marking");
        }
    }

    /** Phase 2: VolcanoPlanner for trait propagation + exchange insertion. */
    private static RelNode cbo(RelNode marked, RelNode rawRelNode, PlannerContext context, RuleProfilingListener listener) {
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        OpenSearchDistributionTraitDef distTraitDef = context.getDistributionTraitDef();
        volcanoPlanner.addRelTraitDef(distTraitDef);
        volcanoPlanner.addRule(new OpenSearchAggregateSplitRule(context));
        volcanoPlanner.addRule(new OpenSearchSortSplitRule(context));
        volcanoPlanner.addRule(new OpenSearchJoinSplitRule(context));
        volcanoPlanner.addRule(new OpenSearchUnionSplitRule(context));
        volcanoPlanner.addRule(new OpenSearchDistributionDeriveRule(context));
        volcanoPlanner.addRule(AbstractConverter.ExpandConversionRule.INSTANCE);

        if (listener != null) {
            volcanoPlanner.addListener(listener);
            listener.beginPhase("cbo");
        }
        try {
            RelOptCluster volcanoCluster = RelOptCluster.create(volcanoPlanner, rawRelNode.getCluster().getRexBuilder());
            volcanoCluster.setMetadataQuerySupplier(RelMetadataQuery::instance);

            // TODO: eliminate this copy
            RelNode copied = RelNodeUtils.copyToCluster(marked, volcanoCluster, distTraitDef);

            // Root demands SINGLETON with null locality — satisfied by either SHARD+SINGLETON
            // (1-shard scan, no ER) or COORDINATOR+SINGLETON (after ER). Multi-shard scans stamp
            // RANDOM → ER inserted by ExpandConversionRule + trait def's convert(). Single-shard
            // scans stamp SHARD+SINGLETON → already satisfies, no top ER.
            volcanoPlanner.setRoot(copied);
            RelTraitSet desiredTraits = copied.getTraitSet().replace(distTraitDef.anySingleton());
            if (!copied.getTraitSet().equals(desiredTraits)) {
                volcanoPlanner.setRoot(volcanoPlanner.changeTraits(copied, desiredTraits));
            }
            return volcanoPlanner.findBestExp();
        } finally {
            if (listener != null) listener.endPhase("cbo");
        }
    }
}

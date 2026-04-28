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
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchFilterRule;
import org.opensearch.analytics.planner.rules.OpenSearchProjectRule;
import org.opensearch.analytics.planner.rules.OpenSearchSortRule;
import org.opensearch.analytics.planner.rules.OpenSearchTableScanRule;

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
        return markAndOptimize(rawRelNode, context);
    }

    /**
     * Phase 1 (RBO marking) + Phase 2 (CBO exchange insertion).
     * Package-private so planner rule tests can inspect the marked+optimized tree.
     */
    static RelNode markAndOptimize(RelNode rawRelNode, PlannerContext context) {
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(rawRelNode));

        // Phase 1: RBO — pre-marking logical optimizations then marking rules, single HepPlanner
        HepProgramBuilder hepBuilder = new HepProgramBuilder();

        // Pre-marking: reduce constant expressions before marking rules fire.
        // TODO: establish a FrontEnd API contract specifying which standard Calcite optimizations
        // frontends apply themselves before submitting a RelNode. Rules already applied by the
        // frontend should not be re-added here — re-applying them increases overall planning time.
        hepBuilder.addMatchOrder(HepMatchOrder.ARBITRARY);
        hepBuilder.addRuleCollection(
            List.of(
                new ReduceExpressionsRule.FilterReduceExpressionsRule(Filter.class, RelBuilder.proto(Contexts.empty())),
                new ReduceExpressionsRule.ProjectReduceExpressionsRule(Project.class, RelBuilder.proto(Contexts.empty()))
            )
        );

        // Marking: convert LogicalXxx → OpenSearchXxx bottom-up
        // TODO: migrate rules from deprecated RelOptRule to RelRule<Config> once the planner
        // moves to its own Gradle module. The OpenSearch monorepo injects -proc:none globally,
        // blocking the Immutables annotation processor required by RelRule.Config sub-interfaces.
        // TODO: add SortPushdown rule here — pushes Sort below Exchange to data nodes for top-K
        // optimization. When Sort is pushed to data nodes above a partial aggregate, FragmentConversionDriver
        // must call convertShardScanFragment → attachPartialAggOnTop → attachFragmentOnTop(Sort) in sequence.
        hepBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        hepBuilder.addRuleCollection(
            List.of(
                new OpenSearchTableScanRule(context),
                new OpenSearchFilterRule(context),
                new OpenSearchProjectRule(context),
                new OpenSearchAggregateRule(context),
                new OpenSearchSortRule(context)
            )
        );

        HepPlanner markingPlanner = new HepPlanner(hepBuilder.build());
        markingPlanner.setRoot(rawRelNode);
        RelNode marked = markingPlanner.findBestExp();

        LOGGER.info("After marking:\n{}", RelOptUtil.toString(marked));

        // Phase 2: CBO — VolcanoPlanner for trait propagation + exchange insertion
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        OpenSearchDistributionTraitDef distTraitDef = context.getDistributionTraitDef();
        volcanoPlanner.addRelTraitDef(distTraitDef);
        volcanoPlanner.addRule(new OpenSearchAggregateSplitRule(context));
        volcanoPlanner.addRule(AbstractConverter.ExpandConversionRule.INSTANCE);

        RelOptCluster volcanoCluster = RelOptCluster.create(volcanoPlanner, rawRelNode.getCluster().getRexBuilder());
        volcanoCluster.setMetadataQuerySupplier(RelMetadataQuery::instance);

        // TODO: eliminate this copy
        RelNode copied = RelNodeUtils.copyToCluster(marked, volcanoCluster, distTraitDef);

        // Root must be SINGLETON — coordinator gathers all results
        volcanoPlanner.setRoot(copied);
        RelTraitSet desiredTraits = copied.getTraitSet().replace(distTraitDef.singleton());
        if (!copied.getTraitSet().equals(desiredTraits)) {
            volcanoPlanner.setRoot(volcanoPlanner.changeTraits(copied, desiredTraits));
        }
        RelNode result = volcanoPlanner.findBestExp();

        LOGGER.info("After CBO:\n{}", RelOptUtil.toString(result));
        return result;
    }
}

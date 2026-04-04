package org.opensearch.analytics.planner;

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
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchFilterRule;
import org.opensearch.analytics.planner.rules.OpenSearchProjectRule;
import org.opensearch.analytics.planner.rules.OpenSearchSortRule;
import org.opensearch.analytics.planner.rules.OpenSearchTableScanRule;

import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.QueryDAG;

import java.util.List;

/**
 * Central planner for the Analytics Plugin.
 *
 * <p>Three phases:
 * <ol>
 *   <li>HepPlanner (RBO): converts LogicalXxx → OpenSearchXxx with backend
 *       assignment, predicate annotation, and distribution traits.</li>
 *   <li>VolcanoPlanner (CBO): requests SINGLETON at root (coordinator must
 *       gather all results). Split rule fires on aggregates, Volcano inserts
 *       exchanges via trait enforcement where distribution mismatches.</li>
 *   <li>DAG construction: cuts at exchange boundaries, builds stage tree.</li>
 * </ol>
 *
 * <p>TODO: eliminate copyToCluster — have frontends create RelNodes with Volcano cluster.
 * <p>TODO: Per-stage plan forking (multiple plan generation)
 * <p>TODO: Fragment conversion (backend.convertFragment)
 * <p>TODO: Join strategy selection, sort removal via CBO
 *
 * @opensearch.internal
 */
public class PlannerImpl {

    private static final Logger LOGGER = LogManager.getLogger(PlannerImpl.class);

    public static RelNode createPlan(RelNode rawRelNode, PlannerContext context) {
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(rawRelNode));

        // Phase 1: RBO — convert LogicalXxx → OpenSearchXxx
        HepProgramBuilder markingBuilder = new HepProgramBuilder();
        markingBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        // Rules as a collection so HEP tries all rules at each node in bottom-up
        // tree order. This ensures children are marked before parents regardless
        // of tree shape (e.g. filter above aggregate, aggregate above filter).
        markingBuilder.addRuleCollection(List.of(
            new OpenSearchTableScanRule(context),
            new OpenSearchFilterRule(context),
            new OpenSearchProjectRule(context),
            new OpenSearchAggregateRule(context),
            new OpenSearchSortRule(context)
        ));

        HepPlanner markingPlanner = new HepPlanner(markingBuilder.build());
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

        RelOptCluster volcanoCluster = RelOptCluster.create(volcanoPlanner,
            rawRelNode.getCluster().getRexBuilder());
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

        // Phase 3: DAG construction — cut at exchange boundaries
        QueryDAG dag = DAGBuilder.build(result);
        LOGGER.info("QueryDAG:\n{}", dag);

        // Phase 4: Plan forking — generate per-stage alternatives
        PlanForker.forkAll(dag);
        LOGGER.info("After plan forking:\n{}", dag);

        return result;
    }
}

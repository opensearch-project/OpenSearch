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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rules.ExtractLiteralAggRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggLiteralArgProjectSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateReduceRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchDistinctCountRule;
import org.opensearch.analytics.planner.rules.OpenSearchDistributionDeriveRule;
import org.opensearch.analytics.planner.rules.OpenSearchFilterRule;
import org.opensearch.analytics.planner.rules.OpenSearchJoinRule;
import org.opensearch.analytics.planner.rules.OpenSearchJoinSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchLateMaterializationRewriter;
import org.opensearch.analytics.planner.rules.OpenSearchProjectRule;
import org.opensearch.analytics.planner.rules.OpenSearchSortPushdownRewriter;
import org.opensearch.analytics.planner.rules.OpenSearchSortRule;
import org.opensearch.analytics.planner.rules.OpenSearchSortSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchTableScanRule;
import org.opensearch.analytics.planner.rules.OpenSearchTopKRewriter;
import org.opensearch.analytics.planner.rules.OpenSearchUnionRule;
import org.opensearch.analytics.planner.rules.OpenSearchUnionSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchValuesRule;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Optional;

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
 * <p>TODO: Join strategy selection, sort removal via CBO
 *
 * @opensearch.internal
 */
public class PlannerImpl {

    private static final Logger LOGGER = LogManager.getLogger(PlannerImpl.class);

    /**
     * Like {@link CoreRules#FILTER_PROJECT_TRANSPOSE} but refuses to push a Filter below a Project
     * that computes any non-deterministic expression (e.g. {@code eval r = rand() | where r > 0}).
     *
     * <p>This is a <b>semantic-correctness</b> guard, not a delegation/performance one. The stock
     * rule only guards against window functions ({@code !containsOver()}); pushing a Filter past a
     * {@code rand()} Project inlines {@code RAND()} into the predicate, so a reference to one
     * already-computed random value ({@code $ref > literal}) becomes a fresh {@code RAND() > literal}
     * evaluated again at scan time. That re-evaluates / duplicates the non-deterministic expression
     * and changes results, so the Filter must stay above the Project regardless of backend.
     *
     * <p>(Aside: such an inlined {@code RAND() > literal} predicate is also what gets incorrectly
     * marked Lucene-delegation-viable today — a separate filter-viability gap where a field-less
     * predicate inherits its child's viable backends instead of validating its scalar calls. That is
     * tracked/fixed separately; it is not the reason for this rule.)
     */
    private static final FilterProjectTransposeRule FILTER_PROJECT_TRANSPOSE_DETERMINISTIC = FilterProjectTransposeRule.Config.DEFAULT
        .withOperandFor(
            Filter.class,
            filter -> true,
            Project.class,
            project -> project.getProjects().stream().allMatch(RexUtil::isDeterministic)
        ).toRule();

    public static RelNode createPlan(RelNode rawRelNode, PlannerContext context) {
        return runAllOptimizations(rawRelNode, context);
    }

    /**
     * Phase 1 (RBO marking) + Phase 2 (CBO exchange insertion).
     * Package-private so planner rule tests can inspect the marked+optimized tree.
     */
    public static RelNode runAllOptimizations(RelNode rawRelNode, PlannerContext context) {
        RelNodeUtils.logPlan(LOGGER, "Input RelNode", rawRelNode);

        RuleProfilingListener listener = context.isProfilingEnabled() ? new RuleProfilingListener() : null;

        RelNode modifiedRelNode = rawRelNode;
        modifiedRelNode = removeSubQueries(modifiedRelNode, listener);
        modifiedRelNode = trimFields(modifiedRelNode);
        modifiedRelNode = extractLiteralAgg(modifiedRelNode, listener);
        modifiedRelNode = reduceExpressions(modifiedRelNode, listener);
        modifiedRelNode = pushdownRules(modifiedRelNode, listener);
        modifiedRelNode = decomposeAggregates(modifiedRelNode, listener);
        modifiedRelNode = mark(modifiedRelNode, context, listener);
        RelNodeUtils.logPlan(LOGGER, "After marking", modifiedRelNode);
        modifiedRelNode = splitAggLiteralArgProject(modifiedRelNode, listener);
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
        RelNodeUtils.logPlan(LOGGER, "After CBO", modifiedRelNode);
        Optional<RelNode> lateMat = OpenSearchLateMaterializationRewriter.rewrite(modifiedRelNode);
        if (lateMat.isPresent()) {
            modifiedRelNode = lateMat.get();
            RelNodeUtils.logPlan(LOGGER, "After late-materialization", modifiedRelNode);
        }
        Optional<RelNode> topK = OpenSearchTopKRewriter.rewrite(modifiedRelNode, context);
        if (topK.isPresent()) {
            modifiedRelNode = topK.get();
            RelNodeUtils.logPlan(LOGGER, "After TopK rewrite", modifiedRelNode);
        }
        Optional<RelNode> sortPushdown = OpenSearchSortPushdownRewriter.rewrite(modifiedRelNode);
        if (sortPushdown.isPresent()) {
            modifiedRelNode = sortPushdown.get();
            RelNodeUtils.logPlan(LOGGER, "After sort pushdown", modifiedRelNode);
        }

        if (listener != null) {
            RuleProfilingListener.PlannerProfile profile = listener.snapshot();
            context.recordProfilingResults(profile);
            LOGGER.info("Planner profile for raw RelNode is :\n{}", profile.format());
        }
        return modifiedRelNode;
    }

    /**
     * Phase 0: lower {@link org.apache.calcite.rex.RexSubQuery}s (EXISTS / IN / SOME / ANY,
     * including the PPL {@code subsearch} shapes that the frontend lowers to them) into
     * {@code LogicalCorrelate} via the three {@code *_SUB_QUERY_TO_CORRELATE} rules, then
     * decorrelate back to a standard join shape. Without this phase, downstream rules see
     * a {@code RexSubQuery} inside a filter / project predicate and either reject the
     * operator outright (e.g. {@link org.opensearch.analytics.planner.rules.OpenSearchFilterRule}
     * resolves leaf predicates through a {@code ScalarFunction} table that doesn't and
     * shouldn't cover {@code EXISTS}) or carry the un-removed subquery into substrait
     * emission. Runs first so every later phase observes a subquery-free tree.
     */
    private static RelNode removeSubQueries(RelNode input, RuleProfilingListener listener) {
        // The PPL frontend injects a SUBSEARCH_MAXOUT Sort(fetch=N) at the top of every subsearch.
        // Inside an EXISTS that limit is semantically irrelevant (existence needs only one row), but
        // it becomes a correlated Sort(fetch>1) after FILTER_SUB_QUERY_TO_CORRELATE, which
        // RelDecorrelator refuses to decorrelate (it only handles fetch==1) — leaving a
        // LogicalCorrelate that the marking phase rejects with "unmarked child [LogicalCorrelate]".
        // Strip that limit while the subquery is still an identifiable EXISTS RexSubQuery so the
        // decorrelation below can fold it into a standard join.
        RelNode prepared = stripExistsSubqueryLimits(input);
        return HepPhase.named("subquery-remove")
            .addRuleCollection(
                List.of(
                    CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                    CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                    CoreRules.JOIN_SUB_QUERY_TO_CORRELATE
                )
            )
            // RexSubQuery removal introduces LogicalCorrelate; decorrelate back to a
            // straight join shape that the marking + capability rules already handle.
            // RelDecorrelator is a visitor (not a RelOptRule) so it runs as a
            // post-processing step inside the same listener phase.
            .postProcess(
                withCorrelates -> RelDecorrelator.decorrelateQuery(
                    withCorrelates,
                    RelBuilder.proto(Contexts.empty()).create(prepared.getCluster(), null)
                )
            )
            .run(prepared, listener);
    }

    /**
     * Removes a top-level fetch-only {@link Sort} (no collation, no offset) from the body of every
     * {@code EXISTS} {@link RexSubQuery} in the tree. The PPL frontend injects a SUBSEARCH_MAXOUT
     * {@code Sort(fetch=N)} at the top of each subsearch; for an EXISTS that limit cannot change the
     * boolean result (it only tests for ≥1 row), yet it blocks {@link RelDecorrelator} from
     * decorrelating the correlated subquery. Scoped to EXISTS only — IN / scalar subqueries keep
     * their limit, where it is semantically meaningful.
     */
    private static RelNode stripExistsSubqueryLimits(RelNode input) {
        RexShuttle rexShuttle = new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                RexSubQuery rewritten = (RexSubQuery) super.visitSubQuery(subQuery);
                if (rewritten.getOperator().getKind() == SqlKind.EXISTS) {
                    RelNode body = stripExistsSubqueryLimits(rewritten.rel);
                    RelNode unlimited = stripTopFetchOnlySort(body);
                    if (unlimited != rewritten.rel) {
                        return rewritten.clone(unlimited);
                    }
                    if (body != rewritten.rel) {
                        return rewritten.clone(body);
                    }
                }
                return rewritten;
            }
        };
        RelShuttle relShuttle = new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode node) {
                RelNode visited = super.visit(node);
                return visited.accept(rexShuttle);
            }
        };
        return input.accept(relShuttle);
    }

    /**
     * If {@code node} is a {@link Sort} that only limits row count (a {@code fetch} with no sort
     * keys and no {@code offset}), returns its input; otherwise returns {@code node} unchanged. A
     * sort with collation or an offset is preserved — dropping either could change which rows the
     * EXISTS sees relative to a correlated predicate.
     */
    private static RelNode stripTopFetchOnlySort(RelNode node) {
        if (node instanceof Sort sort && sort.getCollation().getFieldCollations().isEmpty() && sort.offset == null && sort.fetch != null) {
            return sort.getInput();
        }
        return node;
    }

    /**
     * Phase 0b: lower {@code LITERAL_AGG(literal)} aggregate calls into an
     * {@code Aggregate + Project} shape via {@link ExtractLiteralAggRule}.
     *
     * <p>{@code LITERAL_AGG} is a Calcite-internal aggregate that Calcite expects
     * each backend to implement natively (Calcite's own Interpreter and Enumerable
     * codegen do; the SQL JDBC implementor inlines the literal directly into the
     * SQL output). DataFusion has no equivalent UDAF, so we lower it away before
     * the marking phase observes the plan.
     *
     * <p>{@code SubQueryRemoveRule}'s {@code NOT IN} / {@code SOME} / {@code ALL}
     * rewrites are the only Calcite source of {@code LITERAL_AGG} that we know
     * of, but the rule is kept as its own pass — independent of the subquery
     * removal phase — for two reasons: (1) phase listing reflects each
     * concern separately so profiling output stays meaningful, (2) the rule
     * still fires correctly if a future frontend / pushdown rule constructs
     * {@code LITERAL_AGG} directly via {@code RelBuilder.literalAgg(...)}.
     */
    private static RelNode extractLiteralAgg(RelNode input, RuleProfilingListener listener) {
        return HepPhase.named("literal-agg-extract").addRuleInstance(new ExtractLiteralAggRule()).run(input, listener);
    }

    /**
     * Phase 1c': duplicate an aggregate's literal-config-arg Project (e.g. percentile's {@code 50})
     * into a pinned upper copy (literal stays with the aggregate) over an unpinned physical-only
     * lower copy (pushes below the ExchangeReducer). Runs AFTER marking — operates on
     * {@code OpenSearch*} nodes and emits a pinned {@code OpenSearchProject} whose
     * {@code computeSelfCost} forces the CBO-inserted ER below it, keeping the literal in the
     * coordinator fragment for the DataFusion substrait converter. Placed after marking so the
     * pre-marking {@code PROJECT_MERGE} cannot re-fuse the two copies. See
     * {@link OpenSearchAggLiteralArgProjectSplitRule}.
     */
    private static RelNode splitAggLiteralArgProject(RelNode input, RuleProfilingListener listener) {
        return HepPhase.named("agg-literal-arg-split").addRuleInstance(new OpenSearchAggLiteralArgProjectSplitRule()).run(input, listener);
    }

    /**
     * Phase 1a: constant-expression reduction on Filter and Project predicates. Kept in
     * its own phase so {@code ProjectReduceExpressionsRule} cannot use a downstream
     * Filter's predicates (introduced by the pushdown phase in Phase 1b) to rewrite
     * Project expressions. Co-locating the reducer with the transposes lets Calcite
     * simplify e.g. {@code m = (int0 <= 4)} into {@code m = IS NOT NULL(int0)} once the
     * Filter sits below the Project — semantically correct but emits operators the
     * Project may not have backend support for.
     */
    private static RelNode reduceExpressions(RelNode input, RuleProfilingListener listener) {
        return HepPhase.named("reduce-expressions")
            .bottomUp()
            .addRuleCollection(
                List.of(
                    new ReduceExpressionsRule.FilterReduceExpressionsRule(Filter.class, RelBuilder.proto(Contexts.empty())),
                    new ReduceExpressionsRule.ProjectReduceExpressionsRule(Project.class, RelBuilder.proto(Contexts.empty()))
                )
            )
            .run(input, listener);
    }

    /**
     * Phase 1b: Filter pushdown past Project / Aggregate / Join via Calcite's transpose rules.
     * Pre-marking placement keeps marking single-pass — transposes emit plain {@code Logical*}
     * nodes, then marking lowers the canonical post-pushdown shape in one go.
     */
    private static RelNode pushdownRules(RelNode input, RuleProfilingListener listener) {
        return HepPhase.named("pushdown-rules")
            .bottomUp()
            // Transposes (filter-into-*) cascade together within one fixpoint, alongside
            // PROJECT_MERGE which collapses adjacent Projects. FILTER_MERGE runs as its own
            // instruction so it only fires after the transposes have settled — that way any
            // auto-injected NOT NULL collapses with the user's WHERE on the post-pushdown filter,
            // not on a half-pushed intermediate.
            //
            // SORT_PROJECT_TRANSPOSE is intentionally omitted: lifting Project above Sort puts it
            // above the Exchange, defeating projection pushdown. Keeping it below lets DataFusion
            // prune the scan. QTF relocates the below-Sort Project above its wrapper itself.
            //
            // SORT_REMOVE_REDUNDANT drops a Sort/LIMIT whose input is provably bounded to
            // within the limit (e.g. a collation-less `head N` or a sort over a scalar
            // aggregate, getMaxRowCount <= 1): a no-op that the marking rule must not have to
            // special-case. Runs here, pre-marking, on plain Logical* so marking stays a pure
            // Logical* -> OpenSearch* conversion. Cascades with LIMIT_MERGE in the same
            // fixpoint so stacked limits collapse first, then any now-redundant Sort is removed.
            .addRuleCollection(
                List.of(
                    FILTER_PROJECT_TRANSPOSE_DETERMINISTIC,
                    CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                    CoreRules.FILTER_INTO_JOIN,
                    CoreRules.PROJECT_MERGE,
                    CoreRules.LIMIT_MERGE,
                    CoreRules.SORT_REMOVE_REDUNDANT
                )
            )
            .addRuleInstance(CoreRules.FILTER_MERGE)
            .run(input, listener);
    }

    /**
     * Phase 1b: pre-marking rewrites on plain {@link org.apache.calcite.rel.logical.LogicalAggregate}.
     * Runs before {@link OpenSearchAggregateRule} marks the aggregate so the marking phase, the
     * Volcano split rule, and the {@code DistributedAggregateRewriter} see the rewritten shape:
     * <ul>
     *   <li>{@link OpenSearchDistinctCountRule} — single-arg {@code COUNT(DISTINCT x)} →
     *       {@code APPROX_COUNT_DISTINCT(x)} so distinct counts engage the engine-native
     *       HLL sketch merge instead of additive SUM-of-counts.</li>
     *   <li>{@link OpenSearchAggregateReduceRule} — {@code AVG} / {@code STDDEV} / {@code VAR} →
     *       primitive {@code SUM} / {@code COUNT} (+ {@code SUM_SQ} for variance) plus a scalar
     *       {@link org.apache.calcite.rel.logical.LogicalProject} computing the quotient.</li>
     * </ul>
     */
    private static RelNode decomposeAggregates(RelNode input, RuleProfilingListener listener) {
        return HepPhase.named("aggregate-decompose")
            .bottomUp()
            .addRuleInstance(new OpenSearchDistinctCountRule())
            .addRuleInstance(new OpenSearchAggregateReduceRule())
            .run(input, listener);
    }

    /**
     * Invokes Calcite's {@link RelFieldTrimmer} to slim each node to only the columns its consumer
     * needs, inserting a narrowing Project above the scan so DataFusion prunes the parquet read.
     */
    static RelNode trimFields(RelNode input) {
        RelBuilder relBuilder = RelBuilder.proto(Contexts.empty()).create(input.getCluster(), null);
        // Trimming is a pure optimization; the untrimmed tree is always a correct fallback. The
        // trimmer's stock handlers reject some valid shapes via IllegalArgumentException (e.g.
        // RelBuilder.sortLimit on a non-literal OFFSET) — fall back rather than fail the query.
        try {
            return new RelFieldTrimmer(null, relBuilder).trim(input);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("RelFieldTrimmer skipped (falling back to untrimmed tree): {}", e.toString());
            return input;
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
        return HepPhase.named("marking")
            .bottomUp()
            .addRuleCollection(
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
            )
            .run(input, listener);
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
            RelNode best = volcanoPlanner.findBestExp();
            if (LOGGER.isDebugEnabled()) {
                StringWriter sw = new StringWriter();
                volcanoPlanner.dump(new PrintWriter(sw));
                LOGGER.debug("Volcano memo:\n{}", sw);
            }
            return best;
        } finally {
            if (listener != null) listener.endPhase("cbo");
        }
    }
}

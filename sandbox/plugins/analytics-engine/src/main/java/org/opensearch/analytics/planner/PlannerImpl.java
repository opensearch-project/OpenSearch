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
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
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
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rules.ExtractLiteralAggRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggLiteralArgProjectSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateReduceRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateRule;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchBroadcastJoinSplitRule;
import org.opensearch.analytics.planner.rules.OpenSearchDistinctCountRule;
import org.opensearch.analytics.planner.rules.OpenSearchDistributionDeriveRule;
import org.opensearch.analytics.planner.rules.OpenSearchFilterRule;
import org.opensearch.analytics.planner.rules.OpenSearchHashJoinSplitRule;
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
        LOGGER.debug("Input RelNode:\n{}", RelOptUtil.toString(rawRelNode));

        RuleProfilingListener listener = context.isProfilingEnabled() ? new RuleProfilingListener() : null;

        RelNode modifiedRelNode = rawRelNode;
        modifiedRelNode = removeSubQueries(modifiedRelNode, listener);
        modifiedRelNode = extractLiteralAgg(modifiedRelNode, listener);
        modifiedRelNode = reduceExpressions(modifiedRelNode, listener);
        modifiedRelNode = pushdownRules(modifiedRelNode, listener);
        modifiedRelNode = decomposeAggregates(modifiedRelNode, listener);
        modifiedRelNode = reorderJoins(modifiedRelNode, context, listener);
        modifiedRelNode = trimUnusedFields(modifiedRelNode, context);
        modifiedRelNode = mark(modifiedRelNode, context, listener);
        LOGGER.debug("After marking:\n{}", RelOptUtil.toString(modifiedRelNode));
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
        LOGGER.debug("After CBO:\n{}", RelOptUtil.toString(modifiedRelNode));
        Optional<RelNode> lateMat = OpenSearchLateMaterializationRewriter.rewrite(modifiedRelNode);
        if (lateMat.isPresent()) {
            modifiedRelNode = lateMat.get();
            LOGGER.debug("After late-materialization:\n{}", RelOptUtil.toString(modifiedRelNode));
        }
        Optional<RelNode> topK = OpenSearchTopKRewriter.rewrite(modifiedRelNode, context);
        if (topK.isPresent()) {
            modifiedRelNode = topK.get();
            LOGGER.debug("After TopK rewrite:\n{}", RelOptUtil.toString(modifiedRelNode));
        }
        Optional<RelNode> sortPushdown = OpenSearchSortPushdownRewriter.rewrite(modifiedRelNode);
        if (sortPushdown.isPresent()) {
            modifiedRelNode = sortPushdown.get();
            LOGGER.debug("After sort pushdown:\n{}", RelOptUtil.toString(modifiedRelNode));
        }

        if (listener != null) {
            RuleProfilingListener.PlannerProfile profile = listener.snapshot();
            context.recordProfilingResults(profile);
            LOGGER.info("Planner profile for raw RelNode is :\n{}", profile.format());
        }
        return modifiedRelNode;
    }

    /**
     * Pre-marking cost-based join reordering for multi-way joins. Collapses the frontend's
     * left-deep {@code LogicalJoin} tree into a single {@code MultiJoin} ({@code JOIN_TO_MULTI_JOIN}),
     * then re-orders it with Calcite's bushy-join heuristic ({@code MULTI_JOIN_OPTIMIZE_BUSHY}), which
     * ranks join factors by {@code RelMetadataQuery.getRowCount} — the per-index counts seeded by
     * {@code IndexRowCountFetcher}. The effect: the smaller/more-selective joins run first, so a fat
     * fact-table intermediate is not carried through every downstream join (and not re-shuffled at
     * each worker tier). This is the plan-layer analog of the column-prune win — less data moved by
     * construction, not by a bigger memory ceiling.
     *
     * <p>Runs here (pre-marking, on {@code Logical*}) for the same reason as {@link #trimUnusedFields}:
     * the reorder rules match {@code LogicalProject(MultiJoin)} / {@code LogicalJoin}, and marking lowers
     * the reordered shape in one pass. Placed BEFORE the trimmer so the trimmer prunes the final order.
     *
     * <p><b>The two rules run as SEPARATE HEP instructions</b> — {@code JOIN_TO_MULTI_JOIN} to fixpoint,
     * THEN {@code MULTI_JOIN_OPTIMIZE_BUSHY} to fixpoint. Running them in one rule collection loops
     * indefinitely (the optimize rule's {@code Join} output re-triggers {@code JOIN_TO_MULTI_JOIN}); as
     * ordered instructions the flatten completes once and the optimizer consumes its {@code MultiJoin}
     * without re-flattening (the documented deferral hazard at the old {@code reduceExpressions} TODO).
     *
     * <p><b>Gated</b> by {@link AnalyticsSettings#MPP_JOIN_REORDER} (default {@code false}) AND to plans
     * with 3+ joins that are ALL equi-joins — a 2-way join has a single order (nothing to reorder), and
     * a cross-join (PPL {@code transpose}) must not be flattened into a {@code MultiJoin}. A reorder-rule
     * edge case falls back to the input plan rather than failing the query.
     */
    private static RelNode reorderJoins(RelNode input, PlannerContext context, RuleProfilingListener listener) {
        if (!AnalyticsSettings.MPP_JOIN_REORDER.get(context.getSettings())) {
            return input;
        }
        // 3+ PURE-equi joins only: fewer than 3 has a single order; and every join must be a pure equi-join
        // (isEqui() = no residual non-equi conjunct). A cross-join (no keys) or a mixed equi+theta condition
        // (e.g. a.x=b.x AND a.y>b.y) must NOT be flattened into a MultiJoin — the bushy rule is narrow around
        // condition shape, and a leftKeys-non-empty-but-not-pure-equi join would slip a theta predicate into
        // the reorder. (Same spirit as the trimUnusedFields gate, tightened to isEqui.)
        List<org.apache.calcite.rel.core.Join> joins = RelNodeUtils.findNodes(input, org.apache.calcite.rel.core.Join.class);
        boolean reorderable = joins.size() >= 3 && joins.stream().allMatch(j -> j.analyzeCondition().isEqui());
        if (!reorderable) {
            return input;
        }
        // Exclude plans carrying a Correlate or a non-INNER join. JOIN_TO_MULTI_JOIN flattens inner joins
        // into a MultiJoin, but MULTI_JOIN_OPTIMIZE_BUSHY only re-expands a MultiJoin it can fully reorder;
        // an EXISTS/NOT-EXISTS subquery (LogicalCorrelate, or a semi/anti join once decorrelated — TPC-H
        // q21/q11) leaves a residual MultiJoin that no bushy match consumes, and marking then rejects the
        // unmarked MultiJoin ("Filter rule encountered unmarked child [MultiJoin]"). Skip those shapes.
        if (!RelNodeUtils.findNodes(input, org.apache.calcite.rel.core.Correlate.class).isEmpty()
            || joins.stream().anyMatch(j -> j.getJoinType() != org.apache.calcite.rel.core.JoinRelType.INNER)) {
            return input;
        }
        try {
            RelNode reordered = HepPhase.named("join-reorder")
                .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
                .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY)
                .run(input, listener);
            // Belt-and-suspenders: the optimize rule must leave ZERO MultiJoin nodes — a residual one is a
            // deferred failure (marking throws on it, which the try/catch here can't see because the reorder
            // phase itself didn't throw). If any survived, discard the reorder and keep the as-written tree.
            if (!RelNodeUtils.findNodes(reordered, org.apache.calcite.rel.rules.MultiJoin.class).isEmpty()) {
                LOGGER.debug("Join reorder left a residual MultiJoin; falling back to as-written order");
                return input;
            }
            LOGGER.debug("After join reorder:\n{}", RelOptUtil.toString(reordered));
            return reordered;
        } catch (Exception | AssertionError e) {
            // Defensive: a reorder-rule edge case must not fail planning — Calcite can assert-fail (not just
            // throw a RuntimeException) on an unsupported condition shape when assertions are enabled. Fall
            // back to the as-written order; correctness is unaffected, only the ordering win is lost. The
            // returned `input` is the original, unmutated tree (HepPlanner builds a fresh output).
            LOGGER.debug("Join reorder skipped (fell back to as-written order): {}", e.toString());
            return input;
        }
    }

    /**
     * Pre-marking column pruning. Runs Calcite's {@link RelFieldTrimmer} on the still-plain
     * {@code Logical*} plan (before {@link #mark} lowers it to {@code OpenSearch*}) so columns no
     * operator references are dropped at the source: the trimmer inserts narrowing Projects and
     * remaps every {@code RexInputRef} / aggregate-arg / join-condition index itself. Trimmed columns
     * then never enter marking, CBO, the distribution-enforcement pass, OR the hash-shuffle wire —
     * which for an aggregate-over-join (TPC-H q5/q9) is the difference between shuffling the full
     * join width and shuffling only the group keys + aggregate inputs.
     *
     * <p>Doing this here (pre-marking, on {@code Logical*}) rather than as post-enforcement surgery on
     * an {@code OpenSearchShuffleExchange} avoids hand-remapping a downstream join's input indices +
     * per-column {@code FieldStorageInfo} — the trimmer is the battle-tested Calcite path for exactly
     * that remapping.
     *
     * <p>Constructed like Calcite's own {@code Programs.trim()}: a {@code null} validator (our rels
     * arrive already-validated from the frontend) + a logical {@link RelBuilder}. Guarded: a trimmer
     * edge case falls back to the untrimmed plan rather than failing the query.
     *
     * <p><b>SCOPED TO JOIN PLANS.</b> The byte-width win is the wide multi-input shuffle (a join ships
     * its full output width when only the downstream-referenced columns are needed — TPC-H q5/q9 shrink
     * 5-25x). Single-input plans (scan/filter/window/union/bare-aggregate) gain nothing here — scan
     * column-pruning is already a NATIVE concern (DataFusion reads only the columns the query touches
     * regardless of a Project above the scan), so trimming them only churns plan shapes (and the
     * plan-shape golden tests) for no runtime benefit. Gating on "plan has joins and they are ALL
     * EQUI-joins" confines the rewrite to exactly the distributable shapes that benefit — and excludes
     * any plan containing a trivial CROSS JOIN (e.g. the one PPL {@code transpose} lowers to, which
     * never distributes and which the whole-tree trimmer would mis-rewrite even from a sibling arm).
     * Gated by the {@code analytics.mpp.shuffle.prune_columns} cluster setting
     * ({@link AnalyticsSettings#MPP_SHUFFLE_PRUNE_COLUMNS}, default {@code true}).
     */
    private static RelNode trimUnusedFields(RelNode input, PlannerContext context) {
        if (!AnalyticsSettings.MPP_SHUFFLE_PRUNE_COLUMNS.get(context.getSettings())) {
            return input;
        }
        // Confine the trim to plans that (a) contain at least one join and (b) whose joins are ALL
        // equi-joins. Rationale:
        // - Only an equi-join's hash-shuffle ships full width, so only join plans gain (scan/filter/
        // window/union/bare-agg get native scan-pruning and would only churn shapes).
        // - The "ALL equi" requirement (not "ANY") is load-bearing for CORRECTNESS. RelFieldTrimmer
        // rewrites the WHOLE tree, not a subtree, so a plan that mixes a real equi-join with a CROSS
        // JOIN (condition=[true]) — e.g. PPL `transpose`, which lowers to a cross-join wrapped in
        // ROW_NUMBER windows + FILTER aggregates — would have its delicate cross-join branch
        // mis-rewritten into a valid-but-WRONG plan (no exception → the try/catch below would NOT
        // catch it; extensive-coverage q82 first surfaced the pure-transpose case). Requiring every
        // join to carry equi-keys excludes any plan containing such a cross-join, sibling arm or not.
        // A cross-join never distributes, so excluding it loses no byte-width win.
        List<org.apache.calcite.rel.core.Join> joins = RelNodeUtils.findNodes(input, org.apache.calcite.rel.core.Join.class);
        boolean allEquiJoins = !joins.isEmpty() && joins.stream().allMatch(j -> !j.analyzeCondition().leftKeys.isEmpty());
        if (!allEquiJoins) {
            return input;
        }
        try {
            RelBuilder relBuilder = RelBuilder.proto(Contexts.empty()).create(input.getCluster(), null);
            RelNode trimmed = new RelFieldTrimmer(null, relBuilder).trim(input);
            LOGGER.debug("After field trimming:\n{}", RelOptUtil.toString(trimmed));
            return trimmed;
        } catch (RuntimeException e) {
            // Defensive: a RelFieldTrimmer edge case (an unsupported rel shape) must not fail planning.
            // Fall back to the untrimmed plan — correctness is unaffected, only the byte-width win is lost.
            LOGGER.debug("Field trimming skipped (fell back to untrimmed plan): {}", e.toString());
            return input;
        }
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
        // NOTE: join reordering does NOT run here — running JOIN_TO_MULTI_JOIN + MULTI_JOIN_OPTIMIZE_BUSHY
        // in the same ARBITRARY pass loops indefinitely (they invert each other). It lives in its own
        // dedicated phase, {@link #reorderJoins}, which runs them as two separate fixpoint instructions.
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
        volcanoPlanner.addRule(new OpenSearchBroadcastJoinSplitRule(context));
        volcanoPlanner.addRule(new OpenSearchHashJoinSplitRule(context));
        volcanoPlanner.addRule(new OpenSearchUnionSplitRule(context));
        volcanoPlanner.addRule(new OpenSearchDistributionDeriveRule(context));
        volcanoPlanner.addRule(AbstractConverter.ExpandConversionRule.INSTANCE);

        if (listener != null) {
            volcanoPlanner.addListener(listener);
            listener.beginPhase("cbo");
        }
        try {
            RelOptCluster volcanoCluster = RelOptCluster.create(volcanoPlanner, rawRelNode.getCluster().getRexBuilder());
            // Use our metadata query so OpenSearchJoin gets a PK-FK row-count estimate instead of
            // Calcite's no-stats cartesian × 0.15 over-estimate. The subclass overrides only
            // getRowCount; every other metadata def falls through to the default handler chain.
            volcanoCluster.setMetadataQuerySupplier(OpenSearchRelMetadataQuery::new);

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
            // NB: do NOT log volcanoPlanner.dump() here — it runs Dumpers.dumpGraphviz, whose
            // PartiallyOrderedSet build is O(memo^2+) and takes MINUTES for a multi-way join (a 6-way join's
            // memo hangs the query purely in the debug dump; findBestExp already returned). The chosen plan
            // is already rendered by the "After CBO" DEBUG line in runAllOptimizations, so no dump is needed.
            return best;
        } finally {
            if (listener != null) listener.endPhase("cbo");
        }
    }
}

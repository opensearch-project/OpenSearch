/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;
import org.opensearch.analytics.spi.RuntimeFilterFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decides which shuffle joins get a runtime filter, plants the probe predicate, and builds the pre-pass
 * that computes the filter.
 *
 * <p>{@link #plan} is pure analysis and answers "for this plan, which build side should summarise which
 * key, and which probe-side stage should test which column". Every correctness rule that decides whether
 * a filter is legal lives there, separately from the plan surgery, so it can be asserted directly. The
 * payload that eventually satisfies a planted predicate is {@link ShuffleRuntimeFilterPayload}'s job.
 *
 * <p>This is the shuffle family, where the filter is <em>not</em> free. Measurement on the sf=100
 * cluster showed the win comes from dropping probe rows before they enter the shuffle (roughly 3.4x
 * on a five-way join whose cost is shipping the whole fact table), while the cost is a second scan of
 * the build table's key column — that table is already scanned as a shuffle producer in the same
 * query. Hence the row gate: above it, the pre-pass cannot repay itself. Spark guards the same cost
 * with a creation-side threshold and abandons the filter rather than paying.
 *
 * @opensearch.internal
 */
public final class ShuffleRuntimeFilters {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleRuntimeFilters.class);

    private ShuffleRuntimeFilters() {}

    /**
     * One planned filter: which stage summarises which column, and which stage tests which column.
     *
     * <p>Columns are named rather than numbered on purpose. A key's ordinal is relative to the
     * <em>join input's</em> row type, and a Project between the scan and the join shifts it; resolving
     * by name against the producer fragment is the only mapping that survives that.
     *
     * @param filterId        identifies the filter in both the plan predicate and the payload instruction
     * @param buildStageId    producer stage whose subtree is summarised
     * @param buildKeyColumn  column in the build producer's output to summarise
     * @param probeStageId    producer stage whose rows are filtered
     * @param probeKeyColumn  column in the probe producer's scan to test
     * @param bloomBytes      filter size for THIS filter, sized from the build side's estimated
     *                        cardinality; identical for every contribution so the union is defined
     */
    public record Descriptor(int filterId, int buildStageId, String buildKeyColumn, int probeStageId, String probeKeyColumn,
        int bloomBytes) {
    }

    /**
     * Plans a filter for every eligible shuffle join in {@code dag}.
     *
     * <p>Must run on the DAG <em>before</em> the shuffle promotion, because the predicate this enables
     * has to be planted before fragment conversion, and because the pre-rewrite DAG is where a join's
     * two shuffle inputs still name their producer stages.
     *
     * <p>Every rejection is silent and logged at debug: a plan with no filters is the plan as it runs
     * today.
     *
     * @param maxBuildRows estimated build-side OUTPUT rows above which the pre-pass is not worth running;
     *                     {@code <= 0} disables the shuffle family entirely
     * @param minProbeScanBytes estimated scan bytes the filtered side must reach for a filter to be worth
     *                          its cost at all
     */
    public static List<Descriptor> plan(QueryDAG dag, int maxBloomBytes, long maxBuildRows, long minProbeScanBytes) {
        if (dag == null || maxBloomBytes <= 0 || maxBuildRows <= 0) {
            return List.of();
        }
        // Per-query budget, not just per filter: a per-filter cap bounds nothing when the filter count is
        // unbounded. Measured: five filters at the ceiling exhausted the native pool before computing anything.
        long remainingBudget = (long) maxBloomBytes * MAX_FILTERS_PER_QUERY;
        Map<Integer, Stage> byId = new HashMap<>();
        indexStages(dag.rootStage(), byId);

        List<Descriptor> descriptors = new ArrayList<>();
        int nextFilterId = 0;
        for (Stage stage : byId.values().stream().sorted((a, b) -> Integer.compare(a.getStageId(), b.getStageId())).toList()) {
            OpenSearchJoin join = joinOverTwoShuffles(stage.getFragment());
            if (join == null) {
                continue;
            }
            if (remainingBudget <= 0) {
                LOGGER.debug("[runtime-filter] per-query filter budget exhausted after {} filter(s)", descriptors.size());
                break;
            }
            int ceilingForThisFilter = (int) Math.min(maxBloomBytes, remainingBudget);
            Descriptor descriptor = planForJoin(join, byId, nextFilterId, ceilingForThisFilter, maxBuildRows, minProbeScanBytes);
            if (descriptor != null) {
                descriptors.add(descriptor);
                remainingBudget -= descriptor.bloomBytes();
                nextFilterId++;
            }
        }
        return descriptors;
    }

    private static Descriptor planForJoin(
        OpenSearchJoin join,
        Map<Integer, Stage> byId,
        int filterId,
        int maxBloomBytes,
        long maxBuildRows,
        long minProbeScanBytes
    ) {
        // Summarise the SMALLER input and filter the larger, independently of the join's own build/probe
        // assignment. A positional convention instead picked the fact table to summarise on every multi-way
        // join, so the row gate refused all of them and the feature fired on nothing.
        //
        // Sizes come from the PRODUCER stages: a join input is ShuffleExchange(StageInputScan) with no table
        // scan, so measuring it reports the same for both sides and the choice degenerates to "input 0".
        Integer stage0 = producerStageId(join.getInput(0));
        Integer stage1 = producerStageId(join.getInput(1));
        if (stage0 == null || stage1 == null) {
            return null;
        }
        Stage producer0 = byId.get(stage0);
        Stage producer1 = byId.get(stage1);
        if (producer0 == null || producer1 == null) {
            return null;
        }
        long rows0 = summarisableRows(producer0);
        long rows1 = summarisableRows(producer1);
        final int buildIdx = rows0 <= rows1 ? 0 : 1;
        final int probeIdx = 1 - buildIdx;
        if (!RuntimeFilterEligibility.canFilterProbeSide(join.getJoinType(), probeIdx)) {
            LOGGER.debug("[runtime-filter] join type {} preserves input {}; a filter there would drop rows", join.getJoinType(), probeIdx);
            return null;
        }

        int buildStageId = buildIdx == 0 ? stage0 : stage1;
        int probeStageId = probeIdx == 0 ? stage0 : stage1;
        Stage buildStage = buildIdx == 0 ? producer0 : producer1;
        Stage probeStage = probeIdx == 0 ? producer0 : producer1;
        // Refuse a build side whose pre-pass would need the shuffle promotion: a pre-pass mini-DAG runs
        // fork / adapt / select / convert only, so a copied subtree still holding a distributed join reaches
        // a data node without its shuffle instructions and computes nothing. Reachable because an
        // intermediate producer reports unbounded rows, so two intermediates still resolve to input 0.
        // Lifting this means running the promotion for the mini-DAG, not relaxing the check.
        if (summarisableRows(buildStage) == Long.MAX_VALUE || joinOverTwoShuffles(buildStage.getFragment()) != null) {
            LOGGER.debug(
                "[runtime-filter] build stage {} is an intermediate or contains a distributed join; its "
                    + "pre-pass would need the shuffle promotion, which the pre-pass pipeline does not run",
                buildStageId
            );
            return null;
        }
        // A multi-level subtree is allowed, provided the pre-pass gets its OWN copy (see deepCopy): sharing
        // a Stage would race two concurrent executions on the instructions attached to it. What must be
        // bounded is cost, so the gate below covers everything the pre-pass reads, not just what it emits.
        long prePassScanRows = subtreeScanRows(buildStage);
        if (prePassScanRows > maxBuildRows) {
            LOGGER.debug(
                "[runtime-filter] the pre-pass for build stage {} would scan an estimated {} rows across {} "
                    + "stage(s), over the {} gate",
                buildStageId,
                prePassScanRows,
                stageCount(buildStage),
                maxBuildRows
            );
            return null;
        }

        JoinInfo info = join.analyzeCondition();
        if (info.leftKeys.isEmpty()) {
            // Pure-theta join: no equi key to summarise.
            return null;
        }

        // Gate on estimated OUTPUT rows, not scanned rows: selectivity is what decides whether the extra
        // scan is worth it. A dimension scanning 20M rows to emit 400k is a cheap, useful filter that a
        // scan-row gate would refuse.
        long buildRows = estimatedOutputRows(buildStage, buildIdx == 0 ? rows0 : rows1);
        if (buildRows > maxBuildRows) {
            LOGGER.debug(
                "[runtime-filter] build stage {} estimated to emit {} rows, over the {} gate; " + "the pre-pass would not repay itself",
                buildStageId,
                buildRows,
                maxBuildRows
            );
            return null;
        }

        // Only the FIRST equi key. A composite key needs either one filter per column (each weaker than the
        // conjunction) or a tuple hash both sides agree on exactly.
        // leftKeys index into input 0 and rightKeys into input 1, so which list is the build side's follows
        // from the size choice above, not from the lists' names.
        int buildKeyOrdinal = (buildIdx == 0 ? info.leftKeys : info.rightKeys).get(0);
        int probeKeyOrdinal = (probeIdx == 0 ? info.leftKeys : info.rightKeys).get(0);
        String buildKeyColumn = fieldName(join.getInput(buildIdx), buildKeyOrdinal);
        String probeKeyColumn = fieldName(join.getInput(probeIdx), probeKeyOrdinal);
        if (buildKeyColumn == null || probeKeyColumn == null) {
            return null;
        }
        // Refuse a key the backend's signatures cannot accept. Unlike every other refusal here, skipping it
        // does not cost an optimization but the query: the probe predicate goes into the MAIN plan, where an
        // unresolvable predicate is fatal rather than open.
        RelDataType buildKeyType = keyType(join.getInput(buildIdx), buildKeyOrdinal);
        RelDataType probeKeyType = keyType(join.getInput(probeIdx), probeKeyOrdinal);
        if (!RuntimeFilterEligibility.isSupportedFilterKeyType(buildKeyType)
            || !RuntimeFilterEligibility.isSupportedFilterKeyType(probeKeyType)) {
            LOGGER.debug(
                "[runtime-filter] join key types ({} / {}) are not both filterable; no filter",
                buildKeyType == null ? "?" : buildKeyType.getSqlTypeName(),
                probeKeyType == null ? "?" : probeKeyType.getSqlTypeName()
            );
            return null;
        }
        // The predicate must sit above a scan that binds the column, which the probe producer often is not:
        // in a left-deep tree every join above the innermost has one intermediate input, with no scan of its
        // own. So the application point is searched for DOWNWARD.
        Stage target = probeTarget(probeStage, probeKeyColumn);
        if (target == null) {
            LOGGER.debug("[runtime-filter] no scan below probe stage {} can carry '{}' soundly; no filter", probeStageId, probeKeyColumn);
            return null;
        }
        if (!producerExposes(buildStage.getFragment(), buildKeyColumn)) {
            LOGGER.debug("[runtime-filter] build stage {} does not expose '{}'; no filter", buildStageId, buildKeyColumn);
            return null;
        }
        // Gate in BYTES, not rows. Measured: a filtered side of 80M rows pays, one of 60M narrower rows on a
        // tenth-scale cluster does not. No row threshold separates those two; their scan sizes differ by an
        // order of magnitude.
        long probeScanBytes = estimatedScanBytes(target);
        if (probeScanBytes < minProbeScanBytes) {
            LOGGER.debug(
                "[runtime-filter] probe stage {} scans an estimated {} bytes, under the {} floor; "
                    + "removing rows from it would not repay the pre-pass",
                target.getStageId(),
                probeScanBytes,
                minProbeScanBytes
            );
            return null;
        }

        Descriptor descriptor = new Descriptor(
            filterId,
            buildStageId,
            buildKeyColumn,
            target.getStageId(),
            probeKeyColumn,
            bloomBytesFor(buildRows, maxBloomBytes)
        );
        LOGGER.debug("[runtime-filter] planned {}", descriptor);
        return descriptor;
    }

    // ── Mutation ─────────────────────────────────────────────────────────

    /**
     * Stage-id base for pre-pass stages, chosen far above any id {@code DAGBuilder} allocates so a
     * pre-pass can never be mistaken for a stage of the query proper in a log or a metric.
     */
    private static final int PRE_PASS_STAGE_ID_BASE = 100_000;

    /**
     * A DAG with probe predicates planted, and the descriptors whose predicate actually landed.
     *
     * <p>The two are reported together because a planned filter is not necessarily a planted one — an
     * ambiguous placement drops it — and running a pre-pass for a filter nothing probes is pure cost.
     * Only the descriptors named here are worth computing a payload for.
     */
    public record Planted(QueryDAG dag, List<Descriptor> descriptors) {
    }

    /**
     * Plants each descriptor's probe predicate, returning the rebuilt DAG and the descriptors planted.
     *
     * <p>Must run before the shuffle promotion: that pipeline rebuilds every stage's plan alternatives
     * from its fragment, so a predicate planted afterwards would never reach a converted plan — and one
     * planted before is picked up for free.
     *
     * <p>The predicate goes directly above the target stage's table scan. The fragment is shard-local, so
     * any position in it drops the same rows before the shuffle; above the scan is simply the earliest,
     * which means the fewest rows through whatever sits on top.
     *
     * <p>One stage can be the target of several filters — a fact table joined to two dimensions is filtered
     * by both — so the predicates are planted as a chain, each above the previous one's input scan.
     */
    public static Planted plantProbePredicates(QueryDAG dag, List<Descriptor> descriptors) {
        if (dag == null || descriptors.isEmpty()) {
            return new Planted(dag, List.of());
        }
        // A list per stage: two joins over the same fact table resolve to the SAME leaf, so keying by stage
        // id alone would keep the last and silently drop the rest.
        Map<Integer, List<Descriptor>> byProbeStage = new HashMap<>();
        for (Descriptor d : descriptors) {
            byProbeStage.computeIfAbsent(d.probeStageId(), k -> new ArrayList<>()).add(d);
        }
        List<Descriptor> planted = new ArrayList<>(descriptors.size());
        Stage root = plantInto(dag.rootStage(), byProbeStage, planted);
        QueryDAG rebuilt = root == dag.rootStage() ? dag : new QueryDAG(dag.queryId(), root);
        return new Planted(rebuilt, planted);
    }

    private static Stage plantInto(Stage stage, Map<Integer, List<Descriptor>> byProbeStage, List<Descriptor> planted) {
        List<Stage> children = new ArrayList<>(stage.getChildStages().size());
        boolean changed = false;
        for (Stage child : stage.getChildStages()) {
            Stage rebuilt = plantInto(child, byProbeStage, planted);
            children.add(rebuilt);
            changed |= rebuilt != child;
        }

        RelNode fragment = stage.getFragment();
        for (Descriptor descriptor : byProbeStage.getOrDefault(stage.getStageId(), List.of())) {
            if (fragment == null) {
                break;
            }
            RelNode withPredicate = insertProbeFilter(fragment, descriptor);
            if (withPredicate != null) {
                fragment = withPredicate;
                changed = true;
                planted.add(descriptor);
                LOGGER.debug(
                    "[runtime-filter] planted os_runtime_filter({}, {}) on stage {}",
                    descriptor.filterId(),
                    descriptor.probeKeyColumn(),
                    stage.getStageId()
                );
            }
        }
        return changed ? UnifiedDispatch.copyStage(stage, fragment, children, stage.getPlanAlternatives()) : stage;
    }

    /**
     * Wraps the fragment's table scan in a filter carrying the probe predicate, or {@code null} when it
     * cannot be placed.
     *
     * <p>Requires exactly one scan exposing the column. Two would make the placement ambiguous — the
     * predicate belongs to one specific side of the join — and guessing there could filter the wrong
     * scan, so the filter is dropped instead.
     */
    private static RelNode insertProbeFilter(RelNode fragment, Descriptor descriptor) {
        List<OpenSearchTableScan> candidates = new ArrayList<>();
        for (OpenSearchTableScan scan : RelNodeUtils.findNodes(fragment, OpenSearchTableScan.class)) {
            if (scan.getRowType().getFieldNames().contains(descriptor.probeKeyColumn())) {
                candidates.add(scan);
            }
        }
        if (candidates.size() != 1) {
            LOGGER.debug(
                "[runtime-filter] {} scans expose '{}' in the probe fragment; placement is ambiguous, no filter",
                candidates.size(),
                descriptor.probeKeyColumn()
            );
            return null;
        }
        OpenSearchTableScan scan = candidates.get(0);
        int ordinal = scan.getRowType().getFieldNames().indexOf(descriptor.probeKeyColumn());
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
        RexNode key = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(ordinal).getType(), ordinal);
        RelNode filter = new OpenSearchFilter(
            scan.getCluster(),
            scan.getTraitSet(),
            scan,
            RuntimeFilterFunction.makeProbeCall(rexBuilder, descriptor.filterId(), key),
            ((OpenSearchRelNode) scan).getViableBackends()
        );
        return replaceNode(fragment, scan, filter);
    }

    /** Rebuilds {@code root} with {@code target} replaced by {@code replacement}. */
    private static RelNode replaceNode(RelNode root, RelNode target, RelNode replacement) {
        if (root == target) {
            return replacement;
        }
        RelNode unwrapped = RelNodeUtils.unwrapHep(root);
        List<RelNode> inputs = new ArrayList<>(unwrapped.getInputs().size());
        boolean changed = false;
        for (RelNode input : unwrapped.getInputs()) {
            RelNode rebuilt = replaceNode(RelNodeUtils.unwrapHep(input), target, replacement);
            inputs.add(rebuilt);
            changed |= rebuilt != RelNodeUtils.unwrapHep(input);
        }
        return changed ? unwrapped.copy(unwrapped.getTraitSet(), inputs) : root;
    }

    /**
     * A standalone mini-DAG that computes one descriptor's build-side contribution.
     *
     * <p>Standalone rather than a stage added to the query's own DAG. Adding it there would mean the
     * shuffle promotion sees a join stage with a third child, and it would be converted only inside that
     * promotion — too late, because the payload has to exist before the main dispatch and running a stage
     * is asynchronous. A separate mini-DAG is converted and run on its own schedule, and the query's DAG
     * is never perturbed, so there is nothing to strip afterwards either.
     *
     * <p>Shape is {@code Aggregate(os_bloom_agg($0, $1))} over {@code Project(key, sizeLiteral)} over the
     * build producer's own fragment. The Project exists because an {@code AggregateCall}'s arguments are
     * field ordinals, not expressions, so a literal argument has to be materialised as a constant column
     * — the same convention the engine already uses for an aggregate's literal config arguments.
     *
     * @return the mini-DAG, or {@code null} when the shape cannot be built
     */
    public static QueryDAG prePassDag(String queryId, Stage buildStage, Descriptor descriptor) {
        RelNode buildFragment = buildStage.getFragment();
        if (buildFragment == null) {
            return null;
        }
        List<String> names = buildFragment.getRowType().getFieldNames();
        int keyOrdinal = names.indexOf(descriptor.buildKeyColumn());
        if (keyOrdinal < 0) {
            return null;
        }
        RelOptCluster cluster = buildFragment.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        List<String> viableBackends = ((OpenSearchRelNode) RelNodeUtils.unwrapHep(buildFragment)).getViableBackends();

        RexNode key = rexBuilder.makeInputRef(buildFragment.getRowType().getFieldList().get(keyOrdinal).getType(), keyOrdinal);
        RexNode size = RuntimeFilterFunction.makeSizeLiteral(rexBuilder, descriptor.bloomBytes());
        RelDataType projectType = typeFactory.builder()
            .add(descriptor.buildKeyColumn(), key.getType())
            .add("__rf_bytes", size.getType())
            .build();
        RelNode project = new OpenSearchProject(
            cluster,
            buildFragment.getTraitSet(),
            buildFragment,
            List.of(key, size),
            projectType,
            viableBackends
        );

        AggregateCall bloomAgg = AggregateCall.create(
            RuntimeFilterFunction.BLOOM_AGG,
            /* distinct */ false,
            /* approximate */ false,
            /* ignoreNulls */ false,
            List.of(),
            List.of(0, 1),
            /* filterArg */ -1,
            /* distinctKeys */ null,
            RelCollations.EMPTY,
            // A global aggregate over no group keys: the one group IS the empty group, which is what
            // decides whether the return type is nullable.
            /* hasEmptyGroup */ true,
            project,
            /* type */ null,
            "__rf_bloom"
        );
        RelNode aggregate = new OpenSearchAggregate(
            cluster,
            project.getTraitSet(),
            project,
            ImmutableBitSet.of(),
            List.of(ImmutableBitSet.of()),
            List.of(bloomAgg),
            AggregateMode.SINGLE,
            viableBackends,
            Map.of()
        );

        Stage prePass = new Stage(
            PRE_PASS_STAGE_ID_BASE + descriptor.filterId(),
            aggregate,
            buildStage.getChildStages().stream().map(ShuffleRuntimeFilters::deepCopy).toList(),
            ExchangeInfo.singleton(),
            /* exchangeSinkProvider */ null,
            buildStage.getTargetResolver()
        );
        return new QueryDAG(queryId, prePass);
    }

    /** The join in this fragment whose both inputs are shuffle exchanges, or null. */
    static OpenSearchJoin joinOverTwoShuffles(RelNode fragment) {
        if (fragment == null) {
            return null;
        }
        for (OpenSearchJoin join : RelNodeUtils.findNodes(fragment, OpenSearchJoin.class)) {
            if (join.getInputs().size() != 2) {
                continue;
            }
            boolean bothShuffles = RelNodeUtils.unwrapHep(join.getInput(0)) instanceof OpenSearchShuffleExchange
                && RelNodeUtils.unwrapHep(join.getInput(1)) instanceof OpenSearchShuffleExchange;
            if (bothShuffles) {
                return join;
            }
        }
        return null;
    }

    /**
     * Total estimated scan rows the pre-pass would read across a build subtree.
     *
     * <p>Summed rather than maximised, unlike {@code ShuffleEnrichment.subtreeMaxScanRows}: the question
     * here is what a second execution of the whole subtree costs, and every table in it is read.
     */
    private static long subtreeScanRows(Stage stage) {
        long total = stage.getFragment() == null ? 0L : ShuffleEnrichment.subtreeMaxScanRows(stage.getFragment());
        for (Stage child : stage.getChildStages()) {
            total = Math.addExact(total, subtreeScanRows(child));
        }
        return total;
    }

    /** Stages in this subtree, for the log line that explains a refusal. */
    private static int stageCount(Stage stage) {
        int count = 1;
        for (Stage child : stage.getChildStages()) {
            count += stageCount(child);
        }
        return count;
    }

    /**
     * A private copy of a stage subtree, sharing no {@link Stage} object with the original.
     *
     * <p>This is what makes a multi-level pre-pass safe. Fragments are shared deliberately — a
     * {@code RelNode} is immutable, and copying one would be pointless — but every {@code Stage} is new,
     * because a Stage carries mutable execution state: plan alternatives, a role, and the instructions the
     * shuffle enrichment attaches. Two graphs sharing one Stage would race on exactly those.
     *
     * <p>Stage ids are preserved rather than renumbered. Renumbering would require rewriting every
     * {@code OpenSearchStageInputScan}'s child-stage reference inside the fragments, and it buys nothing:
     * the mini-DAG runs under its own query id, and shuffle buffers are keyed by query id and stage id
     * together, so the same stage id under a different query cannot collide.
     *
     * <p>Plan alternatives are deliberately NOT copied — the four plan-side passes repopulate them for the
     * mini-DAG, and carrying the query's own converted plans across would be both stale and confusing.
     */
    private static Stage deepCopy(Stage stage) {
        List<Stage> children = new ArrayList<>(stage.getChildStages().size());
        for (Stage child : stage.getChildStages()) {
            children.add(deepCopy(child));
        }
        return UnifiedDispatch.copyStage(stage, stage.getFragment(), children, List.of());
    }

    /**
     * Filters a single query may build, bounding total payload together with the per-filter ceiling.
     *
     * <p>Ten, matching Spark's {@code runtimeFilter.number.threshold}, which exists for the same reason
     * Spark states plainly: to keep a query with many joins from exhausting memory with Bloom filters. The
     * measured failure here was the same in kind — five filters at the per-filter ceiling was enough.
     */
    private static final int MAX_FILTERS_PER_QUERY = 10;

    /** Bits of filter per distinct key, targeting roughly a 1% false-positive rate. */
    private static final double BITS_PER_KEY = 10.0;

    /**
     * Floor, so a tiny build side still gets a filter worth probing rather than a few blocks. Subordinate
     * to the ceiling: an operator who caps below this gets the cap.
     */
    private static final int MIN_BLOOM_BYTES = 64 * 1024;

    /**
     * Smallest bitset the backend can represent — the parquet split-block Bloom's {@code
     * BITSET_MIN_LENGTH}, one block of eight 32-bit words. A request under it is raised to it there, so
     * mirroring the value here keeps the size this class derives equal to the size actually allocated.
     */
    private static final int BACKEND_MIN_BITSET_BYTES = 32;

    /**
     * Filter size for a build side estimated to emit {@code buildRows} rows, capped at
     * {@code maxBloomBytes}.
     *
     * <p>Sized from the estimate rather than fixed, because a fixed size fails silently in one
     * direction. Measured on sf=100: a build side of 22.7M keys in the previous fixed 1 MiB is about
     * <b>2.7 bits per key</b>, where the false-positive rate approaches 1 — the filter admitted nearly
     * every row and the query gained 3.9%, inside noise. The same query with ~11 bits per key gained
     * <b>78%</b>. Nothing about the plumbing differed; only the size.
     *
     * <p>Rows are used as a proxy for distinct keys, which over-sizes a filter whose key repeats. That
     * is the safe direction: too many bits costs payload, too few costs the entire optimization.
     *
     * <p>Public so the sizing rule can be asserted directly on numbers. It is the one part of this
     * feature where being wrong produces no error and no result — only a filter that quietly keeps
     * everything — so it is worth pinning at the boundaries rather than only through a planned query.
     *
     * <p><b>Where this departs from Spark, deliberately.</b> Spark keeps its runtime Bloom near 1 MiB
     * ({@code numBits} 8388608, sized for {@code expectedNumItems} 1M and clamped by
     * {@code maxNumItems} 4M), and that is coherent only because {@code creationSideThreshold} refuses
     * to build a filter from a build side above ~10 MB at all. Applied here, Spark's rule would decline
     * the query measured at 78% and forgo the win. Sizing to the build side buys it, at a payload cost
     * the cap bounds.
     */
    public static int bloomBytesFor(long buildRows, int maxBloomBytes) {
        long requested = (long) Math.ceil(Math.max(buildRows, 1L) * BITS_PER_KEY / 8.0);
        // Floor first, ceiling last: the ceiling is the operator's explicit limit and must hold even when it
        // sits below the floor.
        long floored = Math.max(requested, MIN_BLOOM_BYTES);
        // Return what the backend will ACTUALLY allocate — the request rounded to a power of two — or the
        // ceiling is applied to a size that never exists. Up when it still fits, because rounding down
        // halves the bits per key; only a breach falls back to the largest power of two under the ceiling.
        long rounded = ceilingPowerOfTwo(floored);
        long capped = rounded <= maxBloomBytes ? rounded : Long.highestOneBit(maxBloomBytes);
        // Below the backend's hard minimum no bitset exists at all, so a ceiling set under it cannot be
        // honoured by anything and that minimum wins.
        return (int) Math.max(capped, BACKEND_MIN_BITSET_BYTES);
    }

    /** {@code value} when it is already a power of two, else the next one above it. */
    private static long ceilingPowerOfTwo(long value) {
        long highest = Long.highestOneBit(value);
        return highest == value ? value : highest << 1;
    }

    /**
     * Estimated bytes the filtered side reads, as {@code scan rows × estimated row width}.
     *
     * <p>Width comes from Calcite's row-size metadata, so a wide fact row counts for more than a narrow
     * dimension row of the same count — which is the whole point of measuring bytes. When the metadata is
     * unavailable the width falls back to 8 bytes per column, an underestimate that can only refuse a
     * filter rather than admit one, keeping an unknown estimate on the safe side of the gate.
     */
    private static long estimatedScanBytes(Stage target) {
        RelNode fragment = target.getFragment();
        if (fragment == null) {
            return 0L;
        }
        long rows = ShuffleEnrichment.subtreeMaxScanRows(fragment);
        double width;
        try {
            Double metadataWidth = fragment.getCluster().getMetadataQuery().getAverageRowSize(fragment);
            width = metadataWidth != null && !metadataWidth.isNaN() && metadataWidth > 0
                ? metadataWidth
                : 8.0 * fragment.getRowType().getFieldCount();
        } catch (RuntimeException e) {
            width = 8.0 * fragment.getRowType().getFieldCount();
        }
        // Saturating: a wide estimate over a large table can overflow, and an overflowed negative would
        // read as "tiny" and pass a gate meant to refuse it.
        double bytes = rows * width;
        return bytes >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) bytes;
    }

    /**
     * Estimated rows this producer emits, which is what decides whether summarising it is useful.
     *
     * <p>Falls back to {@code scanRows} when Calcite has no metadata for the fragment. The fallback is the
     * conservative direction: a scan count is never smaller than an output count, so an unknown estimate can
     * only refuse a filter, never admit one the gate would have rejected.
     */
    private static long estimatedOutputRows(Stage producer, long scanRows) {
        RelNode fragment = producer.getFragment();
        if (fragment == null) {
            return scanRows;
        }
        try {
            Double rows = fragment.getCluster().getMetadataQuery().getRowCount(fragment);
            if (rows != null && !rows.isNaN() && rows >= 0) {
                return Math.min(scanRows, (long) Math.ceil(rows));
            }
        } catch (RuntimeException e) {
            LOGGER.debug("[runtime-filter] no row-count metadata for stage {}; using its scan count", producer.getStageId(), e);
        }
        return scanRows;
    }

    /**
     * Cost of summarising this producer, in rows — the quantity the "summarise the smaller side" choice
     * compares.
     *
     * <p>A producer whose fragment holds no table scan is an <em>intermediate</em>: it consumes another
     * stage's output through a stage-input scan. Reported by scan rows it measures zero, which made it win
     * the comparison every time and then be refused as a non-leaf, so the filter was never built from the
     * dimension sitting opposite it. It is reported as unbounded instead, because the question being asked
     * is "what would a second scan of this cost", and an intermediate cannot be scanned a second time at
     * all.
     */
    private static long summarisableRows(Stage producer) {
        if (producer.getFragment() == null || RelNodeUtils.findNodes(producer.getFragment(), OpenSearchTableScan.class).isEmpty()) {
            return Long.MAX_VALUE;
        }
        return ShuffleEnrichment.subtreeMaxScanRows(producer.getFragment());
    }

    /** Producer stage id behind a join input's shuffle exchange, or null if the shape differs. */
    private static Integer producerStageId(RelNode joinInput) {
        if (!(RelNodeUtils.unwrapHep(joinInput) instanceof OpenSearchShuffleExchange shuffle)) {
            return null;
        }
        RelNode inner = RelNodeUtils.unwrapHep(shuffle.getInput(0));
        return inner instanceof OpenSearchStageInputScan scan ? scan.getChildStageId() : null;
    }

    private static String fieldName(RelNode input, int ordinal) {
        List<RelDataTypeField> fields = input.getRowType().getFieldList();
        return ordinal >= 0 && ordinal < fields.size() ? fields.get(ordinal).getName() : null;
    }

    /** The declared type of an input's key column, or {@code null} when the ordinal is out of range. */
    private static RelDataType keyType(RelNode input, int ordinal) {
        List<RelDataTypeField> fields = input.getRowType().getFieldList();
        return ordinal >= 0 && ordinal < fields.size() ? fields.get(ordinal).getType() : null;
    }

    /**
     * The stage whose table scan should carry the probe predicate for {@code column}, or {@code null} when
     * no sound application point exists.
     *
     * <p>Returns {@code probeProducer} itself when its own scan binds the column. Otherwise descends into
     * its child producers, because that is where the column comes from: filtering the fact table at its own
     * leaf removes rows from <em>every</em> shuffle above it, not just the one join being considered.
     *
     * <p><b>Why moving the predicate down is sound at all.</b> The predicate says "this key is absent from
     * the other side of the join", and a row it rejects cannot survive that join wherever it is dropped. So
     * dropping it earlier changes nothing — <em>provided</em> nothing between the leaf and the join depends
     * on the row's presence. {@link #transparentToKeyPushdown} is that proviso, and it is deliberately a
     * whitelist: an operator nobody has reasoned about blocks the descent rather than being assumed safe.
     */
    private static Stage probeTarget(Stage probeProducer, String column) {
        List<Stage> candidates = new ArrayList<>();
        collectProbeTargets(probeProducer, column, candidates);
        if (candidates.size() == 1) {
            return candidates.get(0);
        }
        // Zero means the column is renamed, computed, or behind an operator the descent will not cross.
        // More than one means the name is ambiguous: an intermediate can carry a `status` from two
        // different tables, and the join key physically comes from exactly one of them. Filtering the
        // wrong table by the right name drops rows that belong in the result, so the only safe answer
        // when the name does not identify a single scan is no filter.
        if (candidates.size() > 1) {
            LOGGER.debug("[runtime-filter] '{}' resolves to {} scans below the probe side; ambiguous", column, candidates.size());
        }
        return null;
    }

    private static void collectProbeTargets(Stage stage, String column, List<Stage> out) {
        if (stage.getFragment() == null || !transparentToKeyPushdown(stage.getFragment())) {
            return;
        }
        if (!RuntimeFilterEligibility.keyPassesThroughProjectsUnchanged(stage.getFragment(), column)) {
            LOGGER.debug("[runtime-filter] '{}' is computed by a project in stage {}; no application point", column, stage.getStageId());
            return;
        }
        if (scanExposes(stage.getFragment(), column)) {
            out.add(stage);
            // No descent past a stage that binds the column itself: its own scan is the application point,
            // and anything below it reaches this stage through that scan.
            return;
        }
        for (Stage child : stage.getChildStages()) {
            collectProbeTargets(child, column, out);
        }
    }

    /**
     * True when dropping rows below this fragment cannot change what the fragment produces, beyond
     * removing rows the join above would have discarded anyway.
     *
     * <p>Each rejection is a real hazard, not caution for its own sake:
     * <ul>
     *   <li>{@link OpenSearchAggregate} — an aggregate over fewer input rows is a different value. Even
     *       {@code count(*)} changes, and a runtime filter must never change a result.</li>
     *   <li>{@link OpenSearchSort} — a sort carrying a limit selects <em>which</em> rows survive, so
     *       removing candidates changes the survivors. A limit is exactly the case where "the join would
     *       have discarded it anyway" stops being true, because the row's presence displaced another.</li>
     *   <li>{@link OpenSearchUnion} — the column would have to be filtered on every branch to be filtered
     *       at all; planting on one branch and not another silently filters half the input.</li>
     *   <li>A non-{@code INNER} join — an outer join null-extends its preserved side, so a row removed
     *       below it takes its preserved partner's output row with it. {@code SEMI} / {@code ANTI} are
     *       likewise sensitive to whether a match exists, not just to which rows survive.</li>
     * </ul>
     */
    private static boolean transparentToKeyPushdown(RelNode fragment) {
        if (!RelNodeUtils.findNodes(fragment, OpenSearchAggregate.class).isEmpty()) {
            return false;
        }
        if (!RelNodeUtils.findNodes(fragment, OpenSearchSort.class).isEmpty()) {
            return false;
        }
        if (!RelNodeUtils.findNodes(fragment, OpenSearchUnion.class).isEmpty()) {
            return false;
        }
        for (OpenSearchJoin join : RelNodeUtils.findNodes(fragment, OpenSearchJoin.class)) {
            if (join.getJoinType() != JoinRelType.INNER) {
                return false;
            }
        }
        return true;
    }

    /** True when some table scan in this fragment exposes {@code column}. */
    private static boolean scanExposes(RelNode fragment, String column) {
        if (fragment == null) {
            return false;
        }
        for (OpenSearchTableScan scan : RelNodeUtils.findNodes(fragment, OpenSearchTableScan.class)) {
            if (scan.getRowType().getFieldNames().contains(column)) {
                return true;
            }
        }
        return false;
    }

    /** True when the producer fragment's own output carries {@code column}. */
    private static boolean producerExposes(RelNode fragment, String column) {
        return fragment != null && fragment.getRowType().getFieldNames().contains(column);
    }

    private static void indexStages(Stage stage, Map<Integer, Stage> out) {
        if (stage == null) {
            return;
        }
        out.put(stage.getStageId(), stage);
        for (Stage child : stage.getChildStages()) {
            indexStages(child, out);
        }
    }
}

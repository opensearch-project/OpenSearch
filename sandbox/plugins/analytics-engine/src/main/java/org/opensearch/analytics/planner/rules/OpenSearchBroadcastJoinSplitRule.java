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
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

/**
 * Broadcast-join split rule (M2). Sibling of {@link OpenSearchJoinSplitRule} (coord-centric)
 * and {@link OpenSearchHashJoinSplitRule}. Fires on {@link OpenSearchJoin}; emits a
 * broadcast-shape alternative where one side (the build) is replicated to every probe-side
 * worker via {@link org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange} and
 * the other side (the probe) keeps its SHARD+RANDOM trait. The join itself runs at
 * SHARD+RANDOM alongside the probe scan; CBO ranks broadcast vs hash via the cost model
 * (broadcast cost = build_rows × probe_nodes; hash cost = total_rows + setup × N).
 *
 * <p>For INNER joins, emits two alternatives (left-as-build, right-as-build) and lets cost
 * decide which side to broadcast. For LEFT/RIGHT/FULL outer joins, only the row-preserved
 * side can be probe; the other must be build. SEMI/ANTI: build is always the right side.
 *
 * <p>Gates:
 * <ul>
 *   <li>{@code analytics.mpp.enabled} must be true (master MPP kill switch).</li>
 *   <li>{@code JoinInfo.isEqui()} must be true. Theta routes through the coord-centric path.</li>
 *   <li>Both inputs must already carry SHARD+RANDOM (i.e. the planner has stamped scan
 *       traits but no exchange has wrapped them yet). Without this gate the rule re-fires
 *       on the alternatives it produced.</li>
 *   <li>Probe-node estimate (from cluster setting / data-node count) must be ≥ 1.</li>
 *   <li><b>Estimated build-side bytes must be within {@code analytics.mpp.broadcast.max_bytes}.</b>
 *       Broadcast materializes the whole build side in coordinator memory and replicates it to
 *       every probe node; the runtime {@code BroadcastCaptureSink} hard-fails with
 *       {@code BroadcastSizeExceededException} when the captured payload exceeds that cap. This
 *       pre-flight gate stops CBO from ever emitting a broadcast alternative whose build side is
 *       estimated to blow the cap, so a too-large build falls back to hash-shuffle /
 *       coordinator-centric at planning time instead of failing at runtime. The estimate is
 *       {@code rowCount × avgRowSize}; when either is unknown (no seeded stats) the gate admits
 *       broadcast and the runtime cap stays the safety net — gating on missing stats would
 *       wrongly suppress broadcast for un-analyzed indices.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class OpenSearchBroadcastJoinSplitRule extends RelOptRule {

    private static final Logger logger = LogManager.getLogger(OpenSearchBroadcastJoinSplitRule.class);

    private final PlannerContext context;
    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchBroadcastJoinSplitRule(PlannerContext context) {
        super(operand(OpenSearchJoin.class, any()), "OpenSearchBroadcastJoinSplitRule");
        this.context = context;
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!AnalyticsSettings.MPP_ENABLED.get(context.getSettings())) {
            return false;
        }
        // Broadcast made ineligible for this planning attempt — the re-plan after a runtime
        // broadcast-size overflow (see DefaultPlanExecutor's broadcast→shuffle retry). Stay out of
        // CBO so the join routes through hash-shuffle / coordinator-centric instead.
        if (!context.isBroadcastEligible()) {
            return false;
        }
        OpenSearchJoin join = call.rel(0);
        JoinInfo info = join.analyzeCondition();
        // Require at least one EQUI key, but do NOT require info.isEqui(): a join may carry equi keys
        // AND a residual non-equi predicate (e.g. TPC-H q14: l_partkey=p_partkey AND l_shipdate
        // BETWEEN …). emitBroadcastAlternative copies the FULL join condition (equi + residual) onto
        // the worker join, so the probe-side HashJoinExec applies the residual as a join filter after
        // the equi match. A PURE-theta / cross join (no equi key) still bails (empty leftKeys) and
        // routes coord-centric, matching the existing policy that pure-theta joins are not broadcast.
        if (info.leftKeys.isEmpty()) {
            return false;
        }
        // Refuse to fire on already-resolved alternatives. Both inputs must be vanilla
        // SHARD-distributed scans (untouched by an exchange).
        return isShardScan(join.getLeft()) && isShardScan(join.getRight());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchJoin join = call.rel(0);
        int probeNodes = resolveProbeNodeEstimate(join.getLeft());
        if (probeNodes <= 1) {
            // Broadcast offers no parallelism gain over coord-centric on a 1-node cluster
            // (or in test fixtures with a stubbed ClusterState). Leave alternative space to
            // OpenSearchJoinSplitRule (coord-centric); the strategy advisor will record
            // COORDINATOR_CENTRIC for telemetry.
            return;
        }

        JoinRelType joinType = join.getJoinType();
        // Decide which sides are eligible as the build:
        // INNER, FULL? — both sides eligible (full not handled in M2 spike; left/right could
        // broadcast either side but neither preserves rows, so emit both).
        // LEFT — left rows must be preserved → left is probe → build = right.
        // RIGHT — right rows preserved → build = left.
        // SEMI / ANTI — build = right (M0/M1 contract).
        // FULL — neither side can be duplicated; broadcast doesn't apply.
        boolean leftAsBuildEligible = joinType == JoinRelType.INNER || joinType == JoinRelType.RIGHT;
        boolean rightAsBuildEligible = joinType == JoinRelType.INNER
            || joinType == JoinRelType.LEFT
            || joinType == JoinRelType.SEMI
            || joinType == JoinRelType.ANTI;
        if (!leftAsBuildEligible && !rightAsBuildEligible) {
            return;
        }

        // Pre-flight size gate: only emit a broadcast alternative for a build side whose estimated
        // bytes fit the configured cap. A build that would exceed it is left to OpenSearchHashJoinSplitRule
        // (shuffle) / OpenSearchJoinSplitRule (coord-centric), so CBO never picks a doomed broadcast that
        // the runtime BroadcastCaptureSink would reject. See the class javadoc "build-side bytes" gate.
        long maxBytes = AnalyticsSettings.BROADCAST_MAX_BYTES.get(context.getSettings()).getBytes();
        RelMetadataQuery mq = call.getMetadataQuery();

        if (leftAsBuildEligible && buildSideFitsBroadcast(join.getLeft(), mq, maxBytes)) {
            emitBroadcastAlternative(call, join, /* buildSide = */ true, probeNodes);
        }
        if (rightAsBuildEligible && buildSideFitsBroadcast(join.getRight(), mq, maxBytes)) {
            emitBroadcastAlternative(call, join, /* buildSide = */ false, probeNodes);
        }
    }

    /**
     * Estimates whether replicating {@code buildSide} stays within the broadcast byte cap.
     * Estimate = {@code rows × avgRowWidth} (the whole build side is materialized + replicated, so
     * total — not per-shard — rows are the right quantity).
     *
     * <p><b>Optimistic, selectivity-aware row count.</b> We gate on {@code getRowCount} — the
     * expected post-filter estimate — NOT the selectivity-ignoring {@code getMaxRowCount} upper
     * bound. This mirrors Spark's plan-time {@code canBroadcastBySize}, which compares the
     * filter-reduced {@code stats.sizeInBytes} against {@code autoBroadcastJoinThreshold}: a build
     * behind a selective filter (e.g. TPC-H q17's {@code part WHERE p_brand=… AND p_container=…})
     * estimates small and is allowed to broadcast, even though the unfiltered table is huge. A
     * conservative max-bound would suppress those legitimate broadcasts. The cost of trusting the
     * estimate — a build that filters less than predicted and overflows the runtime cap — is caught
     * by the runtime {@code BroadcastSizeExceededException} + broadcast→shuffle re-plan
     * ({@code DefaultPlanExecutor}). Spark relies on AQE measuring materialized shuffle output to
     * the same end; we re-plan on the capture-sink overflow instead.
     *
     * <p>Row width is derived from the build-side {@code RelDataType} directly (sum of per-column
     * type widths), NOT from {@code RelMetadataQuery.getAverageRowSize}: the {@code OpenSearch*}
     * RelNodes don't register a {@code BuiltInMetadata.Size} handler, so {@code getAverageRowSize}
     * returns {@code null} for them. Computing from the row type keeps the gate self-contained and
     * matches Calcite's own per-type width heuristics (see {@code RelMdSize.averageTypeValueSize}).
     *
     * <p>Returns {@code true} (admit broadcast) when the row count is unknown or the row width can't
     * be estimated: missing stats must not suppress broadcast. A cap of {@code 0} (or negative)
     * means "no limit configured" → always admit.
     */
    static boolean buildSideFitsBroadcast(RelNode buildSide, RelMetadataQuery mq, long maxBytes) {
        if (maxBytes <= 0) {
            return true;
        }
        Double rows = mq.getRowCount(buildSide);
        if (rows == null || rows.isInfinite() || rows.isNaN()) {
            return true;
        }
        double avgRowWidth = estimateRowWidthBytes(buildSide.getRowType());
        if (avgRowWidth <= 0) {
            return true;
        }
        double estimatedBytes = rows * avgRowWidth;
        boolean fits = estimatedBytes <= maxBytes;
        if (!fits && logger.isDebugEnabled()) {
            logger.debug(
                "[BroadcastJoinSplitRule] suppressing broadcast alternative: estimated build-side {} bytes "
                    + "(rows={}, avgRowWidth={}) exceeds analytics.mpp.broadcast.max_bytes={} — leaving shuffle/coord-centric to CBO",
                (long) estimatedBytes,
                rows,
                avgRowWidth,
                maxBytes
            );
        }
        return fits;
    }

    /**
     * Average bytes per row for a {@code RelDataType}, summed over per-column type widths. Mirrors
     * Calcite's {@code RelMdSize.averageTypeValueSize} heuristics (fixed-width types by precision;
     * variable-width CHAR/VARCHAR/BINARY capped). Unknown column types contribute a conservative
     * default so a partially-typed row still estimates non-zero rather than collapsing to 0.
     */
    private static double estimateRowWidthBytes(org.apache.calcite.rel.type.RelDataType rowType) {
        double total = 0d;
        for (org.apache.calcite.rel.type.RelDataTypeField field : rowType.getFieldList()) {
            total += averageTypeWidthBytes(field.getType());
        }
        return total;
    }

    /**
     * Per-column average width in bytes — a LOCAL heuristic in the spirit of Calcite's
     * {@code RelMdSize.averageTypeValueSize} (fixed-width types by size, variable-width CHAR/VARCHAR/
     * BINARY capped), but not an exact mirror: less-common SQL types (unsigned ints, time/timestamp
     * -with-zone, intervals) fall through to {@code defaultWidth} rather than their exact Calcite
     * sizes. That's acceptable here — the result feeds a coarse broadcast size gate backed by a
     * runtime cap + retry, not anything that needs byte-exact accounting.
     */
    private static double averageTypeWidthBytes(org.apache.calcite.rel.type.RelDataType type) {
        // BYTES_PER_CHARACTER in Calcite's RelMdSize is 2 (UTF-16 estimate).
        final int bytesPerChar = 2;
        // Width estimate for variable-width string/binary columns: Calcite caps these at 100 bytes
        // since most values are small even in wide columns. We also USE this as the width when the
        // precision is unspecified — OpenSearch keyword/text fields commonly arrive as bare VARCHAR
        // with RelDataType.PRECISION_NOT_SPECIFIED (-1), and feeding -1 into the formulas below would
        // yield a negative width that silently zeroes the row estimate and no-ops the size gate.
        final double variableWidthCap = 100d;
        final double defaultWidth = 8d; // conservative fallback for types we don't special-case
        if (type.getSqlTypeName() == null) {
            return defaultWidth;
        }
        int precision = type.getPrecision(); // may be PRECISION_NOT_SPECIFIED (-1)
        switch (type.getSqlTypeName()) {
            case BOOLEAN:
            case TINYINT:
                return 1d;
            case SMALLINT:
                return 2d;
            case INTEGER:
            case REAL:
            case DECIMAL:
            case DATE:
            case TIME:
                return 4d;
            case BIGINT:
            case DOUBLE:
            case FLOAT:
            case TIMESTAMP:
                return 8d;
            case BINARY:
                return precision > 0 ? precision : variableWidthCap;
            case VARBINARY:
                return precision > 0 ? Math.min(precision, variableWidthCap) : variableWidthCap;
            case CHAR:
                return precision > 0 ? (double) precision * bytesPerChar : variableWidthCap;
            case VARCHAR:
                // Even in large (e.g. VARCHAR(2000)) columns most strings are small; unspecified
                // precision (the common OpenSearch keyword/text case) falls back to the cap.
                return precision > 0 ? Math.min((double) precision * bytesPerChar, variableWidthCap) : variableWidthCap;
            default:
                return defaultWidth;
        }
    }

    /**
     * Emits one broadcast alternative: the chosen build side is wrapped in a broadcast
     * exchange (via trait conversion); the other side (probe) keeps its SHARD trait. The
     * join's own trait is the probe side's SHARD+RANDOM, which propagates the probe's
     * tableId so the cost gate can validate.
     *
     * @param leftIsBuild true if the left side is broadcast (probe = right); false otherwise.
     */
    private void emitBroadcastAlternative(RelOptRuleCall call, OpenSearchJoin join, boolean leftIsBuild, int probeNodes) {
        RelNode buildSide = leftIsBuild ? join.getLeft() : join.getRight();
        RelNode probeSide = leftIsBuild ? join.getRight() : join.getLeft();
        OpenSearchDistribution probeDist = distributionOf(probeSide);
        if (probeDist == null) {
            return;
        }

        // Demand BROADCAST+REPLICATED on the build side. Volcano materializes an
        // OpenSearchBroadcastExchange via OpenSearchDistributionTraitDef.convert.
        OpenSearchDistribution broadcastDist = distTraitDef.broadcast(probeNodes);
        RelTraitSet buildTraits = buildSide.getTraitSet().replace(broadcastDist);
        RelNode broadcastBuild = convert(buildSide, buildTraits);

        // Probe side keeps its SHARD+RANDOM trait — no exchange. The join sits at the same
        // trait so it runs alongside the probe scan on probe-side data nodes. The
        // distribution copy preserves probe's tableId / shardCount (used by the cost gate).
        RelTraitSet joinTraits = join.getTraitSet().replace(distTraitDef.from(probeDist));
        RelNode workerJoin;
        if (leftIsBuild) {
            workerJoin = join.copy(joinTraits, join.getCondition(), broadcastBuild, probeSide, join.getJoinType(), join.isSemiJoinDone());
        } else {
            workerJoin = join.copy(joinTraits, join.getCondition(), probeSide, broadcastBuild, join.getJoinType(), join.isSemiJoinDone());
        }

        // Coord still gathers the joined output. Register the gather alternative in the memo
        // so a parent demanding COORDINATOR+SINGLETON can find it.
        RelTraitSet coordTraits = join.getTraitSet().replace(distTraitDef.coordSingleton());
        convert(workerJoin, coordTraits);
        call.transformTo(workerJoin);
    }

    /** Resolves the probe-node estimate from the cluster setting, defaulting to the cluster's
     *  data-node count when the setting is unset (-1). The {@code anyChild} parameter lets us
     *  keep the API forward-compatible if we ever want per-side estimates. Tolerant of test
     *  fixtures where ClusterState mocks don't stub {@code nodes()}: returns 1 (no broadcast
     *  benefit) which causes the rule to bail. */
    private int resolveProbeNodeEstimate(RelNode anyChild) {
        Integer override = AnalyticsSettings.MPP_BROADCAST_PROBE_ESTIMATE.get(context.getSettings());
        if (override != null && override > 0) {
            return override;
        }
        // Default: cluster's data-node count. Defensive against partially-mocked ClusterState
        // (test fixtures often stub metadata() but not nodes()).
        org.opensearch.cluster.ClusterState state = context.getClusterState();
        if (state == null || state.nodes() == null) {
            return 1;
        }
        return Math.max(state.nodes().getDataNodes().size(), 1);
    }

    /** True when {@code rel}'s OpenSearchDistribution is SHARD-localized — i.e. the rel is an
     *  unwrapped scan (or scan-shaped subtree) without an exchange already on top. */
    private static boolean isShardScan(RelNode rel) {
        OpenSearchDistribution dist = distributionOf(rel);
        if (dist == null) return false;
        return dist.getLocality() == OpenSearchDistribution.Locality.SHARD;
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }

    @SuppressWarnings("unused")
    private static boolean joinAlreadyResolvedAsBroadcast(OpenSearchJoin join) {
        OpenSearchDistribution joinDist = distributionOf(join);
        if (joinDist == null) return false;
        if (joinDist.getType() != RelDistribution.Type.RANDOM_DISTRIBUTED) return false;
        if (joinDist.getLocality() != OpenSearchDistribution.Locality.SHARD) return false;
        // Already a broadcast-shape join — at least one input should be REPLICATED.
        OpenSearchDistribution ld = distributionOf(join.getLeft());
        OpenSearchDistribution rd = distributionOf(join.getRight());
        return (ld != null && ld.getLocality() == OpenSearchDistribution.Locality.REPLICATED)
            || (rd != null && rd.getLocality() == OpenSearchDistribution.Locality.REPLICATED);
    }
}

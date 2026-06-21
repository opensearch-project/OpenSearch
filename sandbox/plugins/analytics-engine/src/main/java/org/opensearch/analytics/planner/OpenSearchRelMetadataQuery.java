/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

/**
 * Metadata query that corrects Calcite's no-statistics equi-join cardinality estimate for
 * {@link OpenSearchJoin}.
 *
 * <p><b>Why this exists.</b> Calcite dispatches {@code getRowCount(Join)} through
 * {@code RelMdRowCount} → {@code RelMdUtil.getJoinRowCount}, which computes
 * {@code leftRows × rightRows × mq.getSelectivity(join, condition)}. With no per-column NDV
 * statistics (the OpenSearch case — index mappings carry no distinct-value counts), the equi
 * predicate's selectivity falls back to Calcite's default guess of {@code 0.15}. For a
 * fact ⋈ dimension join that is really a primary-key/foreign-key relationship
 * (e.g. {@code partsupp.ps_suppkey = supplier.s_suppkey}, 8M × 100K), that yields
 * {@code 8e6 × 1e5 × 0.15 ≈ 1.2e11} — a near-cartesian over-estimate roughly 15,000× the true
 * output of ~8M rows.
 *
 * <p><b>Why it matters.</b> Volcano costs the whole plan tree. The coordinator-centric plan runs
 * the join at the coordinator, so its output is already SINGLETON — nothing above the join is
 * charged the join's row count. The broadcast and hash-shuffle plans run the join
 * distributed, so the root-SINGLETON requirement forces an {@code OpenSearchExchangeReducer}
 * ABOVE the join, and that reducer charges {@code getRowCount(join)}. The inflated 1.2e11
 * therefore poisons ONLY the distributed plans, so the cost model always falls back to
 * coordinator-centric for large×small joins — which then gathers the whole fact table to the
 * coordinator and trips {@code ReduceSizeExceededException} (TPC-H q2/q11). Correcting the join
 * estimate lets the distributed shapes (and, with a PARTIAL aggregate pushed below the gather,
 * the distributed-agg-over-join shape) compete on realistic costs.
 *
 * <p><b>The correction.</b> For an INNER equi-join with no NDV stats the principled PK-FK
 * estimate is {@code leftRows × rightRows / max(ndv(leftKeys), ndv(rightKeys))}. With the
 * uniqueness assumption that the dimension side's key is unique — i.e. its NDV equals its row
 * count — this collapses to {@code max(leftRows, rightRows)}: every row on the larger (fact) side
 * matches at most one row on the smaller (dimension) side. We then apply the residual non-equi
 * selectivity (Calcite's estimate for the {@code nonEquiConditions}) and adjust per join type the
 * same way {@link RelMdUtil#getJoinRowCount} does (preserved-side floors for outer joins). This is
 * strictly closer to the truth than {@code 0.15 × cartesian} whenever the equi keys are FK-related
 * (the dominant analytics shape) and never produces the cartesian blow-up.
 *
 * <p><b>Scope &amp; safety.</b> Only {@link OpenSearchJoin} with at least one equi key is
 * intercepted; SEMI/ANTI joins (which {@code getJoinRowCount} already estimates from the
 * left side alone), pure-theta joins (no equi keys), and every other RelNode fall through to
 * {@code super.getRowCount}. ALL other metadata defs (selectivity, column uniqueness, distinct row
 * count, collation, …) are inherited unchanged from {@link RelMetadataQuery} — this subclass adds
 * one override and nothing else, so the default Janino handler chain stays intact. (An earlier
 * attempt that swapped the whole {@code RelMetadataProvider} broke every other metadata call with
 * {@code NoHandler}; subclassing avoids that entirely.)
 *
 * @opensearch.internal
 */
public final class OpenSearchRelMetadataQuery extends RelMetadataQuery {

    /** Uses the thread-local default handler chain wired by the protected no-arg constructor. */
    public OpenSearchRelMetadataQuery() {
        super();
    }

    @Override
    public Double getRowCount(RelNode rel) {
        if (rel instanceof OpenSearchJoin join) {
            Double corrected = fkJoinRowCount(join);
            if (corrected != null) {
                return corrected;
            }
        }
        return super.getRowCount(rel);
    }

    /**
     * PK-FK row-count estimate for an INNER/LEFT/RIGHT/FULL equi-join. Returns {@code null} to defer
     * to the default estimate when this correction does not apply (no equi key, semi/anti join, or
     * unknown child row counts).
     */
    private Double fkJoinRowCount(Join join) {
        if (!join.getJoinType().projectsRight()) {
            // SEMI / ANTI: getJoinRowCount already derives these from the left side alone, not the
            // cartesian product, so there is no blow-up to correct.
            return null;
        }
        JoinInfo info = join.analyzeCondition();
        if (info.leftKeys.isEmpty()) {
            // Pure theta join: no equi key to anchor the FK assumption. Defer.
            return null;
        }
        Double left = getRowCount(join.getLeft());
        Double right = getRowCount(join.getRight());
        if (left == null || right == null) {
            return null;
        }

        // FK equi-join base: every row of the larger side matches at most one row of the smaller
        // (unique-key) side, so the equi-join output is max(left, right), not left × right × 0.15.
        double equiRowCount = Math.max(left, right);

        // Apply the residual (non-equi) predicate selectivity, mirroring how getJoinRowCount folds
        // the full condition's selectivity in. Equi keys are already accounted for above, so we ask
        // only for the leftover conjuncts' selectivity.
        RexNode residual = info.getRemaining(join.getCluster().getRexBuilder());
        Double residualSelectivity = getSelectivity(join, residual);
        double selectivity = residualSelectivity == null ? 1.0D : residualSelectivity;
        double innerRowCount = equiRowCount * selectivity;

        switch (join.getJoinType()) {
            case INNER:
                return innerRowCount;
            case LEFT:
                // Preserve every left row; add the inner matches beyond the 1:1 floor.
                return Math.max(left, innerRowCount);
            case RIGHT:
                return Math.max(right, innerRowCount);
            case FULL:
                return Math.max(left + right, innerRowCount);
            default:
                // ASOF / LEFT_ASOF and any future types: defer to Calcite's own handling.
                return null;
        }
    }
}

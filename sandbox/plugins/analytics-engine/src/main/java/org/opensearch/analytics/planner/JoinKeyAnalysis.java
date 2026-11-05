/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts the join keys a join can be PARTITIONED on, which is a strictly wider set than the keys
 * {@link Join#analyzeCondition()} reports.
 *
 * <p><b>The gap.</b> Calcite's {@code JoinInfo} only accepts {@code =} as an equi key. A null-safe
 * equality — {@code a IS NOT DISTINCT FROM b}, i.e. {@code a = b OR (a IS NULL AND b IS NULL)} —
 * comes back as a non-equi residual with {@code leftKeys} EMPTY. Every distribution decision in this
 * planner gates on {@code leftKeys.isEmpty()}, so such a join reads as PURE THETA: the broadcast,
 * hash-shuffle and worker-tier paths all decline and the join is forced coordinator-centric,
 * gathering both inputs in full.
 *
 * <p>Null-safe equality reaches plans without anyone writing it: decorrelating a correlated scalar
 * subquery emits it on the correlation key, so the shape appears whenever a subquery correlates back
 * to the outer query.
 *
 * <p><b>Why partitioning on a null-safe key is sound.</b> Hash-partitioning routes a row by the hash
 * of its key tuple, and NULL hashes to a fixed value like any other, so rows with NULL keys on the
 * left and rows with NULL keys on the right land in the SAME partition. Every pair the null-safe
 * predicate should match is therefore co-located in one partition, and the worker join re-evaluates
 * the ORIGINAL condition there. The same holds for broadcast, where the whole build side is present.
 * This is why null-safe equality is a first-class join key in shuffled joins generally, and it is the
 * reason this analysis may only influence PARTITIONING — never the condition that gets executed.
 *
 * <p><b>Scope.</b> {@link #forDistribution} returns keys for placing exchanges and for row-count
 * estimation. It deliberately does NOT rewrite {@link Join#getCondition()}: the executed condition
 * stays null-safe, which is what preserves the null-matching rows. A rewrite to plain {@code =} would
 * be legal only if both operands were non-nullable, and every OpenSearch index column is nullable by
 * construction (a document may omit any field), so that rewrite would be dead code here.
 *
 * @opensearch.internal
 */
public final class JoinKeyAnalysis {

    private JoinKeyAnalysis() {}

    /**
     * The join's key information for distribution purposes, counting top-level null-safe equality
     * conjuncts as equi keys.
     *
     * <p>Returns {@link Join#analyzeCondition()} verbatim when the condition holds no null-safe
     * conjunct, so for every ordinary {@code =} join this is exactly the previous behaviour.
     *
     * @param join the join whose keys to analyze
     * @return key information; {@code leftKeys} is non-empty iff the join has at least one
     *         partitionable key
     */
    public static JoinInfo forDistribution(Join join) {
        RexNode condition = join.getCondition();
        if (condition == null) {
            return join.analyzeCondition();
        }
        RexNode strict = strictEquiForm(join.getCluster().getRexBuilder(), condition);
        if (strict == condition) {
            return join.analyzeCondition();
        }
        return JoinInfo.of(join.getLeft(), join.getRight(), strict);
    }

    /**
     * Rewrites top-level {@code IS NOT DISTINCT FROM} conjuncts to {@code =} so Calcite's key
     * splitter recognises them. Returns the input unchanged (same identity) when there is nothing to
     * rewrite, which lets callers cheaply detect the no-op case.
     *
     * <p>Only TOP-LEVEL conjuncts are considered, matching the scope over which {@code JoinInfo}
     * looks for equi keys: a null-safe comparison nested inside an {@code OR} is not a key for either
     * analysis.
     *
     * <p>The result is used ONLY as an analysis probe. It is not semantically equivalent to the input
     * over nullable operands — that is precisely the difference this class exists to reason about —
     * and must never be substituted for the join's real condition.
     */
    private static RexNode strictEquiForm(RexBuilder rexBuilder, RexNode condition) {
        List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
        List<RexNode> rewritten = new ArrayList<>(conjuncts.size());
        boolean changed = false;
        for (RexNode conjunct : conjuncts) {
            if (conjunct.getKind() == SqlKind.IS_NOT_DISTINCT_FROM) {
                List<RexNode> operands = ((RexCall) conjunct).getOperands();
                rewritten.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, operands.get(0), operands.get(1)));
                changed = true;
            } else {
                rewritten.add(conjunct);
            }
        }
        if (!changed) {
            return condition;
        }
        return RexUtil.composeConjunction(rexBuilder, rewritten);
    }
}

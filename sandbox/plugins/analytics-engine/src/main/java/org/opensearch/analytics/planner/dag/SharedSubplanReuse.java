/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Repeated-sub-plan detection for a plan that computes the SAME complete aggregate more than once.
 *
 * <p><b>The correctness problem this solves.</b> A query may inline one subquery twice — TPC-H q15 is
 * {@code supplier ⋈ revenue0} plus {@code where total_revenue = [ … max(total_revenue) ]} over the same
 * {@code revenue0}, because the spec's {@code revenue0} VIEW has no PPL equivalent. Each copy is aggregated
 * independently, and {@code SUM(double)} is not associative, so the two copies' partial sums are merged in
 * different orders and disagree in the last bits. The exact {@code =} then matches nothing and the row
 * vanishes: q15 returns 1 row or 0 rows at random (measured ~9/20 correct, in every distribution
 * configuration — coordinator-centric included, so this is not an MPP artifact).
 *
 * <p>Making float summation order-independent would be one cure; computing the shared relation ONCE is the
 * other, and it is correct by construction rather than by numeric luck — both consumers then read the very
 * same rows, so the equality holds whatever order the sum ran in. It also halves the work.
 *
 * <p><b>How.</b> {@link DAGBuilder} asks this class, as it severs the plan, whether a node it is about to
 * descend into is a shared sub-plan. The first occurrence is cut into ONE ordinary child stage; every
 * occurrence then becomes an {@code OpenSearchStageInputScan} on that same child stage id, so the consumer's
 * fragment scans the single named table {@code input-<childStageId>} more than once. That works with no new
 * transport because a multi-input coordinator stage is served by {@code DatafusionMemtableReduceSink}, which
 * buffers each child input into a re-readable {@code MemTable}.
 *
 * <p><b>Deliberately narrow.</b> Only a COMPLETE aggregate ({@link AggregateMode#FINAL} or
 * {@link AggregateMode#SINGLE}) is a candidate, and only when its subtree contains no shuffle, broadcast or
 * late-materialization boundary. That bounds what gets buffered (an aggregate's output, not a raw scan's) and
 * keeps the shared stage a plain gather. Anything else is left alone — a missed reuse costs performance, a wrong
 * one costs correctness.
 *
 * <p><b>Sharing is scoped per FRAGMENT and per buffered consumer</b>, both enforced in {@link DAGBuilder}: a
 * shared stage must be a direct child of the stage scanning it (else {@code No table named 'input-N'}), and the
 * consumer must buffer its inputs (the streaming reduce sink's inputs are once-consumable, so a second read
 * returns nothing). {@code DAGBuilder} rebuilds without sub-plan reuse rather than emit either shape.
 *
 * @opensearch.internal
 */
final class SharedSubplanReuse {

    /**
     * Annotation ids are a per-query sequential counter ({@code ANNOTATED_PREDICATE(id=0, …)}), so two
     * semantically identical subtrees carry DIFFERENT ids — in q15 one copy has {@code id=0,1,4,5} and the
     * other {@code id=2,3,6,7}. They must not defeat the match, and dropping them is safe: the surviving copy
     * keeps its own annotations, and the eliminated copy's are simply no longer referenced.
     */
    private static final Pattern ANNOTATION_ID = Pattern.compile("id=\\d+, ");

    private final Set<String> sharedDigests;
    private final Map<String, Integer> cutStageIdByDigest = new HashMap<>();

    private SharedSubplanReuse(Set<String> sharedDigests) {
        this.sharedDigests = sharedDigests;
    }

    /** Digests every candidate in {@code root} and retains those occurring more than once. */
    static SharedSubplanReuse detect(RelNode root) {
        Map<String, Integer> counts = new HashMap<>();
        Deque<RelNode> queue = new ArrayDeque<>();
        queue.push(root);
        while (!queue.isEmpty()) {
            RelNode node = queue.pop();
            if (isCandidate(node)) {
                counts.merge(digestOf(node), 1, Integer::sum);
            }
            for (RelNode input : node.getInputs()) {
                queue.push(input);
            }
        }
        Set<String> shared = new HashSet<>();
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            if (e.getValue() > 1) {
                shared.add(e.getKey());
            }
        }
        return new SharedSubplanReuse(shared);
    }

    /** Number of distinct shared sub-plans found — diagnostics only. */
    int sharedCount() {
        return sharedDigests.size();
    }

    /** True when no candidate repeats, so {@link DAGBuilder} can skip every reuse check. */
    boolean isEmpty() {
        return sharedDigests.isEmpty();
    }

    /** The digest of {@code node} if it is a shared sub-plan, else {@code null}. */
    String sharedDigestOf(RelNode node) {
        if (!isCandidate(node)) {
            return null;
        }
        String digest = digestOf(node);
        return sharedDigests.contains(digest) ? digest : null;
    }

    /** The child-stage id already cut for {@code digest}, or {@code null} on first encounter. */
    Integer alreadyCutStageId(String digest) {
        return cutStageIdByDigest.get(digest);
    }

    void recordCut(String digest, int childStageId) {
        cutStageIdByDigest.put(digest, childStageId);
    }

    private static boolean isCandidate(RelNode node) {
        if (!(node instanceof OpenSearchAggregate aggregate)) {
            return false;
        }
        if (aggregate.getMode() != AggregateMode.FINAL && aggregate.getMode() != AggregateMode.SINGLE) {
            return false;
        }
        return !containsUnsupportedBoundary(node);
    }

    /**
     * A shared subtree is cut as a plain gather stage. A shuffle / broadcast / late-materialization boundary
     * inside would make that stage need producer or injection wiring too, so those subtrees are not shared.
     */
    private static boolean containsUnsupportedBoundary(RelNode node) {
        if (node instanceof OpenSearchShuffleExchange
            || node instanceof OpenSearchBroadcastExchange
            || node instanceof OpenSearchLateMaterialization) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (containsUnsupportedBoundary(input)) {
                return true;
            }
        }
        return false;
    }

    private static String digestOf(RelNode node) {
        return ANNOTATION_ID.matcher(RelOptUtil.toString(node)).replaceAll("");
    }
}

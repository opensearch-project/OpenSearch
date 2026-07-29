/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.spi.ShardSortBounds;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinator-side shard-metadata probe. Sends one lightweight request per shard target in
 * parallel and uses each reply for two independent things:
 *
 * <ol>
 *   <li><b>Pruning</b> (needs range filters) — drops shards that provably cannot match the
 *       {@code WHERE} predicates.</li>
 *   <li><b>Ordering</b> (needs a bounded field sort) — sorts the survivors by the sort column's
 *       min/max so the most promising shard is dispatched first.</li>
 * </ol>
 *
 * <p>Neither needs the other: a query may have filters, a sort, or both. They share one phase
 * because both answers come from the same shard reader and parquet footers, so one round-trip
 * serves both. Ordering only changes dispatch sequence, never results.
 *
 * <p>Fail-open throughout: a transport failure, timeout, or unexpected exception keeps the target
 * (never prunes it) with no bounds. Missing bounds are also normal for a healthy shard whose sort
 * column has no usable statistics — those shards just sort last.
 *
 * @opensearch.internal
 */
public class CanMatchPreFilterPhase {

    private static final Logger logger = LogManager.getLogger(CanMatchPreFilterPhase.class);

    /** Sent for a sort with no filters: the data node prunes nothing but still folds bounds. */
    private static final byte[] EMPTY_FILTERS = new byte[0];

    private final TransportService transportService;

    public CanMatchPreFilterPhase(TransportService transportService) {
        this.transportService = transportService;
    }

    /**
     * Dispatches can-match requests to all targets in parallel.
     *
     * @param targets     resolved execution targets
     * @param filterBytes serialized filter list (from {@link CanMatchFilterSerializer})
     * @param listener    receives the filtered target list (only those that can match)
     */
    public void filter(
        List<ExecutionTarget> targets,
        byte[] filterBytes,
        String backendId,
        ActionListener<List<ExecutionTarget>> listener
    ) {
        filter(targets, filterBytes, backendId, null, listener);
    }

    /**
     * Dispatches to all targets in parallel, collecting the prune decision and — when
     * {@code sortSpec} is given — each shard's min/max, then returns the survivors in
     * dispatch order.
     *
     * @param sortSpec primary sort key + direction, or {@code null} to skip bounds collection
     */
    public void filter(
        List<ExecutionTarget> targets,
        byte[] filterBytes,
        String backendId,
        SortSpec sortSpec,
        ActionListener<List<ExecutionTarget>> listener
    ) {
        if (targets.isEmpty()) {
            listener.onResponse(Collections.emptyList());
            return;
        }
        boolean hasFilters = filterBytes != null && filterBytes.length > 0;
        // Nothing to prune and nothing to order by — skip the round-trip entirely.
        if (hasFilters == false && sortSpec == null) {
            listener.onResponse(targets);
            return;
        }
        byte[] effectiveFilters = hasFilters ? filterBytes : EMPTY_FILTERS;
        String sortColumn = sortSpec != null ? sortSpec.column() : null;

        Set<ExecutionTarget> matching = Collections.newSetFromMap(new IdentityHashMap<>());
        // Identity-keyed to match `matching` above: targets are compared by reference here.
        Map<ExecutionTarget, ShardSortBounds> boundsByTarget = new IdentityHashMap<>();
        AtomicInteger pending = new AtomicInteger(targets.size());
        Completion completion = new Completion(matching, boundsByTarget, targets, sortSpec, listener);

        for (ExecutionTarget target : targets) {
            if (!(target instanceof ShardExecutionTarget shardTarget)) {
                // Non-shard targets pass through unconditionally
                completion.keep(target, null, pending);
                continue;
            }

            DiscoveryNode node = shardTarget.node();
            AnalyticsCanMatchRequest request = new AnalyticsCanMatchRequest(shardTarget.shardId(), effectiveFilters, backendId, sortColumn);

            try {
                transportService.sendRequest(
                    node,
                    AnalyticsCanMatchAction.NAME,
                    request,
                    new TransportResponseHandler<AnalyticsCanMatchResponse>() {
                        @Override
                        public AnalyticsCanMatchResponse read(StreamInput in) throws IOException {
                            return new AnalyticsCanMatchResponse(in);
                        }

                        @Override
                        public void handleResponse(AnalyticsCanMatchResponse response) {
                            if (response.canMatch()) {
                                completion.keep(target, response.bounds(), pending);
                            } else {
                                logger.debug("can-match: shard {} pruned", shardTarget.shardId());
                                completion.drop(pending);
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            // Fail-open: include this target on any transport error
                            logger.debug(
                                () -> "can-match: transport error for shard " + shardTarget.shardId() + ", keeping (fail-open)",
                                exp
                            );
                            completion.keep(target, null, pending);
                        }

                        @Override
                        public String executor() {
                            return "same";
                        }
                    }
                );
            } catch (Exception e) {
                // Fail-open: connection failure or other exception
                logger.debug("can-match: dispatch failed for shard {}, keeping (fail-open): {}", shardTarget.shardId(), e.getMessage());
                completion.keep(target, null, pending);
            }
        }
    }

    /**
     * Fan-in bookkeeping: collects survivors and their bounds, then on the last response builds
     * the final list — pruned shards removed, survivors ordered by bound where possible.
     */
    private record Completion(Set<ExecutionTarget> matching, Map<ExecutionTarget, ShardSortBounds> boundsByTarget, List<
        ExecutionTarget> originalTargets, SortSpec sortSpec, ActionListener<List<ExecutionTarget>> listener) {

        /** Target survives. {@code bounds} may be null (not requested, or unavailable). */
        void keep(ExecutionTarget target, ShardSortBounds bounds, AtomicInteger pending) {
            synchronized (matching) {
                matching.add(target);
                if (bounds != null) {
                    boundsByTarget.put(target, bounds);
                }
            }
            maybeComplete(pending);
        }

        /** Target pruned — nothing to record, just count it in. */
        void drop(AtomicInteger pending) {
            maybeComplete(pending);
        }

        private void maybeComplete(AtomicInteger pending) {
            if (pending.decrementAndGet() != 0) {
                return;
            }
            List<ExecutionTarget> survivors = new ArrayList<>(matching.size());
            Map<ExecutionTarget, ShardSortBounds> bounds;
            synchronized (matching) {
                // Input order first, so the no-sort and mixed-type-fallback paths are unchanged.
                for (ExecutionTarget t : originalTargets) {
                    if (matching.contains(t)) {
                        survivors.add(t);
                    }
                }
                bounds = new IdentityHashMap<>(boundsByTarget);
            }
            // All shards pruned: keep the first target anyway. Downstream stages (e.g. reduce)
            // still need one shard to execute to produce a valid, well-formed empty result —
            // schema, empty aggregates, etc.
            if (survivors.isEmpty() && originalTargets.isEmpty() == false) {
                survivors.add(originalTargets.get(0));
            }
            if (sortSpec != null) {
                orderByBounds(survivors, bounds, sortSpec);
            }
            listener.onResponse(survivors);
        }
    }

    /**
     * Sorts {@code survivors} in place, most promising first: by {@code max} descending for a
     * {@code DESC} sort, by {@code min} ascending for {@code ASC}. Stable, so ties keep their
     * input order.
     *
     * <p>Shards with no bounds go last — unknown isn't promising.
     *
     * <p>Refuses to reorder at all when the bounds disagree on physical type, since comparing
     * (say) millisecond- and nanosecond-scaled values orders by a meaningless key. Input order
     * is always correct, so falling back is safe.
     */
    private static void orderByBounds(
        List<ExecutionTarget> survivors,
        Map<ExecutionTarget, ShardSortBounds> boundsByTarget,
        SortSpec sortSpec
    ) {
        if (survivors.size() < 2 || boundsByTarget.isEmpty()) {
            return;
        }
        if (hasConsistentValueKind(boundsByTarget) == false) {
            logger.debug("can-match: mixed sort-bound value kinds, keeping input order");
            return;
        }
        boolean descending = sortSpec.descending();
        survivors.sort((left, right) -> {
            ShardSortBounds a = boundsByTarget.get(left);
            ShardSortBounds b = boundsByTarget.get(right);
            if (a == null || b == null) {
                // nulls last; equal when both unknown
                return a == b ? 0 : (a == null ? 1 : -1);
            }
            // DESC wants the largest max first; ASC wants the smallest min first.
            return descending ? Long.compare(b.max(), a.max()) : Long.compare(a.min(), b.min());
        });
        if (logger.isDebugEnabled()) {
            logger.debug(
                "can-match: ordered {} shards by {} {} -> {}",
                survivors.size(),
                sortSpec.column(),
                descending ? "DESC(max)" : "ASC(min)",
                describeOrder(survivors, boundsByTarget)
            );
        }
    }

    /**
     * Renders the ordered shard sequence with each bound, for the DEBUG log.
     *
     * <p>TODO: remove along with the dispatch-order diagnostics once shard ordering is settled.
     */
    private static String describeOrder(List<ExecutionTarget> survivors, Map<ExecutionTarget, ShardSortBounds> boundsByTarget) {
        StringBuilder sb = new StringBuilder();
        for (ExecutionTarget target : survivors) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            if (target instanceof ShardExecutionTarget shardTarget) {
                sb.append(shardTarget.shardId());
            } else {
                sb.append("non-shard");
            }
            ShardSortBounds bounds = boundsByTarget.get(target);
            if (bounds == null) {
                sb.append("[no-bounds]");
            } else {
                sb.append('[').append(bounds.min()).append("..").append(bounds.max()).append(']');
            }
        }
        return sb.toString();
    }

    /** True when every present bound reports the same physical type. */
    private static boolean hasConsistentValueKind(Map<ExecutionTarget, ShardSortBounds> boundsByTarget) {
        byte kind = 0;
        for (ShardSortBounds bounds : boundsByTarget.values()) {
            if (kind == 0) {
                kind = bounds.valueKind();
            } else if (kind != bounds.valueKind()) {
                return false;
            }
        }
        return true;
    }
}

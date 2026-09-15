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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinator-side shard-metadata check. Sends one lightweight request per shard target in
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
     * Result of a {@link #checkShards} call: the surviving targets and their sort-column ranges.
     *
     * @param targets        survivors in dispatch order — most promising first when ordering applied
     * @param boundsByTarget shard-wide min/max, for the subset of targets that reported any
     */
    public record ShardCheckResult(List<ExecutionTarget> targets, Map<ExecutionTarget, ShardSortBounds> boundsByTarget) {

        /** Fail-open result: every target kept, nothing learned about bounds. */
        public static ShardCheckResult keepAll(List<ExecutionTarget> targets) {
            return new ShardCheckResult(targets, Collections.emptyMap());
        }
    }

    /**
     * Dispatches one can-match request per target in parallel and hands back the survivors in
     * dispatch order plus each one's bounds.
     *
     * <p>Returns without a round-trip when there is nothing to learn — no targets, or neither
     * filters nor a sort spec.
     *
     * @param targets     resolved execution targets
     * @param filterBytes serialized filter list (from {@link CanMatchFilterSerializer}); null or
     *                    empty prunes nothing, which is the normal shape for a bare sort
     * @param sortSpec    primary sort key + direction, or {@code null} to skip bounds collection
     */
    public void checkShards(
        List<ExecutionTarget> targets,
        byte[] filterBytes,
        String backendId,
        SortSpec sortSpec,
        ActionListener<ShardCheckResult> listener
    ) {
        if (targets.isEmpty()) {
            listener.onResponse(ShardCheckResult.keepAll(Collections.emptyList()));
            return;
        }
        boolean hasFilters = filterBytes != null && filterBytes.length > 0;
        if (hasFilters == false && sortSpec == null) {
            listener.onResponse(ShardCheckResult.keepAll(targets));
            return;
        }
        byte[] effectiveFilters = hasFilters ? filterBytes : EMPTY_FILTERS;
        String sortColumn = sortSpec != null ? sortSpec.column() : null;

        List<ExecutionTarget> matching = new ArrayList<>(targets.size());
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
                            logger.error(
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
                logger.error("can-match: dispatch failed for shard {}, keeping (fail-open): {}", shardTarget.shardId(), e.getMessage());
                completion.keep(target, null, pending);
            }
        }
    }

    /**
     * Collects survivors and their bounds as responses land, then orders them and fires the listener
     * once the last one is in.
     */
    private record Completion(List<ExecutionTarget> matching, Map<ExecutionTarget, ShardSortBounds> boundsByTarget, List<
        ExecutionTarget> originalTargets, SortSpec sortSpec, ActionListener<ShardCheckResult> listener) {

        void keep(ExecutionTarget target, ShardSortBounds bounds, AtomicInteger pending) {
            synchronized (matching) {
                matching.add(target);
                if (bounds != null) {
                    boundsByTarget.put(target, bounds);
                }
            }
            maybeComplete(pending);
        }

        void drop(AtomicInteger pending) {
            maybeComplete(pending);
        }

        private void maybeComplete(AtomicInteger pending) {
            if (pending.decrementAndGet() != 0) {
                return;
            }
            if (matching.isEmpty() && originalTargets.isEmpty() == false) {
                matching.add(originalTargets.get(0));
            }
            if (sortSpec != null) {
                orderByBounds(matching, boundsByTarget, sortSpec);
            }
            listener.onResponse(new ShardCheckResult(matching, Collections.unmodifiableMap(boundsByTarget)));
        }
    }

    /** Sorts {@code survivors} most-promising-first: by {@code max} for DESC, {@code min} for ASC, unbounded shards last. */
    private static void orderByBounds(
        List<ExecutionTarget> survivors,
        Map<ExecutionTarget, ShardSortBounds> boundsByTarget,
        SortSpec sortSpec
    ) {
        if (survivors.size() < 2 || boundsByTarget.isEmpty()) {
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
        logger.debug("can-match: ordered {} shards by {} {}", survivors.size(), sortSpec.column(), descending ? "DESC(max)" : "ASC(min)");
    }
}

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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinator-side orchestration for the can-match pre-filter phase.
 * Dispatches can-match requests to all target shards in parallel, collects
 * responses, and returns the filtered list of targets that can match.
 *
 * <h2>Integration point</h2>
 * Called from {@code ShardFragmentStageExecution.materializeTasks()} BEFORE
 * building the task list. Only targets with canMatch=true become StageTask entries.
 *
 * <h2>Semantics</h2>
 * <ul>
 *   <li>Empty target list → empty result; no dispatch.</li>
 *   <li>Empty/null filterBytes → all targets pass; no dispatch (no extractable predicate).</li>
 *   <li>Per-target response with {@code canMatch=true} → keep; {@code canMatch=false} → drop.</li>
 *   <li>Per-target transport exception → fail-open (keep the target); the worst case is
 *       a wasted scan, never an incorrect result.</li>
 * </ul>
 *
 * <h2>Concurrency</h2>
 * All targets are dispatched in parallel; the listener is invoked exactly once,
 * after every target has either responded or failed. The internal {@code matching}
 * list is synchronized for the additions.
 *
 * @opensearch.internal
 */
public class CanMatchPreFilterPhase {

    private static final Logger logger = LogManager.getLogger(CanMatchPreFilterPhase.class);

    private final StreamTransportService transportService;

    public CanMatchPreFilterPhase(StreamTransportService transportService) {
        this.transportService = transportService;
    }

    /**
     * Dispatches can-match requests to all targets in parallel.
     *
     * @param targets     all resolved shard targets (must be {@link ShardExecutionTarget}s)
     * @param filterBytes serialized filter predicate; empty/null skips dispatch
     * @param backendId   backend identifier ({@code "datafusion"} / {@code "lucene"}); routed to the data-node SPI
     * @param listener    receives the filtered targets (only those that can match)
     */
    public void filter(List<ExecutionTarget> targets, byte[] filterBytes, String backendId, ActionListener<List<ExecutionTarget>> listener) {
        if (targets.isEmpty() || filterBytes == null || filterBytes.length == 0) {
            listener.onResponse(targets);
            return;
        }

        List<ExecutionTarget> matching = new ArrayList<>(targets.size());
        AtomicInteger pending = new AtomicInteger(targets.size());

        for (ExecutionTarget target : targets) {
            if (!(target instanceof ShardExecutionTarget shardTarget)) {
                // Non-shard targets aren't subject to can-match — keep them.
                addAndMaybeComplete(matching, target, pending, targets, listener);
                continue;
            }

            AnalyticsCanMatchRequest request = new AnalyticsCanMatchRequest(shardTarget.shardId(), filterBytes, backendId);
            logger.info("can-match dispatch shard={} node={} backend={}", shardTarget.shardId(), shardTarget.node().getName(), backendId);
            transportService.sendRequest(
                shardTarget.node(),
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
                            addAndMaybeComplete(matching, target, pending, targets, listener);
                        } else {
                            maybeComplete(matching, pending, targets, listener);
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        // Fail-open: a transport-level failure shouldn't drop a shard we
                        // can't disprove a match for. Worst case: a wasted scan.
                        logger.debug("can-match dispatch failed for {} — failing open", shardTarget.shardId(), exp);
                        addAndMaybeComplete(matching, target, pending, targets, listener);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                }
            );
        }
    }

    private static <T extends TransportResponse> void addAndMaybeComplete(
        List<ExecutionTarget> matching,
        ExecutionTarget target,
        AtomicInteger pending,
        List<ExecutionTarget> originalTargets,
        ActionListener<List<ExecutionTarget>> listener
    ) {
        synchronized (matching) {
            matching.add(target);
        }
        maybeComplete(matching, pending, originalTargets, listener);
    }

    private static void maybeComplete(
        List<ExecutionTarget> matching,
        AtomicInteger pending,
        List<ExecutionTarget> originalTargets,
        ActionListener<List<ExecutionTarget>> listener
    ) {
        if (pending.decrementAndGet() == 0) {
            // Identity-based set: targets are reference-equal across the matching list and the
            // input list (same instances passed through). Linear merge replaces the previous
            // O(N²) snapshot.contains scan that mattered at thousands of shards.
            java.util.Set<ExecutionTarget> matchingSet = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
            synchronized (matching) {
                matchingSet.addAll(matching);
            }
            List<ExecutionTarget> ordered = new ArrayList<>(matchingSet.size());
            for (ExecutionTarget t : originalTargets) {
                if (matchingSet.contains(t)) {
                    ordered.add(t);
                }
            }
            listener.onResponse(ordered);
        }
    }
}

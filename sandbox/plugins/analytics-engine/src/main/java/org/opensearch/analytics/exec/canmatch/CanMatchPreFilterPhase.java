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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinator-side can-match dispatch. Sends a lightweight can-match request
 * to each shard target in parallel, collects responses, and returns the
 * filtered list of targets that can possibly match.
 *
 * <p>Fail-open on any error: transport failures, timeouts, or unexpected
 * exceptions cause the target to be kept (not pruned).
 *
 * @opensearch.internal
 */
public class CanMatchPreFilterPhase {

    private static final Logger logger = LogManager.getLogger(CanMatchPreFilterPhase.class);

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
    public void filter(List<ExecutionTarget> targets, byte[] filterBytes, ActionListener<List<ExecutionTarget>> listener) {
        if (targets.isEmpty()) {
            listener.onResponse(Collections.emptyList());
            return;
        }
        if (filterBytes == null || filterBytes.length == 0) {
            listener.onResponse(targets);
            return;
        }

        Set<ExecutionTarget> matching = Collections.newSetFromMap(new IdentityHashMap<>());
        AtomicInteger pending = new AtomicInteger(targets.size());

        for (ExecutionTarget target : targets) {
            if (!(target instanceof ShardExecutionTarget shardTarget)) {
                // Non-shard targets pass through unconditionally
                addAndMaybeComplete(matching, target, pending, targets, listener);
                continue;
            }

            DiscoveryNode node = shardTarget.node();
            AnalyticsCanMatchRequest request = new AnalyticsCanMatchRequest(shardTarget.shardId(), filterBytes);

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
                                addAndMaybeComplete(matching, target, pending, targets, listener);
                            } else {
                                logger.debug("can-match: shard {} pruned", shardTarget.shardId());
                                maybeComplete(pending, matching, targets, listener);
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            // Fail-open: include this target on any transport error
                            logger.debug(
                                "can-match: transport error for shard {}, keeping (fail-open): {}",
                                shardTarget.shardId(),
                                exp.getMessage()
                            );
                            addAndMaybeComplete(matching, target, pending, targets, listener);
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
                addAndMaybeComplete(matching, target, pending, targets, listener);
            }
        }
    }

    private static void addAndMaybeComplete(
        Set<ExecutionTarget> matching,
        ExecutionTarget target,
        AtomicInteger pending,
        List<ExecutionTarget> originalTargets,
        ActionListener<List<ExecutionTarget>> listener
    ) {
        synchronized (matching) {
            matching.add(target);
        }
        maybeComplete(pending, matching, originalTargets, listener);
    }

    private static void maybeComplete(
        AtomicInteger pending,
        Set<ExecutionTarget> matching,
        List<ExecutionTarget> originalTargets,
        ActionListener<List<ExecutionTarget>> listener
    ) {
        if (pending.decrementAndGet() == 0) {
            // Preserve original ordering
            List<ExecutionTarget> ordered = new ArrayList<>(matching.size());
            synchronized (matching) {
                for (ExecutionTarget t : originalTargets) {
                    if (matching.contains(t)) {
                        ordered.add(t);
                    }
                }
            }
            listener.onResponse(ordered);
        }
    }
}

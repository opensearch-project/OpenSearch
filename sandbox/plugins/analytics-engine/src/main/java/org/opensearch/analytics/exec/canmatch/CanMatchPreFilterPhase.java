/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.StreamTransportService;

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
 * <h2>Flow</h2>
 * <pre>
 * materializeTasks():
 *   List allTargets = targetResolver.resolve(clusterState);
 *
 *   // NEW: can-match filter
 *   canMatchPreFilter.filter(allTargets, filterBytes, ActionListener.wrap(
 *       matchingTargets -> {
 *           // build tasks only for matching targets
 *           for (target : matchingTargets) { tasks.add(new ShardStageTask(...)); }
 *       },
 *       error -> {
 *           // on failure: fall back to all targets (conservative)
 *           for (target : allTargets) { tasks.add(new ShardStageTask(...)); }
 *       }
 *   ));
 * </pre>
 *
 * @opensearch.internal
 */
public class CanMatchPreFilterPhase {

    private final StreamTransportService transportService;

    public CanMatchPreFilterPhase(StreamTransportService transportService) {
        this.transportService = transportService;
    }

    /**
     * Dispatches can-match requests to all targets in parallel.
     * Returns the filtered list of targets where canMatch=true.
     *
     * @param targets     all resolved shard targets
     * @param filterBytes serialized filter predicate
     * @param listener    receives filtered targets (only those that can match)
     */
    public void filter(List<ExecutionTarget> targets, byte[] filterBytes, ActionListener<List> listener) {
        if (targets.isEmpty() || filterBytes == null || filterBytes.length == 0) {
            listener.onResponse(targets);
            return;
        }

        List<ExecutionTarget> matching = new ArrayList<>(targets.size());
        AtomicInteger pending = new AtomicInteger(targets.size());

        for (ExecutionTarget target : targets) {
            // TODO: extract ShardId and DiscoveryNode from target
            // ShardId shardId = target.shardId();
            // DiscoveryNode node = target.node();
            //
            // AnalyticsCanMatchRequest request = new AnalyticsCanMatchRequest(shardId, filterBytes);
            // transportService.sendRequest(node, AnalyticsCanMatchAction.NAME, request, handler);
            //
            // handler.onResponse: if (response.canMatch()) synchronized { matching.add(target); }
            // if (pending.decrementAndGet() == 0) listener.onResponse(matching);
            //
            // handler.onFailure: synchronized { matching.add(target); } // conservative: include on error
            // if (pending.decrementAndGet() == 0) listener.onResponse(matching);

            // Placeholder: include all targets until wired
            synchronized (matching) {
                matching.add(target);
            }
            if (pending.decrementAndGet() == 0) {
                listener.onResponse(matching);
            }
        }
    }
}

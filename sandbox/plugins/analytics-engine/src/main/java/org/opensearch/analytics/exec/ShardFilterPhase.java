/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.action.ShardTarget;
import org.opensearch.analytics.planner.dag.Stage;

import java.util.List;

/**
 * Filter or reorder resolved target shards before dispatch.
 * Called by the stage scheduler after target resolution and before dispatch.
 * Operates on ShardTarget (post-resolution shard+node pairs), analogous
 * to CanMatch operating on SearchShardIterator.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface ShardFilterPhase {

    /**
     * Filter or reorder resolved target shards before dispatch.
     *
     * @param targets the resolved shard+node pairs
     * @param stage   the stage being dispatched
     * @return a filtered or reordered list of targets
     */
    List<ShardTarget> filter(List<ShardTarget> targets, Stage stage);

    /** Identity filter — returns the input list unchanged. */
    ShardFilterPhase IDENTITY = (targets, stage) -> targets;
}

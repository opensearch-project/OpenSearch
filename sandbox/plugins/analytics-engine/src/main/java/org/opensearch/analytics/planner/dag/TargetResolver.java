/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.opensearch.cluster.ClusterState;
import org.opensearch.common.Nullable;

import java.util.List;

/**
 * Resolves execution targets for a {@link Stage}. Attached at plan time during
 * DAG construction. Called lazily by the Scheduler just before dispatch.
 *
 * <p>Implementations:
 * <ul>
 *   <li>{@link ShardTargetResolver} — resolves shard targets from ClusterState</li>
 *   <li>{@link ComposableTargetResolver} — composes a {@link NodeSelector} with an
 *       {@link ExecutionTargetCreator} for dynamic allocation (shuffle, broadcast)</li>
 * </ul>
 *
 * @opensearch.internal
 */
public abstract class TargetResolver {

    /**
     * Resolves execution targets for this stage.
     *
     * @param clusterState  current cluster state
     * @param childManifest manifest returned by the child stage (null for leaf stages).
     *                      TODO: replace Object with a typed ShuffleManifest/BroadcastManifest
     *                      once shuffle and broadcast are implemented.
     */
    public abstract List<ExecutionTarget> resolve(ClusterState clusterState, @Nullable Object childManifest);
}

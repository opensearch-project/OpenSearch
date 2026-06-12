/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.cluster.ClusterState;

/**
 * Immutable per-query view of analytics-engine state, captured once at query entry.
 *
 * <p>Front-ends call {@link EngineContextProvider#getContext(ClusterState)} to obtain a
 * {@code QueryRequestContext} bound to a specific {@link ClusterState} snapshot, then thread
 * it through both schema construction <em>and</em> plan execution. This guarantees the
 * same cluster-state view is used for type resolution and runtime shard routing — without
 * it, two calls to {@code clusterService.state()} could see different snapshots between
 * planning and execution, yielding a plan that references indices the executor no longer
 * sees (or vice-versa).
 *
 * @opensearch.internal
 */
public record QueryRequestContext(ClusterState clusterState, SchemaPlus schema, String querySource) {

    public QueryRequestContext(ClusterState clusterState, SchemaPlus schema) {
        this(clusterState, schema, null);
    }
}

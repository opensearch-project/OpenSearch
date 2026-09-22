/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.ClusterState;

/**
 * Context provided by the analytics engine to front-end plugins.
 *
 * <p>Provides everything a front-end needs for query validation and planning:
 * <ul>
 *   <li>{@link #getContext()} — Calcite schema with tables/fields/types derived from cluster state</li>
 * </ul>
 *
 * <p>Front-ends do not need to know about cluster state or individual back-end
 * capabilities — this context encapsulates both.
 *
 * @opensearch.internal
 */
public interface EngineContextProvider {

    /**
     * Capture a per-query immutable view bound to the given cluster-state snapshot. The
     * returned context carries both the state and the schema built from it, guaranteeing
     * planner and executor see the same view of the cluster. Front-ends should call this
     * once at query entry (typically with {@code clusterService.state()}) and thread the
     * result through both schema-driven planning and {@code planExecutor.execute}.
     *
     * <p>Default implementation builds a fresh {@link SchemaPlus} from the supplied state via
     * {@link OpenSearchSchemaBuilder#buildSchema(ClusterState)}. Engine implementations that
     * already carry an {@code IndexNameExpressionResolver} should override this to reuse it.
     */
    default QueryRequestContext getContext(ClusterState clusterState) {
        return new QueryRequestContext(clusterState, OpenSearchSchemaBuilder.buildSchema(clusterState));
    }

    QueryRequestContext getContext();

    /**
     * Converts a backend-specific exception into an appropriate OpenSearch exception type.
     * Called at the coordinator when a query fails, before surfacing the error to the REST layer.
     * Default implementation performs no conversion.
     *
     * @param e the exception from query execution
     * @return converted exception with correct HTTP status semantics, or {@code e} unchanged
     */
    // TODO: not called by front-ends — only DefaultPlanExecutor invokes this on its own
    // EngineContextProvider. Move off the front-end-facing API surface or drop entirely.
    default Exception convertException(Exception e) {
        return e;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;

/**
 * Context provided by the analytics engine to front-end plugins.
 *
 * <p>Provides everything a front-end needs for query validation and planning:
 * <ul>
 *   <li>{@link #getSchema()} — Calcite schema with tables/fields/types derived from cluster state</li>
 *   <li>{@link #operatorTable()} — supported functions/operators aggregated from all back-end engines</li>
 * </ul>
 *
 * <p>Front-ends do not need to know about cluster state or individual back-end
 * capabilities — this context encapsulates both.
 *
 * @opensearch.internal
 */
public interface EngineContext {

    /**
     * Returns a Calcite schema reflecting the current cluster state.
     * Tables and field types are resolved from index mappings.
     */
    SchemaPlus getSchema();

    /**
     * Returns the operator table containing all functions supported
     * by at least one registered back-end engine.
     */
    SqlOperatorTable operatorTable();

    /**
     * Converts a backend-specific exception into an appropriate OpenSearch exception type.
     * Called at the coordinator when a query fails, before surfacing the error to the REST layer.
     * Default implementation performs no conversion.
     *
     * @param e the exception from query execution
     * @return converted exception with correct HTTP status semantics, or {@code e} unchanged
     */
    default Exception convertException(Exception e) {
        return e;
    }
}

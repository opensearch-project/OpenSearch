/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;

/**
 * Optional capability interface for {@link AnalyticsSearchBackendPlugin}
 * implementations that can provide a full {@link SearchExecEngine} with prepare/execute/stream
 * semantics for the analytics query path.
 * <p>
 * Plugins that implement this interface are used by the analytics executor for the
 * complete query lifecycle.
 *
 * @opensearch.internal
 */
public interface SearchExecEngineProvider {

    /** Unique engine name (e.g., "datafusion", "lucene"). */
    String name();

    /**
     * Creates a full search execution engine bound to the given execution context.
     * The context carries the reader snapshot and task metadata.
     *
     * @param ctx the execution context
     * @return a search execution engine for the analytics query path
     */
    SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx);
}

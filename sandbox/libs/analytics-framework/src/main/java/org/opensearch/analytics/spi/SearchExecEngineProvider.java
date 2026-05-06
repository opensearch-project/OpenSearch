/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;

/**
 * Execution engine factory for backend plugins.
 * Creates a {@link SearchExecEngine} bound to a given execution context for the analytics query path.
 *
 * @opensearch.internal
 */
public interface SearchExecEngineProvider {

    /**
     * Creates a search execution engine bound to the given execution context.
     * The context carries the reader snapshot and task metadata.
     * The backendContext carries backend-specific state produced by instruction handlers.
     */
    SearchExecEngine<ShardScanExecutionContext, EngineResultStream> createSearchExecEngine(
        ShardScanExecutionContext ctx,
        BackendExecutionContext backendContext
    );
}

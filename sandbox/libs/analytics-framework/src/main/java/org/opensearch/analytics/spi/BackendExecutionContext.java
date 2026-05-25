/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.io.IOException;

/**
 * Backend-specific execution context that flows between successive instruction handler
 * calls. The first handler in the chain receives {@code null} and bootstraps the context;
 * subsequent handlers receive and build upon the previous handler's output.
 *
 * <p>Each backend defines its own concrete implementation (e.g.,
 * {@code DataFusionSessionState} holding a native SessionContext handle).
 *
 * <h2>Lifecycle</h2>
 * <p>Extends {@link AutoCloseable} with a narrowed {@code throws IOException} signature so
 * backends can attach native / resource-holding handles to the context and rely on the
 * orchestrator (e.g. {@code AnalyticsSearchService} or {@code LocalStageExecutionFactory}) to
 * close it if the fragment aborts before ownership is transferred to the
 * {@code SearchExecEngine}. Implementations that hold no resources should leave the default
 * no-op {@link #close()}. {@code close()} must be idempotent; in particular it must
 * tolerate being called after the resources have already been handed off to a
 * successfully-constructed engine.
 *
 * @opensearch.internal
 */
public interface BackendExecutionContext extends AutoCloseable {
    @Override
    default void close() throws IOException {
        // Default: no resources to release.
    }
}

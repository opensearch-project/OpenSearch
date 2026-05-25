/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;

/**
 * Backend-specific execution context produced by {@link ShardScanInstructionHandler},
 * consumed by {@link DatafusionSearcher} at execute time.
 *
 * <p>{@link #close()} closes the underlying {@link SessionContextHandle} as the
 * fragment-orchestrator's safety net for error paths that never reach the execute step.
 * The handle's close is idempotent and cooperates with {@link DatafusionContext#close()}
 * (which also closes it once the handle is handed off to an engine), so it is safe to call
 * from both places — whichever runs first wins.
 */
public record DataFusionSessionState(SessionContextHandle sessionContextHandle) implements BackendExecutionContext {

    @Override
    public void close() {
        if (sessionContextHandle != null) {
            sessionContextHandle.close();
        }
    }
}

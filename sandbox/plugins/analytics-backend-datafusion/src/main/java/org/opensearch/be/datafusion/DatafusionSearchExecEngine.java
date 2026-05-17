/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * DataFusion-backed search execution engine.
 * <p>
 * Delegates execution to the native DataFusion runtime via {@link DatafusionSearcher}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine<ShardScanExecutionContext, EngineResultStream> {

    private final DatafusionContext datafusionContext;

    public DatafusionSearchExecEngine(DatafusionContext datafusionContext) {
        this.datafusionContext = datafusionContext;
    }

    @Override
    public void prepare(ShardScanExecutionContext requestContext) {
        byte[] substraitBytes = requestContext.getFragmentBytes();
        long contextId = datafusionContext.task() != null ? datafusionContext.task().getId() : 0L;
        datafusionContext.setDatafusionQuery(new DatafusionQuery(requestContext.getTableName(), substraitBytes, contextId));
    }

    @Override
    public EngineResultStream execute(ShardScanExecutionContext requestContext) throws IOException {
        BufferAllocator allocator = requestContext.getAllocator();
        if (allocator == null) {
            throw new IllegalStateException("ExecutionContext.allocator must be set by the caller before execute()");
        }

        // Register cancellation hook so HTTP disconnect / _tasks/_cancel / timeout
        // immediately fires the Rust CancellationToken.
        long contextId = datafusionContext.getContextId();
        AnalyticsShardTask shardTask = datafusionContext.task() instanceof AnalyticsShardTask t ? t : null;
        if (shardTask != null) {
            shardTask.setCancellationListener(() -> NativeBridge.cancelQuery(contextId));
        }

        try {
            DatafusionSearcher searcher = datafusionContext.getSearcher();
            searcher.search(datafusionContext);
            StreamHandle handle = datafusionContext.takeStreamHandle();
            return new DatafusionResultStream(handle, allocator);
        } finally {
            // Clear the listener so the task doesn't hold a reference after the query.
            if (shardTask != null) {
                shardTask.clearCancellationListener();
            }
        }
    }

    @Override
    public void close() throws IOException {
        datafusionContext.close();
    }
}

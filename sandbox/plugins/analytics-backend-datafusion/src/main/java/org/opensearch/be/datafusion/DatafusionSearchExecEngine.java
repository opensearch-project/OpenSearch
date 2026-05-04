/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * DataFusion-backed search execution engine.
 * <p>
 * Delegates execution to the native DataFusion runtime via {@link DatafusionSearcher}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine<ExecutionContext, EngineResultStream> {

    private static final Logger logger = LogManager.getLogger(DatafusionSearchExecEngine.class);

    private final DatafusionContext datafusionContext;
    private final Supplier<BufferAllocator> allocatorFactory;

    /**
     * Creates an execution engine backed by the given DataFusion context.
     * @param datafusionContext the DataFusion execution context
     * @param allocatorFactory factory for creating a child allocator for result stream memory
     */
    public DatafusionSearchExecEngine(DatafusionContext datafusionContext, Supplier<BufferAllocator> allocatorFactory) {
        this.datafusionContext = datafusionContext;
        this.allocatorFactory = allocatorFactory;
    }

    @Override
    public void prepare(ExecutionContext requestContext) {
        byte[] substraitBytes = requestContext.getFragmentBytes();
        long contextId = datafusionContext.task() != null ? datafusionContext.task().getId() : 0L;
        datafusionContext.setDatafusionQuery(new DatafusionQuery(requestContext.getTableName(), substraitBytes, contextId));
    }

    @Override
    public EngineResultStream execute(ExecutionContext requestContext) throws IOException {
        DatafusionSearcher searcher = datafusionContext.getSearcher();
        searcher.search(datafusionContext);
        StreamHandle handle = datafusionContext.takeStreamHandle();
        BufferAllocator allocator = allocatorFactory.get();
        return new DatafusionResultStream(handle, allocator);
    }

    @Override
    public void close() throws IOException {
        datafusionContext.close();
    }
}

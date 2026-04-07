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
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.be.datafusion.jni.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;

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
        // TODO: wire Substrait conversion (RelNode → Substrait bytes)
        byte[] substraitBytes = null;
        datafusionContext.setDatafusionQuery(new DatafusionQuery(requestContext.getTableName(), substraitBytes));
    }

    @Override
    public EngineResultStream execute(ExecutionContext requestContext) throws IOException {
        DatafusionSearcher searcher = datafusionContext.getSearcher();
        searcher.search(datafusionContext);
        StreamHandle handle = datafusionContext.takeStreamHandle();
        BufferAllocator allocator = allocatorFactory.get();
        return new DatafusionResultStream(handle, allocator);
    }

    /**
     * Executes a boolean tree query by passing the serialized tree and
     * bridge context ID through JNI to the Rust layer.
     * <p>
     * The Rust side deserializes the tree, creates {@code JniTreeShardSearcher}
     * per collector leaf (using the contextId + providerId for JNI callbacks
     * through {@link org.opensearch.index.engine.exec.FilterTreeCallbackBridge}),
     * builds a {@code TreeIndexedTableProvider}, and returns a stream pointer.
     *
     * @param treeBytes      serialized boolean tree bytes
     * @param contextId      context ID registered with FilterTreeCallbackBridge
     * @param substraitBytes serialized substrait plan bytes
     * @param dfContext       the DataFusion execution context
     * @param listener       callback receiving the stream pointer (Long) or error
     */
    public void executeTreeQuery(
        byte[] treeBytes,
        long contextId,
        byte[] substraitBytes,
        DatafusionContext dfContext,
        ActionListener<Long> listener
    ) {
        if (dfContext == null) {
            listener.onFailure(new IllegalArgumentException("DatafusionContext is required for tree query execution"));
            return;
        }
        NativeBridge.executeTreeQueryAsync(
            treeBytes,
            contextId,
            new long[0],   // segmentMaxDocs — Rust resolves via JNI callbacks
            new String[0], // parquetPaths — Rust resolves via reader
            dfContext.getDatafusionQuery() != null ? dfContext.getDatafusionQuery().getIndexName() : "unknown",
            substraitBytes,
            1,     // numPartitions — TODO: make configurable
            0,     // indexLeafCount — Rust reads from tree
            false, // isQueryPlanExplainEnabled
            dfContext.getNativeRuntime().get(),
            listener
        );
    }

    @Override
    public void close() throws IOException {
        datafusionContext.close();
    }
}

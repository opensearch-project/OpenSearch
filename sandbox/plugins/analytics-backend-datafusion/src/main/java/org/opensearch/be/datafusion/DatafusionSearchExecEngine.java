/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * DataFusion-backed {@link SearchExecEngine} implementation.
 * <p>
 * Handles both standard query execution and boolean tree query execution.
 * Tree queries are delegated to the Rust layer via {@link NativeBridge#executeTreeQueryAsync}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine<ExecutionContext, EngineResultStream> {

    private final DatafusionContext datafusionContext;

    /**
     * Creates an execution engine backed by the given DataFusion context.
     * @param datafusionContext the DataFusion execution context
     */
    public DatafusionSearchExecEngine(DatafusionContext datafusionContext) {
        this.datafusionContext = datafusionContext;
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
        return new DatafusionResultStream(datafusionContext.getStreamHandle());
    }

     * Executes a boolean tree query by passing the serialized tree and
     * bridge context ID through JNI to the Rust layer.
     * <p>
     * The Rust side deserializes the tree, creates {@code JniTreeShardSearcher}
     * per collector leaf (using the contextId + providerId for JNI callbacks
     * through {@link org.opensearch.index.engine.exec.FilterTreeCallbackBridge}),
     * builds a {@code TreeIndexedTableProvider}, and returns a stream pointer.
     */
    @Override
    public void executeTreeQuery(byte[] treeBytes, long contextId, DatafusionContext context,
            ActionListener<Long> listener) {
        if (context == null) {
            listener.onFailure(new IllegalArgumentException("DatafusionContext is required for tree query execution"));
            return;
        }
        NativeBridge.executeTreeQueryAsync(
            treeBytes,
            contextId,
            new long[0],   // segmentMaxDocs — Rust resolves via JNI callbacks
            new String[0], // parquetPaths — Rust resolves via reader
            context.getTableName(),
            context.getSubstraitPlan(),
            1,     // numPartitions — TODO: make configurable
            0,     // indexLeafCount — Rust reads from tree
            false, // isQueryPlanExplainEnabled
            context.getRuntimePtr(),
            listener
        );
    }

    @Override
    public void execute(DatafusionContext context) throws IOException {
        context.executeQuery();
    }

    @Override
    public void close() throws IOException {
        // Runtime lifecycle is managed by the plugin, not the engine
    }
}

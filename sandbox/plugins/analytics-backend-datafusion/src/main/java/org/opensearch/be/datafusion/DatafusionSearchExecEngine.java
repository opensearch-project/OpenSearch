/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * DataFusion-backed search execution engine.
 * <p>
 * Converts logical plan fragments to Substrait, executes them via the native
 * DataFusion runtime, and returns results as a {@link DatafusionResultStream}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine<DatafusionContext, byte[], DatafusionResultStream> {

    private final NativeRuntimeHandle nativeRuntime;

    public DatafusionSearchExecEngine(NativeRuntimeHandle nativeRuntime, DataFormat dataFormat) {
        this.nativeRuntime = nativeRuntime;
    }

    @Override
    public byte[] convertFragment(Object fragment) {
        // TODO: wire Substrait conversion (RelNode → Substrait bytes)
        throw new UnsupportedOperationException("Substrait conversion not yet wired");
    }

    @Override
    public DatafusionContext createContext(
        Object reader,
        byte[] plan,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        SearchShardTask task
    ) throws IOException {
        DatafusionReader dfReader = (DatafusionReader) reader;
        DatafusionContext context = new DatafusionContext(request, shardTarget, dfReader, nativeRuntime);
        context.setDatafusionQuery(new DatafusionQuery("", plan));
        return context;
    }

    @Override
    public DatafusionResultStream execute(DatafusionContext context) throws IOException {
        DatafusionSearcher searcher = context.getEngineSearcher();
        searcher.search(context);
        return new DatafusionResultStream(context.getStreamHandle());
    }
}

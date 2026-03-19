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
import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * DataFusion-backed search execution engine.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine<DatafusionContext, byte[]> {

    private final NativeRuntimeHandle nativeRuntime;

    public DatafusionSearchExecEngine(NativeRuntimeHandle nativeRuntime, DataFormat dataFormat) {
        this.nativeRuntime = nativeRuntime;
    }

    @Override
    public void execute(DatafusionContext context) throws IOException {
        DatafusionSearcher searcher = context.getEngineSearcher();
        IndexFilterTree filterTree = context.getFilterTree();
        if (filterTree != null) {
            throw new UnsupportedOperationException("Indexed query path not yet wired");
        } else {
            searcher.search(context);
        }
    }

    @Override
    public DatafusionContext createContext(
        Object reader,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        SearchShardTask task
    ) throws IOException {
        DatafusionReader dfReader = (DatafusionReader) reader;
        return new DatafusionContext(request, shardTarget, dfReader);
    }

    @Override
    public byte[] convertFragment(Object fragment) {
        throw new UnsupportedOperationException("Substrait conversion not yet wired");
    }

    @Override
    public Iterator<?> executePlan(byte[] plan, DatafusionContext context) {
        try {
            context.setDatafusionQuery(new DatafusionQuery("", plan));
            execute(context);
            return Collections.emptyIterator();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

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
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * DataFusion-backed {@link SearchExecEngine}.
 * Plan type is {@code byte[]} (substrait bytes).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine<DatafusionContext, byte[]> {

    private final DatafusionReaderManager readerManager;
    private final long runtimePtr;
    private long nextContextId;

    public DatafusionSearchExecEngine(long runtimePtr, DataFormat dataFormat, ShardPath shardPath) {
        readerManager = new DatafusionReaderManager(dataFormat, shardPath);
        this.runtimePtr = runtimePtr;
    }

    // TODO : figure out stream return type similar to engine bridge
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
        CatalogSnapshot snapshot,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        SearchShardTask task
    ) throws IOException {
        return new DatafusionContext(snapshot, request, shardTarget, readerManager);
    }

    @Override
    public byte[] convertFragment(Object fragment) {
        // TODO: SubstraitConverter.toBytes((RelNode) fragment)
        throw new UnsupportedOperationException("Substrait conversion not yet wired");
    }

    @Override
    public Iterator<?> executePlan(byte[] plan, DatafusionContext context) {
        try {
            context.setDatafusionQuery(new DatafusionQuery("", plan));
            execute(context);
            // TODO results
            return Collections.emptyIterator();
            // return results == null ? Collections.emptyIterator() : Collections.singleton(results).iterator();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public EngineReaderManager<?> getReaderManager() {
        return readerManager;
    }
}

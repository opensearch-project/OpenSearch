/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.EngineReaderManager;
import org.opensearch.index.engine.EngineSearcherSupplier;
import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.index.engine.SearchExecEngine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

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

    public DatafusionSearchExecEngine(DatafusionReaderManager readerManager, long runtimePtr) {
        this.readerManager = readerManager;
        this.runtimePtr = runtimePtr;
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
        org.opensearch.search.internal.ReaderContext readerContext,
        org.opensearch.search.internal.ShardSearchRequest request,
        org.opensearch.search.SearchShardTarget shardTarget,
        org.opensearch.action.search.SearchShardTask task,
        org.opensearch.common.util.BigArrays bigArrays
    ) throws IOException {
        DatafusionSearcher searcher = (DatafusionSearcher) acquireSearcher("create-context");
        return new DatafusionContext(
            new org.opensearch.search.internal.ShardSearchContextId("", nextContextId++),
            request,
            shardTarget,
            searcher
        );
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
            Map<String, Object[]> results = context.getDatafusionResults();
            return results == null ? Collections.emptyIterator() : Collections.singleton(results).iterator();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier() throws IOException {
        DatafusionReader reader = readerManager.acquire();
        return new EngineSearcherSupplier<>() {
            @Override
            protected DatafusionSearcher acquireSearcherInternal(String source) {
                return new DatafusionSearcher(source, reader.getReaderPtr(), () -> {
                    try {
                        readerManager.release(reader);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }

            @Override
            protected void doClose() {
                try {
                    readerManager.release(reader);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    @Override
    public EngineReaderManager<?> getReferenceManager() {
        return readerManager;
    }
}

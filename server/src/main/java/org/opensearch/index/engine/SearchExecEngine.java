/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Shard-level search execution engine interface.
 *
 * @param <C> the engine-specific context type
 * @param <T> the engine-native plan type (e.g. byte[] for substrait)
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchExecEngine<C extends SearchExecutionContext, T> extends Closeable {

    void execute(C context) throws IOException;

    default void execute(C context, ActionListener<C> listener) {
        try {
            execute(context);
            listener.onResponse(context);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    C createContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        SearchShardTask task,
        BigArrays bigArrays
    ) throws IOException;

    default T convertFragment(Object fragment) {
        throw new UnsupportedOperationException("convertFragment not supported by " + getClass().getSimpleName());
    }

    default Iterator<?> executePlan(T plan, C context) {
        throw new UnsupportedOperationException("executePlan not supported by " + getClass().getSimpleName());
    }

    EngineSearcherSupplier<?> acquireSearcherSupplier() throws IOException;

    default EngineSearcher<?> acquireSearcher(String source) throws IOException {
        return acquireSearcherSupplier().acquireSearcher(source);
    }

    EngineReaderManager<?> getReferenceManager();

    @Override
    default void close() throws IOException {}
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.Closeable;
import java.io.IOException;

/**
 * Shard-level search execution engine interface.
 *
 * @param <C> the engine-specific context type
 * @param <T> the engine-native plan type (e.g. byte[] for substrait)
 * @param <S> the result stream type returned by {@link #execute}
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchExecEngine<C extends SearchExecutionContext, T, S> extends Closeable {

    /**
     * Converts a logical plan fragment into the engine's native plan format.
     */
    default T convertFragment(Object fragment) {
        throw new UnsupportedOperationException("convertFragment not supported by " + getClass().getSimpleName());
    }

    /**
     * Creates a search context bound to the given reader and plan.
     * The reader is provided by {@link DataFormatAwareEngine}
     * which owns all reader managers.
     */
    C createContext(Object reader, T plan, ShardSearchRequest request, SearchShardTarget shardTarget, SearchShardTask task)
        throws IOException;

    /**
     * Executes the plan held by the context and returns the result stream.
     */
    S execute(C context) throws IOException;

    @Override
    default void close() throws IOException {}
}

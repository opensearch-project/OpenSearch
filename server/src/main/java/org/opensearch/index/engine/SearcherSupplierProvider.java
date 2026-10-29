/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;

import java.util.function.Function;

/**
 * Optional capability of an {@link org.opensearch.index.engine.exec.Indexer} that can serve the standard
 * {@code _search} path. {@code IndexShard} checks for it with {@code instanceof}; an indexer without it
 * rejects searches with a clear error.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearcherSupplierProvider {

    /**
     * Acquires a point-in-time {@link Engine.SearcherSupplier}. The supplier pins the reader and the store until
     * it is closed, so an open scroll or PIT survives engine close.
     *
     * @param wrapper applied to each acquired {@link Engine.Searcher} (e.g. reader wrapping)
     * @param scope   the searcher scope
     * @return a searcher supplier whose {@code close()} releases the underlying reader resources
     */
    Engine.SearcherSupplier acquireSearcherSupplier(Function<Engine.Searcher, Engine.Searcher> wrapper, Engine.SearcherScope scope)
        throws EngineException;

    /** The {@code 400} returned when a shard cannot serve {@code _search}, with {@code reason} saying why. */
    static OpenSearchException searchNotSupported(ShardId shardId, String reason) {
        return new OpenSearchStatusException("Shard {} does not support _search: {}", RestStatus.BAD_REQUEST, shardId, reason);
    }
}

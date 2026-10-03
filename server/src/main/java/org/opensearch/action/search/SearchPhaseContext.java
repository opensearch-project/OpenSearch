/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.Logger;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.transport.Transport;

import java.util.concurrent.Executor;

/**
 * This interface provides contextual state and access to resources across multiple search phases.
 * It combines the {@link SearchCoordinator} role (phase transitions, error routing, response delivery)
 * with shared resource access (connections, transport, executor).
 *
 * @opensearch.internal
 */
@InternalApi
public interface SearchPhaseContext extends SearchCoordinator, Executor {
    // TODO maybe we can make this concrete later - for now we just implement this in the base class for all initial phases

    /**
     * Returns the total number of shards to the current search across all indices
     */
    int getNumShards();

    /**
     * Returns a logger for this context to prevent each individual phase to create their own logger.
     */
    Logger getLogger();

    /**
     * Returns the currently executing search task
     */
    SearchTask getTask();

    /**
     * Returns the currently executing search request
     */
    SearchRequest getRequest();

    /**
     * Returns a connection to the node if connected otherwise and {@link org.opensearch.transport.ConnectTransportException} will be
     * thrown.
     */
    Transport.Connection getConnection(String clusterAlias, String nodeId);

    /**
     * Returns the {@link SearchTransportService} to send shard request to other nodes
     */
    SearchTransportService getSearchTransport();

    /**
     * Releases a search context with the given context ID on the node the given connection is connected to.
     * @see org.opensearch.search.query.QuerySearchResult#getContextId()
     * @see org.opensearch.search.fetch.FetchSearchResult#getContextId()
     *
     */
    default void sendReleaseSearchContext(
        ShardSearchContextId contextId,
        Transport.Connection connection,
        OriginalIndices originalIndices
    ) {
        if (connection != null) {
            getSearchTransport().sendFreeContext(connection, contextId, originalIndices);
        }
    }

    /**
     * Builds an request for the initial search phase.
     */
    ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt);

    /**
     * Registers a {@link Releasable} that will be closed when the search request finishes or fails.
     */
    void addReleasable(Releasable releasable);
}

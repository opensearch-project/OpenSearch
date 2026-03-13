/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;

import java.io.Closeable;

/**
 * Engine-agnostic search execution context.
 * <p>
 * This is the minimal contract between {@link org.opensearch.index.engine.SearchExecEngine}
 * and the transport/coordination layer ({@code SearchService}).
 * <p>
 * Contains only what callers actually need: request, results, pagination, and lifecycle.
 * Engine-specific state (Lucene query, DF substrait plan, searcher, etc.) lives in
 * the engine's own context subtype.
 * <p>
 * {@link org.opensearch.search.internal.SearchContext} extends this to add Lucene-specific
 * methods for backward compatibility.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchExecutionContext extends Closeable {

    /**
     * Unique id for this search context within the shard.
     */
    ShardSearchContextId id();

    /**
     * The shard-level search request.
     */
    ShardSearchRequest request();

    /**
     * The shard this search targets.
     */
    SearchShardTarget shardTarget();

    /**
     * Query phase results.
     */
    QuerySearchResult queryResult();

    /**
     * Fetch phase results.
     */
    FetchSearchResult fetchResult();

    /**
     * Pagination: starting offset.
     */
    int from();

    /**
     * Pagination: number of hits to return.
     */
    int size();

    /**
     * Document IDs to fetch (set between query and fetch phases).
     */
    int[] docIdsToLoad();

    /**
     * Set document IDs for the fetch phase.
     */
    void docIdsToLoad(int[] docIds, int from, int size);
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.Closeable;

/**
 * Engine-agnostic search execution context.
 * <p>
 * This is the minimal contract between {@link SearchExecEngine}
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

    CatalogSnapshot catalogSnapshot();

    /**
     * The shard-level search request.
     */
    ShardSearchRequest request();

    /**
     * The shard this search targets.
     */
    SearchShardTarget shardTarget();
}

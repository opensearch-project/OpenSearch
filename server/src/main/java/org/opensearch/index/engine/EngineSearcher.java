/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.search.SearchExecutionContext;

import java.io.IOException;

/**
 * Engine-agnostic searcher interface.
 * <p>
 * Each engine implementation provides its own searcher that knows how to
 * execute queries against its reader. The searcher is acquired from
 * {@link SearchExecEngine} and used to execute searches against a
 * point-in-time snapshot.
 *
 * @param <C> the context type this searcher operates on
 * @opensearch.experimental
 */
@ExperimentalApi
public interface EngineSearcher<C extends SearchExecutionContext> extends Releasable {

    /**
     * The source that caused this searcher to be acquired (for debugging/tracking).
     */
    String source();

    /**
     * Execute a search using this searcher, populating results on the context.
     */
    void search(C context) throws IOException;
}

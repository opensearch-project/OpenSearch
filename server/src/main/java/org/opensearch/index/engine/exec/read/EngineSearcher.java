/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.read;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;

import java.io.IOException;

/**
 * Base class for read engine searcher
 * @param <Q> Query type
 */
@ExperimentalApi
public interface EngineSearcher<Q> extends Releasable {

    /**
     * The source that caused this searcher to be acquired.
     */
    String source();

    /**
     * Perform search operation in the engine for the provided input query
     */
    default void search(Q query) throws IOException {
        throw new UnsupportedOperationException();
    }
}

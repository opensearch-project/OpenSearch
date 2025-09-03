/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.search.ReferenceManager;

import java.io.IOException;

public interface EngineReaderManager<T> {
    T acquire() throws IOException;

    void release(T reader) throws IOException;

    default void addListener(ReferenceManager.RefreshListener listener) {
        // no-op
    }
}

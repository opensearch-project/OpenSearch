/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import java.util.Iterator;

/**
 * A closeable stream of record batches returned by engine execution.
 * Callers iterate batches via the returned iterator and MUST close the stream
 * when done to release native resources.
 *
 * @opensearch.internal
 */
public interface EngineResultStream extends AutoCloseable {

    /**
     * Returns an iterator over the record batches in this stream.
     * Each call returns the same iterator instance — the stream is single-pass.
     */
    Iterator<EngineResultBatch> iterator();

    @Override
    void close();
}

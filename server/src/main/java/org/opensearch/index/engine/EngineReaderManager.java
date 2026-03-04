/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Engine-agnostic reader manager.
 * <p>
 * For Lucene, wraps {@code ReferenceManager<OpenSearchDirectoryReader>}.
 * For pluggable engines, wraps the engine-specific reader lifecycle.
 *
 * @param <T> the reader type managed by this instance
 * @opensearch.experimental
 */
@ExperimentalApi
public interface EngineReaderManager<T> {

    T acquire() throws IOException;

    void release(T reader) throws IOException;
}

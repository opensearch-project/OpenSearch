/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Provides source-field data for a given data format.
 *
 * @param <C>       the context type
 * @param <R>       the result batch type
 * @param <ReaderT> the engine-specific reader type
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SourceProvider<C extends SourceContext, R, ReaderT> extends Closeable {

    C createContext(Object query, ReaderT reader) throws IOException;

    Iterator<R> execute(C context) throws IOException;
}

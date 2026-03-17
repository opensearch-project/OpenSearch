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

/**
 * @param <C> the context type
 * @param <R> the result batch type
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SourceProvider<C extends SourceContext, R> extends Closeable {

    C createContext(Object query, Object reader) throws IOException;

    Object execute(C context) throws IOException;

    R next(C context, Object stream) throws IOException;
}

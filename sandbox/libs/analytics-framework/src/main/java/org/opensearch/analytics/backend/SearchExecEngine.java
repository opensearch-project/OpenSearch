/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;

/**
 * Shard-level search execution engine interface.
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchExecEngine<T, V> extends Closeable {
    /**
     * Creates an execution context from a resolved plan.
     *
     * @param context               ExecutionContext
     */
    void prepare(T context);

    /**
     * Executes the context and returns a result stream.
     * @param context the execution context
     */
    V execute(T context) throws IOException;

    @Override
    default void close() throws IOException {}
}

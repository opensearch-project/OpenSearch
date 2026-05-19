/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io;

/**
 * Interface for factory to provide handler implementation for type {@link T}
 * @param <T> The type of content to be read/written to stream
 *
 * @opensearch.internal
 */
public interface IndexIOStreamHandlerFactory<T> {

    /**
     * Implements logic to provide handler based on the stream versions
     * @param version stream version
     * @return Handler for reading/writing content streams to/from - {@link T}
     */
    IndexIOStreamHandler<T> getHandler(int version);
}

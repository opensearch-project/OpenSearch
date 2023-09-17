/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common.helpers;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchWrapperException;

/**
 * An exception that indicates an error occurred while processing a {@link SearchRequestMap}.
 */
public class SearchRequestMapProcessingException extends OpenSearchException implements OpenSearchWrapperException {

    /**
     * Constructs a new SearchRequestMapProcessingException with the specified message.
     *
     * @param msg The error message.
     * @param args Arguments to substitute in the error message.
     */
    public SearchRequestMapProcessingException(String msg, Object... args) {
        super(msg, args);
    }

    /**
     * Constructs a new SearchRequestMapProcessingException with the specified message and cause.
     *
     * @param msg The error message.
     * @param cause The cause of the exception.
     * @param args Arguments to substitute in the error message.
     */
    public SearchRequestMapProcessingException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}

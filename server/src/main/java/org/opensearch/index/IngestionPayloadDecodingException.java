/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Thrown by {@link IngestionPayloadDecoder#decode} when a message payload cannot be decoded.
 */
@ExperimentalApi
public class IngestionPayloadDecodingException extends RuntimeException {

    public IngestionPayloadDecodingException(String message) {
        super(message);
    }

    public IngestionPayloadDecodingException(String message, Throwable cause) {
        super(message, cause);
    }
}

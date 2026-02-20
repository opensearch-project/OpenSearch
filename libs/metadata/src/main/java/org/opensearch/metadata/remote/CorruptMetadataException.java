/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.remote;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Exception thrown when metadata is corrupted or fails checksum validation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CorruptMetadataException extends IOException {

    /**
     * Creates a new CorruptMetadataException with the specified message.
     *
     * @param message the detail message
     */
    public CorruptMetadataException(String message) {
        super(message);
    }

    /**
     * Creates a new CorruptMetadataException with the specified message and cause.
     *
     * @param message the detail message
     * @param cause   the cause of this exception
     */
    public CorruptMetadataException(String message, Throwable cause) {
        super(message, cause);
    }
}

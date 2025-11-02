/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import java.io.IOException;

/**
 * Exception thrown when primary term-based routing operations fail.
 * Provides detailed context about the failure including error type,
 * primary term, and filename for debugging purposes.
 *
 * @opensearch.internal
 */
public class PrimaryTermRoutingException extends IOException {

    private final ErrorType errorType;
    private final long primaryTerm;
    private final String filename;

    /**
     * Types of errors that can occur during primary term routing.
     */
    public enum ErrorType {
        /** Error accessing primary term from IndexShard */
        PRIMARY_TERM_ACCESS_ERROR,
        
        /** Error creating or accessing primary term directory */
        DIRECTORY_CREATION_ERROR,
        
        /** Error routing file to appropriate directory */
        FILE_ROUTING_ERROR,
        
        /** Error validating directory accessibility */
        DIRECTORY_VALIDATION_ERROR
    }

    /**
     * Creates a new PrimaryTermRoutingException.
     *
     * @param message the error message
     * @param errorType the type of error that occurred
     * @param primaryTerm the primary term associated with the error
     * @param filename the filename associated with the error, may be null
     * @param cause the underlying cause, may be null
     */
    public PrimaryTermRoutingException(String message, ErrorType errorType, long primaryTerm, String filename, Throwable cause) {
        super(buildDetailedMessage(message, errorType, primaryTerm, filename), cause);
        this.errorType = errorType;
        this.primaryTerm = primaryTerm;
        this.filename = filename;
    }

    /**
     * Creates a new PrimaryTermRoutingException without a filename.
     *
     * @param message the error message
     * @param errorType the type of error that occurred
     * @param primaryTerm the primary term associated with the error
     * @param cause the underlying cause, may be null
     */
    public PrimaryTermRoutingException(String message, ErrorType errorType, long primaryTerm, Throwable cause) {
        this(message, errorType, primaryTerm, null, cause);
    }

    /**
     * Creates a new PrimaryTermRoutingException without a cause.
     *
     * @param message the error message
     * @param errorType the type of error that occurred
     * @param primaryTerm the primary term associated with the error
     * @param filename the filename associated with the error, may be null
     */
    public PrimaryTermRoutingException(String message, ErrorType errorType, long primaryTerm, String filename) {
        this(message, errorType, primaryTerm, filename, null);
    }

    /**
     * Gets the error type.
     *
     * @return the ErrorType
     */
    public ErrorType getErrorType() {
        return errorType;
    }

    /**
     * Gets the primary term associated with the error.
     *
     * @return the primary term
     */
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    /**
     * Gets the filename associated with the error.
     *
     * @return the filename, may be null
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Builds a detailed error message including context information.
     */
    private static String buildDetailedMessage(String message, ErrorType errorType, long primaryTerm, String filename) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(errorType).append("] ");
        sb.append(message);
        sb.append(" (primaryTerm=").append(primaryTerm);
        
        if (filename != null) {
            sb.append(", filename=").append(filename);
        }
        
        sb.append(")");
        return sb.toString();
    }
}
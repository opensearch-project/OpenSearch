/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.index.engine.exec.DataFormat;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Exception for multi-format store operations that provides comprehensive
 * error context including format, operation, and path information.
 */
public class MultiFormatStoreException extends IOException {
    
    private final DataFormat affectedFormat;
    private final String operation;
    private final Path attemptedPath;
    
    /**
     * Creates a new MultiFormatStoreException with comprehensive context
     * 
     * @param message the error message
     * @param affectedFormat the data format that was being operated on
     * @param operation the operation that failed
     * @param attemptedPath the path that was being accessed
     * @param cause the underlying cause of the exception
     */
    public MultiFormatStoreException(
        String message,
        DataFormat affectedFormat,
        String operation,
        Path attemptedPath,
        Throwable cause
    ) {
        super(formatMessage(message, affectedFormat, operation, attemptedPath), cause);
        this.affectedFormat = affectedFormat;
        this.operation = operation;
        this.attemptedPath = attemptedPath;
    }
    
    /**
     * Creates a new MultiFormatStoreException without an underlying cause
     * 
     * @param message the error message
     * @param affectedFormat the data format that was being operated on
     * @param operation the operation that failed
     * @param attemptedPath the path that was being accessed
     */
    public MultiFormatStoreException(
        String message,
        DataFormat affectedFormat,
        String operation,
        Path attemptedPath
    ) {
        this(message, affectedFormat, operation, attemptedPath, null);
    }
    
    /**
     * Formats the error message with comprehensive context information
     * 
     * @param message the base error message
     * @param format the affected data format
     * @param operation the failed operation
     * @param path the attempted path
     * @return formatted error message with context
     */
    private static String formatMessage(
        String message, 
        DataFormat format, 
        String operation, 
        Path path
    ) {
        return String.format(
            "Multi-format store operation failed: %s [format=%s, operation=%s, path=%s]",
            message, 
            format != null ? format.name() : "unknown", 
            operation != null ? operation : "unknown",
            path != null ? path.toString() : "unknown"
        );
    }
    
    /**
     * Returns the data format that was affected by this exception
     * 
     * @return the affected data format, or null if unknown
     */
    public DataFormat getAffectedFormat() { 
        return affectedFormat; 
    }
    
    /**
     * Returns the operation that failed
     * 
     * @return the failed operation name, or null if unknown
     */
    public String getOperation() { 
        return operation; 
    }
    
    /**
     * Returns the path that was being accessed when the exception occurred
     * 
     * @return the attempted path, or null if unknown
     */
    public Path getAttemptedPath() { 
        return attemptedPath; 
    }
}
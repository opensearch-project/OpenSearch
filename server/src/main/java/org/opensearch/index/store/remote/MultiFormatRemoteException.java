/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.opensearch.index.engine.exec.DataFormat;

import java.io.IOException;

/**
 * Exception for multi-format remote storage operations.
 * Provides detailed context including format, operation, and remote path information
 * to help with debugging and error resolution in multi-format remote storage scenarios.
 */
public class MultiFormatRemoteException extends IOException {
    
    private final DataFormat affectedFormat;
    private final String operation;
    private final String remotePath;
    
    /**
     * Creates a MultiFormatRemoteException with detailed context
     * 
     * @param message the error message
     * @param affectedFormat the data format involved in the operation
     * @param operation the operation that failed
     * @param remotePath the remote path involved
     * @param cause the underlying cause
     */
    public MultiFormatRemoteException(
        String message,
        DataFormat affectedFormat,
        String operation,
        String remotePath,
        Throwable cause
    ) {
        super(formatMessage(message, affectedFormat, operation, remotePath), cause);
        this.affectedFormat = affectedFormat;
        this.operation = operation;
        this.remotePath = remotePath;
    }
    
    /**
     * Creates a MultiFormatRemoteException without underlying cause
     * 
     * @param message the error message
     * @param affectedFormat the data format involved in the operation
     * @param operation the operation that failed
     * @param remotePath the remote path involved
     */
    public MultiFormatRemoteException(
        String message,
        DataFormat affectedFormat,
        String operation,
        String remotePath
    ) {
        this(message, affectedFormat, operation, remotePath, null);
    }
    
    /**
     * Formats the error message with format-specific context
     */
    private static String formatMessage(
        String message,
        DataFormat format,
        String operation,
        String remotePath
    ) {
        return String.format(
            "Multi-format remote storage operation failed: %s [format=%s, operation=%s, remotePath=%s]",
            message,
            format != null ? format.name() : "unknown",
            operation != null ? operation : "unknown",
            remotePath != null ? remotePath : "unknown"
        );
    }
    
    /**
     * Returns the data format that was affected by this operation
     * 
     * @return the affected DataFormat, or null if not specified
     */
    public DataFormat getAffectedFormat() {
        return affectedFormat;
    }
    
    /**
     * Returns the operation that failed
     * 
     * @return the operation name, or null if not specified
     */
    public String getOperation() {
        return operation;
    }
    
    /**
     * Returns the remote path involved in the failed operation
     * 
     * @return the remote path, or null if not specified
     */
    public String getRemotePath() {
        return remotePath;
    }
    
    /**
     * Creates a detailed error message for logging and debugging
     * 
     * @return formatted error message with all available context
     */
    public String getDetailedMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("MultiFormatRemoteException Details:\n");
        sb.append("  Message: ").append(getMessage()).append("\n");
        sb.append("  Affected Format: ").append(affectedFormat != null ? affectedFormat.name() : "unknown").append("\n");
        sb.append("  Failed Operation: ").append(operation != null ? operation : "unknown").append("\n");
        sb.append("  Remote Path: ").append(remotePath != null ? remotePath : "unknown").append("\n");
        
        if (getCause() != null) {
            sb.append("  Underlying Cause: ").append(getCause().getClass().getSimpleName())
              .append(" - ").append(getCause().getMessage()).append("\n");
        }
        
        return sb.toString();
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
               "format=" + (affectedFormat != null ? affectedFormat.name() : "null") +
               ", operation='" + operation + '\'' +
               ", remotePath='" + remotePath + '\'' +
               ", message='" + getMessage() + '\'' +
               '}';
    }
}

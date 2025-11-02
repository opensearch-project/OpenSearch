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
 * Exception thrown when operations on a distributed segment directory fail.
 * This exception provides additional context about which directory index
 * and operation caused the failure to aid in debugging and monitoring.
 *
 * @opensearch.internal
 */
public class DistributedDirectoryException extends IOException {

    private final int directoryIndex;
    private final String operation;

    /**
     * Creates a new DistributedDirectoryException with directory context.
     *
     * @param message the error message
     * @param directoryIndex the index of the directory where the error occurred (0-4)
     * @param operation the operation that was being performed when the error occurred
     * @param cause the underlying cause of the exception
     */
    public DistributedDirectoryException(String message, int directoryIndex, String operation, Throwable cause) {
        super(String.format("Directory %d operation '%s' failed: %s", directoryIndex, operation, message), cause);
        this.directoryIndex = directoryIndex;
        this.operation = operation;
    }

    /**
     * Creates a new DistributedDirectoryException with directory context.
     *
     * @param message the error message
     * @param directoryIndex the index of the directory where the error occurred (0-4)
     * @param operation the operation that was being performed when the error occurred
     */
    public DistributedDirectoryException(String message, int directoryIndex, String operation) {
        this(message, directoryIndex, operation, null);
    }

    /**
     * Gets the directory index where the error occurred.
     *
     * @return the directory index (0-4)
     */
    public int getDirectoryIndex() {
        return directoryIndex;
    }

    /**
     * Gets the operation that was being performed when the error occurred.
     *
     * @return the operation name
     */
    public String getOperation() {
        return operation;
    }
}
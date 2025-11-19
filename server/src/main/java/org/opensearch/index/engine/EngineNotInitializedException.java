/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

/**
 * Exception thrown when an engine operation is attempted before the engine is fully initialized.
 * This is a specific type of IllegalStateException that allows callers to handle initialization
 * issues separately from other illegal state conditions.
 *
 * @opensearch.internal
 */
public class EngineNotInitializedException extends IllegalStateException {
    
    /**
     * Constructs a new EngineNotInitializedException with the specified detail message.
     *
     * @param message the detail message explaining why the engine is not initialized
     */
    public EngineNotInitializedException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new EngineNotInitializedException with the specified detail message and cause.
     *
     * @param message the detail message explaining why the engine is not initialized
     * @param cause the underlying cause of the initialization failure
     */
    public EngineNotInitializedException(String message, Throwable cause) {
        super(message, cause);
    }
}

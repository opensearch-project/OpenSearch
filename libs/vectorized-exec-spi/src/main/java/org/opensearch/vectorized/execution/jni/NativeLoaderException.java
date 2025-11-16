/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.jni;

/**
 * Exception thrown when native library operations fail.
 * This includes errors during library loading, native method invocation,
 * or resource management failures.
 */
public class NativeLoaderException extends RuntimeException {

    /**
     * Constructs a new native exception with the specified detail message.
     * @param message the detail message
     */
    public NativeLoaderException(String message) {
        super(message);
    }

    /**
     * Constructs a new native exception with the specified detail message and cause.
     * @param message the detail message
     * @param cause the cause of this exception
     */
    public NativeLoaderException(String message, Throwable cause) {
        super(message, cause);
    }
}

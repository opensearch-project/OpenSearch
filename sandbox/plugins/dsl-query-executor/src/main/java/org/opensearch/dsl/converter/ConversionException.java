/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

/**
 * Thrown when DSL-to-Calcite conversion fails.
 */
public class ConversionException extends Exception {

    /**
     * Creates a conversion exception.
     *
     * @param message description of what failed
     */
    public ConversionException(String message) {
        super(message);
    }

    /**
     * Creates a conversion exception with a cause.
     *
     * @param message description of what failed
     * @param cause the underlying exception
     */
    public ConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}

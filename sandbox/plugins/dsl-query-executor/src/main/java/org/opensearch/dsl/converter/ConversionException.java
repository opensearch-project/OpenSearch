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
 *
 * <p>WHY: extends {@link IllegalArgumentException} so {@code ExceptionsHelper.status} maps it to
 * HTTP 400, matching vanilla {@code NumberFieldMapper}, which rejects the same inputs with an
 * {@code IllegalArgumentException}. A plain checked exception would fall through to 500.
 */
public class ConversionException extends IllegalArgumentException {

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

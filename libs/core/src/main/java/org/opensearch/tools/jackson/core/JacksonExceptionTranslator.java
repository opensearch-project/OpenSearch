/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.jackson.core;

import java.io.IOException;

import tools.jackson.core.JacksonException;
import tools.jackson.core.exc.InputCoercionException;
import tools.jackson.core.exc.StreamConstraintsException;
import tools.jackson.core.exc.StreamReadException;
import tools.jackson.core.exc.StreamWriteException;
import tools.jackson.core.exc.UnexpectedEndOfInputException;

/**
 * Translates Jackson 3 exception hierarchy to Jackson 2 compatible one
 */
public final class JacksonExceptionTranslator {
    private JacksonExceptionTranslator() {}

    /**
     * Translates Jackson 3 exception hierarchy to Jackson 2 compatible one
     * @param ex exception to translate
     * @return translated exception
     */
    public static IOException translateToIOExceptionOrRethrowReturning(JacksonException ex) {
        if (ex instanceof UnexpectedEndOfInputException) {
            return new org.opensearch.tools.jackson.core.UnexpectedEndOfInputException(ex.getMessage(), ex);
        } else if (ex instanceof InputCoercionException) {
            return new org.opensearch.tools.jackson.core.InputCoercionException(ex.getMessage(), ex);
        } else if (ex instanceof StreamReadException) {
            return new JsonParseException(ex.getMessage(), ex);
        } else if (ex instanceof StreamWriteException) {
            return new JsonGenerationException(ex.getMessage(), ex);
        } else if (ex instanceof StreamConstraintsException) {
            return new org.opensearch.tools.jackson.core.StreamConstraintsException(ex.getMessage(), ex);
        } else {
            return new IOException(ex);
        }
    }

    /**
     * Translates Jackson 3 exception hierarchy to Jackson 2 compatible one and throws it
     * @param ex exception to translate
     */
    public static void translateToIOExceptionOrRethrow(JacksonException ex) throws IOException {
        throw translateToIOExceptionOrRethrowReturning(ex);
    }
}

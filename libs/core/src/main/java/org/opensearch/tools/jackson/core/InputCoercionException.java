/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.jackson.core;

import java.io.IOException;

/**
 * Mirror of {@link tools.jackson.core.exc.InputCoercionException} that extends  {@link IOException}
 */
public class InputCoercionException extends JsonParseException {
    private static final long serialVersionUID = 1L;

    public InputCoercionException(String message, Throwable cause) {
        super(message, cause);
    }

    public InputCoercionException(String message) {
        super(message);
    }

    public InputCoercionException(Throwable cause) {
        super(cause);
    }
}

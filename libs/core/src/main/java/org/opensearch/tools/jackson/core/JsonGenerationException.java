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
 * Mirror of {@link tools.jackson.core.exc.StreamWriteException} that extends  {@link IOException}
 */
public class JsonGenerationException extends IOException {
    private static final long serialVersionUID = 1L;

    public JsonGenerationException(String message, Throwable cause) {
        super(message, cause);
    }

    public JsonGenerationException(Throwable cause) {
        super(cause);
    }
}

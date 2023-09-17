/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.exception;

import java.io.IOException;

/**
 * Exception thrown when remote integrity checks
 *
 * @opensearch.internal
 */
public class CorruptFileException extends IOException {

    private final String fileName;

    public CorruptFileException(String message, String fileName) {
        super(message);
        this.fileName = fileName;
    }

    public CorruptFileException(String message, Throwable cause, String fileName) {
        super(message, cause);
        this.fileName = fileName;
    }

    public CorruptFileException(Throwable cause, String fileName) {
        super(cause);
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }
}

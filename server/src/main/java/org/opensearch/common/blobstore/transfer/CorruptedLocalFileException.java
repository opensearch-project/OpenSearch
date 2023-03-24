/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

import java.io.IOException;

/**
 * Exception is raised when local file is found corrupted after upload.
 */
public class CorruptedLocalFileException extends IOException {
    public CorruptedLocalFileException(String msg) {
        super(msg);
    }

    public CorruptedLocalFileException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

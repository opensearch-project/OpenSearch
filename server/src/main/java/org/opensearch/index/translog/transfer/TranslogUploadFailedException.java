/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import java.io.IOException;

/**
 * Exception is thrown if there are any exceptions while uploading translog to remote store.
 * @opensearch.internal
 */
public class TranslogUploadFailedException extends IOException {

    public TranslogUploadFailedException(String message) {
        super(message);
    }

    public TranslogUploadFailedException(String message, Throwable cause) {
        super(message, cause);
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Thrown by {@link BlobContainer#writeBlobConditionally} when the conditional write precondition fails, i.e. the blob
 * was modified (or created) by another writer since the supplied version token was read.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class BlobVersionConflictException extends IOException {

    public BlobVersionConflictException(String message) {
        super(message);
    }

    public BlobVersionConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}

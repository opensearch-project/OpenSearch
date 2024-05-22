/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import java.io.IOException;

/**
 * Exception to be thrown when a segment upload fails.
 *
 * @opensearch.internal
 */
public class SegmentUploadFailedException extends IOException {

    /**
     * Creates a new SegmentUploadFailedException.
     *
     * @param message error message
     */
    public SegmentUploadFailedException(String message) {
        super(message);
    }
}

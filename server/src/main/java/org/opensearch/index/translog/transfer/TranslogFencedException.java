/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

/**
 * Thrown when a translog upload is rejected because this shard copy has been fenced in the remote store, i.e. another
 * shard copy has taken over as the writer (at a higher primary term, or at the same term via relocation handoff).
 * This is a fatal condition for the local shard copy: it must stop acknowledging writes.
 *
 * @opensearch.internal
 */
public class TranslogFencedException extends TranslogUploadFailedException {

    public TranslogFencedException(String message) {
        super(message);
    }

    public TranslogFencedException(String message, Throwable cause) {
        super(message, cause);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.TranslogException;

import java.io.IOException;

/**
 * Exception is thrown if there are any exceptions while uploading translog to remote store.
 * @opensearch.internal
 */
public class TranslogUploadFailedException extends TranslogException {

    public TranslogUploadFailedException(ShardId shardId, String message) {
        super(shardId, message);
    }

    public TranslogUploadFailedException(ShardId shardId, String message, Throwable cause) {
        super(shardId, message, cause);
    }

    public TranslogUploadFailedException(StreamInput in) throws IOException {
        super(in);
    }
}

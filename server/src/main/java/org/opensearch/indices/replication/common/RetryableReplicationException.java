/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.opensearch.OpenSearchException;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Exception thrown if replication fails that may be retried.
 *
 * @opensearch.internal
 */
public class RetryableReplicationException extends OpenSearchException {

    public RetryableReplicationException(IndexShard shard, Throwable cause) {
        this(shard, null, cause);
    }

    public RetryableReplicationException(IndexShard shard, @Nullable String extraInfo, Throwable cause) {
        this(shard.shardId(), extraInfo, cause);
    }

    public RetryableReplicationException(ShardId shardId, @Nullable String extraInfo, Throwable cause) {
        super(shardId + ": Replication failed on " + (extraInfo == null ? "" : " (" + extraInfo + ")"), cause);
    }

    public RetryableReplicationException(StreamInput in) throws IOException {
        super(in);
    }

    public RetryableReplicationException(Exception e) {
        super(e);
    }

    public RetryableReplicationException(String msg) {
        super(msg);
    }

    public RetryableReplicationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

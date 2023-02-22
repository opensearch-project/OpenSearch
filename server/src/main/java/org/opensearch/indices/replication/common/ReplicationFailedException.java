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
 * Exception thrown if replication fails
 *
 * @opensearch.internal
 */
public class ReplicationFailedException extends OpenSearchException {

    public ReplicationFailedException(IndexShard shard, Throwable cause) {
        this(shard, null, cause);
    }

    public ReplicationFailedException(IndexShard shard, @Nullable String extraInfo, Throwable cause) {
        this(shard.shardId(), extraInfo, cause);
    }

    public ReplicationFailedException(ShardId shardId, @Nullable String extraInfo, Throwable cause) {
        super(shardId + ": Replication failed on " + (extraInfo == null ? "" : " (" + extraInfo + ")"), cause);
    }

    public ReplicationFailedException(StreamInput in) throws IOException {
        super(in);
    }

    public ReplicationFailedException(Exception e) {
        super(e);
    }

    public ReplicationFailedException(String msg) {
        super(msg);
    }

    public ReplicationFailedException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

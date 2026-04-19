/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Exception thrown when a segment merge operation fails within the engine.
 *
 * @opensearch.experimental
 */
public class MergeFailedEngineException extends OpenSearchException {

    /**
     * Constructs a new MergeFailedEngineException.
     *
     * @param shardId the shard where the merge failed
     * @param t       the underlying cause of the failure
     */
    public MergeFailedEngineException(ShardId shardId, Throwable t) {
        super("Merge failed", t);
        setShard(shardId);
    }

    /**
     * Constructs a new MergeFailedEngineException from a {@link StreamInput}.
     *
     * @param in the stream input to deserialize from
     * @throws IOException if an I/O error occurs
     */
    public MergeFailedEngineException(StreamInput in) throws IOException {
        super(in);
    }
}

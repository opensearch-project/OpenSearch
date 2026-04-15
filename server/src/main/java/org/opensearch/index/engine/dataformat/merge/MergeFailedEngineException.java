/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.EngineException;

/**
 * Exception thrown when a segment merge operation fails within the engine.
 *
 * @opensearch.experimental
 */
public class MergeFailedEngineException extends EngineException {

    /**
     * Constructs a new MergeFailedEngineException.
     *
     * @param shardId the shard where the merge failed
     * @param t       the underlying cause of the failure
     */
    public MergeFailedEngineException(ShardId shardId, Throwable t) {
        super(shardId, "Merge failed", t);
    }
}

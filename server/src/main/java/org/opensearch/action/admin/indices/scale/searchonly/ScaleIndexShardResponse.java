/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Response containing synchronization status for a single shard during search-only scale operations.
 * <p>
 * This response captures the critical state information needed to determine if a shard
 * is ready for a scale operation to proceed, including:
 * <ul>
 *   <li>Whether the shard has uncommitted operations that need to be persisted</li>
 *   <li>Whether the shard needs additional synchronization with remote storage</li>
 * </ul>
 * <p>
 * The cluster manager uses this information from all primary shards to decide
 * whether it's safe to finalize a scale-down operation.
 */
class ScaleIndexShardResponse implements Writeable {
    private final ShardId shardId;
    private final boolean needsSync;
    private final int uncommittedOperations;

    /**
     * Constructs a new ShardSearchOnlyResponse.
     *
     * @param shardId               the ID of the shard that was synchronized
     * @param needsSync             whether the shard still needs additional synchronization
     * @param uncommittedOperations the number of operations not yet committed to the transaction log
     */
    ScaleIndexShardResponse(ShardId shardId, boolean needsSync, int uncommittedOperations) {
        this.shardId = shardId;
        this.needsSync = needsSync;
        this.uncommittedOperations = uncommittedOperations;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if there is an I/O error during deserialization
     */
    ScaleIndexShardResponse(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.needsSync = in.readBoolean();
        this.uncommittedOperations = in.readVInt();
    }

    /**
     * Serializes this response to the given output stream.
     *
     * @param out the output stream to write to
     * @throws IOException if there is an I/O error during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeBoolean(needsSync);
        out.writeVInt(uncommittedOperations);
    }

    /**
     * Returns the shard ID associated with this response.
     *
     * @return the shard ID
     */
    ShardId getShardId() {
        return shardId;
    }

    /**
     * Indicates whether the shard needs additional synchronization before scaling.
     * <p>
     * A shard may need synchronization if:
     * <ul>
     *   <li>It has pending operations that need to be synced to remote storage</li>
     *   <li>The local and remote states don't match</li>
     * </ul>
     *
     * @return true if additional synchronization is needed, false otherwise
     */
    boolean needsSync() {
        return needsSync;
    }

    /**
     * Indicates whether the shard has operations that haven't been committed to the transaction log.
     * <p>
     * Uncommitted operations represent recent writes that haven't been fully persisted,
     * making it unsafe to proceed with a scale-down operation until they are committed.
     *
     * @return true if there are uncommitted operations, false otherwise
     */
    boolean hasUncommittedOperations() {
        return uncommittedOperations > 0;
    }
}

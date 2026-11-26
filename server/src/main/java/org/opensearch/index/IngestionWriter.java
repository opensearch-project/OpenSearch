/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;

import java.io.Closeable;

/**
 * Produces messages to the partitions of a pull-based index's stream. Shard N reads partition N, so a message
 * written to partition N is indexed by shard N.
 *
 * <p>Implementations must be thread-safe: writes for the same index may arrive concurrently.
 */
@ExperimentalApi
public interface IngestionWriter extends Closeable {

    /**
     * Produces one message to a partition. The listener completes once the stream has acknowledged the message,
     * with the message's position in that partition, or fails if the message could not be written.
     *
     * @param partition the partition to write to
     * @param key       the message key
     * @param message   the message, in the format the index's consumer reads
     * @param listener  notified with the message's position in the partition
     */
    void write(int partition, String key, byte[] message, ActionListener<IngestionShardPointer> listener);
}

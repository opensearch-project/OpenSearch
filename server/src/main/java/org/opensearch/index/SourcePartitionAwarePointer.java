/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.PublicApi;

/**
 * Marker interface for {@link IngestionShardPointer} implementations that carry source partition
 * information. Used by the checkpoint tracking layer to maintain per-partition progress when a
 * single shard consumes from multiple source partitions.
 * <p>
 * Implementations should return the source partition ID (e.g., Kafka partition number) from
 * {@link #getSourcePartition()}.
 */
@PublicApi(since = "3.7.0")
public interface SourcePartitionAwarePointer {
    /**
     * Returns the source partition ID that this pointer belongs to.
     * @return the partition ID
     */
    int getSourcePartition();
}

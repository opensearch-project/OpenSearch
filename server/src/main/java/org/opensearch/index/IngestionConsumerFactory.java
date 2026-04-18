/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.common.annotation.PublicApi;

import java.util.List;

/**
 * A factory for creating {@link IngestionShardConsumer}.
 *
 * @param <T> the type of the {@link IngestionShardConsumer}
 * @param <P> the type of the {@link IngestionShardPointer}
 */
@PublicApi(since = "3.6.0")
public interface IngestionConsumerFactory<T extends IngestionShardConsumer, P extends IngestionShardPointer> {
    /**
     * Initialize the factory with the configuration parameters. This method is called once when the factory is created,
     * and the required parameters are provided from the {@link org.opensearch.cluster.metadata.IngestionSource} in
     * {@link  org.opensearch.cluster.metadata.IndexMetadata}.
     * @param ingestionSource the ingestion source with configuration parameters to initialize the factory
     */
    void initialize(IngestionSource ingestionSource);

    /**
     *  Create a consumer to ingest messages from a shard of the streams. When the ingestion engine created per shard,
     *  this method is called to create the consumer in the poller. Before the invocation of this method, the configuration
     *  is passed to the factory through the {@link #initialize(IngestionSource)} method.
     *
     * @param clientId the client id assigned to the consumer
     * @param shardId the id of the shard
     * @return the created consumer
     */
    T createShardConsumer(String clientId, int shardId);

    /**
     * Parses the pointer from a string representation to the pointer object. This is used for recovering from the index
     * checkpoints.
     * @param pointer the string representation of the pointer
     * @return the recovered pointer
     */
    P parsePointerFromString(String pointer);

    /**
     * Returns the total number of partitions available in the source stream. This is used to compute
     * partition-to-shard assignments when {@code partition_strategy} is set to {@code auto}.
     * <p>
     * The default implementation returns -1, indicating that the source does not support partition
     * count discovery. Implementations should override this method to enable multi-partition consumption.
     *
     * @return the total number of source partitions, or -1 if unknown
     */
    default int getSourcePartitionCount() {
        return -1;
    }

    /**
     * Create a consumer for a shard that reads from multiple source partitions. This is used when
     * {@code partition_strategy} is set to {@code auto} and a shard is assigned more than one partition.
     * <p>
     * The default implementation falls back to {@link #createShardConsumer(String, int)}, which creates
     * a single-partition consumer using the shard ID as the partition ID (legacy 1:1 behavior).
     * Implementations should override this method to support multi-partition consumption.
     *
     * @param clientId the client id assigned to the consumer
     * @param shardId the OpenSearch shard id
     * @param partitionIds the list of source partition IDs to consume from
     * @return the created consumer
     */
    default T createMultiPartitionShardConsumer(String clientId, int shardId, List<Integer> partitionIds) {
        return createShardConsumer(clientId, shardId);
    }
}

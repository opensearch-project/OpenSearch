/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * A factory for creating {@link IngestionShardConsumer}.
 *
 * @param <T> the type of the {@link IngestionShardConsumer}
 * @param <P> the type of the {@link IngestionShardPointer}
 */
@ExperimentalApi
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
}

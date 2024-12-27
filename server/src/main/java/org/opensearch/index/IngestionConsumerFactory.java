/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;

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
     * and the parameters are parsed from the {@link org.opensearch.cluster.metadata.IngestionSource} in
     * {@link  org.opensearch.cluster.metadata.IndexMetadata}.
     * @param params the configuration parameters to initialize the factory
     */
    void initialize(Map<String, Object> params);

    /**
     *  Create a consumer to ingest messages from a shard of the streams. When the ingestion engine created per shard,
     *  this method is called to create the consumer in the poller. Before the invocation of this method, the configuration
     *  is passed to the factory through the {@link #initialize(Map)} method.
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

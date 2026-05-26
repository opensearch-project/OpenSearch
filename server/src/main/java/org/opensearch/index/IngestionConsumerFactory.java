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

/**
 * A factory for creating {@link IngestionShardConsumer}.
 *
 * @param <T> the type of the {@link IngestionShardConsumer}
 * @param <P> the type of the {@link IngestionShardPointer}
 */
@PublicApi(since = "3.6.0")
public interface IngestionConsumerFactory<T extends IngestionShardConsumer, P extends IngestionShardPointer> {
    /**
     * Initialize the factory with the configuration parameters.
     *
     * @param ingestionSource the ingestion source with configuration parameters to initialize the factory
     * @deprecated Implement {@link #createShardConsumer(String, int, IngestionSource)} instead, which receives
     * the ingestion source directly and requires no separate initialization step.
     */
    @Deprecated(forRemoval = true)
    default void initialize(IngestionSource ingestionSource) {
        // no-op: implementations should override createShardConsumer(String, int, IngestionSource) instead
    }

    /**
     * Create a consumer to ingest messages from a shard of the streams.
     *
     * @param clientId the client id assigned to the consumer
     * @param shardId the id of the shard
     * @return the created consumer
     * @deprecated Use {@link #createShardConsumer(String, int, IngestionSource)} instead.
     */
    @Deprecated(forRemoval = true)
    default T createShardConsumer(String clientId, int shardId) {
        throw new UnsupportedOperationException(
            "Implement createShardConsumer(String, int, IngestionSource) instead of the deprecated two-step API."
        );
    }

    /**
     * Parses the pointer from a string representation to the pointer object. This is used for recovering from the index
     * checkpoints.
     * @param pointer the string representation of the pointer
     * @return the recovered pointer
     */
    P parsePointerFromString(String pointer);

    /**
     * Create a consumer to ingest messages from a shard of the streams, using the provided ingestion source directly.
     * Implementations should override this method rather than the deprecated
     * {@link #initialize(IngestionSource)} and {@link #createShardConsumer(String, int)} pair.
     * <p>
     * The default implementation delegates to the deprecated two-step pair for backward compatibility
     * with existing implementations that override {@link #initialize(IngestionSource)}.
     *
     * @param clientId        the client id assigned to the consumer
     * @param shardId         the id of the shard
     * @param ingestionSource the ingestion source configuration
     * @return the created consumer
     */
    @SuppressWarnings("deprecation")
    default T createShardConsumer(String clientId, int shardId, IngestionSource ingestionSource) {
        initialize(ingestionSource);
        return createShardConsumer(clientId, shardId);
    }
}

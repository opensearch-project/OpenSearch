/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Factory for creating Kafka consumers
 */
public class KafkaConsumerFactory implements IngestionConsumerFactory<KafkaPartitionConsumer, KafkaOffset> {

    /**
     * Configuration for the Kafka source
     */
    protected KafkaSourceConfig config;

    private volatile int cachedPartitionCount = -1;

    /**
     * Constructor.
     */
    public KafkaConsumerFactory() {}

    @Override
    public void initialize(IngestionSource ingestionSource) {
        config = new KafkaSourceConfig((int) ingestionSource.getMaxPollSize(), ingestionSource.params());
    }

    @Override
    public KafkaPartitionConsumer createShardConsumer(String clientId, int shardId) {
        assert config != null;
        return new KafkaPartitionConsumer(clientId, config, shardId);
    }

    @Override
    public KafkaOffset parsePointerFromString(String pointer) {
        return new KafkaOffset(Long.valueOf(pointer));
    }

    @Override
    public int getSourcePartitionCount() {
        if (cachedPartitionCount > 0) {
            return cachedPartitionCount;
        }
        assert config != null;
        // Create a temporary consumer to query partition metadata
        try (KafkaPartitionConsumer tempConsumer = new KafkaPartitionConsumer(
            "partition-count-query",
            config,
            0
        )) {
            cachedPartitionCount = tempConsumer.getPartitionCount();
            return cachedPartitionCount;
        } catch (IOException e) {
            throw new RuntimeException("Failed to close temporary consumer while querying partition count", e);
        }
    }

    @Override
    public KafkaPartitionConsumer createMultiPartitionShardConsumer(String clientId, int shardId, List<Integer> partitionIds) {
        assert config != null;
        if (partitionIds.size() == 1) {
            return new KafkaPartitionConsumer(clientId, config, partitionIds.get(0));
        }
        // TODO: Return KafkaMultiPartitionConsumer once implemented (PR 3)
        throw new UnsupportedOperationException(
            "Multi-partition consumer not yet implemented. Assign a single partition per shard or use partition_strategy=fixed."
        );
    }
}

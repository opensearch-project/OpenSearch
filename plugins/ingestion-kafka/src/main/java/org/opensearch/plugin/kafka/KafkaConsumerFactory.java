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

/**
 * Factory for creating Kafka consumers.
 */
public class KafkaConsumerFactory implements IngestionConsumerFactory<KafkaPartitionConsumer, KafkaOffset> {

    /**
     * Constructor.
     */
    public KafkaConsumerFactory() {}

    @Override
    public KafkaPartitionConsumer createShardConsumer(String clientId, int shardId, IngestionSource ingestionSource) {
        KafkaSourceConfig localConfig = new KafkaSourceConfig((int) ingestionSource.getMaxPollSize(), ingestionSource.params());
        // Constructing a KafkaPartitionConsumer should *never* throw an exception.
        KafkaPartitionConsumer consumer = new KafkaPartitionConsumer(clientId, localConfig, shardId);
        try {
            // But initializing it *may* throw an exception.
            consumer.initialize();
        } catch (Exception e) {
            // In which case, we *must* close the Kafka connection, or else it will leak.
            consumer.close();
            throw new IllegalStateException(e);
        }
        return consumer;
    }

    @Override
    public KafkaOffset parsePointerFromString(String pointer) {
        return new KafkaOffset(Long.valueOf(pointer));
    }
}

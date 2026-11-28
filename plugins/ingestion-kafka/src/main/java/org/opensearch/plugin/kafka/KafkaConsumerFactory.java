/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;

import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Factory for creating Kafka consumers.
 */
public class KafkaConsumerFactory implements IngestionConsumerFactory<KafkaPartitionConsumer, KafkaOffset> {

    private final List<KafkaClientConfigurator> clientConfigurators;

    /**
     * Constructor.
     */
    public KafkaConsumerFactory() {
        this(List.of());
    }

    /**
     * Constructor.
     * @param clientConfigurators adjust the consumer properties of each index, in order
     */
    public KafkaConsumerFactory(List<KafkaClientConfigurator> clientConfigurators) {
        this.clientConfigurators = clientConfigurators;
    }

    @Override
    public KafkaPartitionConsumer createShardConsumer(String clientId, int shardId, IndexMetadata indexMetadata) {
        IngestionSource ingestionSource = indexMetadata.getIngestionSource();
        KafkaSourceConfig localConfig = new KafkaSourceConfig((int) ingestionSource.getMaxPollSize(), ingestionSource.params());
        // Constructing a KafkaPartitionConsumer should *never* throw an exception.
        KafkaPartitionConsumer consumer = new KafkaPartitionConsumer(clientId, localConfig, shardId, customizer(indexMetadata));
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

    // visible for testing
    Consumer<Properties> customizer(IndexMetadata indexMetadata) {
        return properties -> {
            for (KafkaClientConfigurator configurator : clientConfigurators) {
                configurator.configure(indexMetadata, KafkaClientConfigurator.ClientType.CONSUMER, properties);
            }
        };
    }

    @Override
    public KafkaOffset parsePointerFromString(String pointer) {
        return new KafkaOffset(Long.valueOf(pointer));
    }
}

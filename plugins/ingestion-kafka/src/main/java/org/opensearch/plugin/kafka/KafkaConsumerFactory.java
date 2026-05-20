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
        return new KafkaPartitionConsumer(clientId, localConfig, shardId);
    }

    @Override
    public KafkaOffset parsePointerFromString(String pointer) {
        return new KafkaOffset(Long.valueOf(pointer));
    }
}

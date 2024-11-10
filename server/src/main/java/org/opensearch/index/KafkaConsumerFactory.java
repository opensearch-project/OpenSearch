/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

public class KafkaConsumerFactory extends IngestionConsumerFactory<KafkaPartitionConsumer> {

    @Override
    public KafkaPartitionConsumer createShardConsumer(String clientId, int shardId) {
        KafkaSourceConfig config = (KafkaSourceConfig) this.config;
        return new KafkaPartitionConsumer(clientId, config, shardId);
    }
}

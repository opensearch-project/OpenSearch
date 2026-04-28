/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;

/**
 * Factory for creating Kinesis consumers
 */
public class KinesisConsumerFactory implements IngestionConsumerFactory<KinesisShardConsumer, SequenceNumber> {

    /**
     * Configuration for the Kinesis source
     */
    protected KinesisSourceConfig config;

    /**
     * Constructor.
     */
    public KinesisConsumerFactory() {}

    @Override
    public void initialize(IngestionSource ingestionSource) {
        config = new KinesisSourceConfig(ingestionSource.params());
    }

    @Override
    public KinesisShardConsumer createShardConsumer(String clientId, int shardId) {
        assert config != null;
        return new KinesisShardConsumer(clientId, config, shardId);
    }

    @Override
    public SequenceNumber parsePointerFromString(String pointer) {
        return new SequenceNumber(pointer);
    }
}

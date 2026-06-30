/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IngestionConsumerFactory;

/**
 * Factory for creating Kinesis consumers.
 */
public class KinesisConsumerFactory implements IngestionConsumerFactory<KinesisShardConsumer, SequenceNumber> {

    /**
     * Constructor.
     */
    public KinesisConsumerFactory() {}

    @Override
    public KinesisShardConsumer createShardConsumer(String clientId, int shardId, IndexMetadata indexMetadata) {
        KinesisSourceConfig localConfig = new KinesisSourceConfig(indexMetadata.getIngestionSource().params());
        return new KinesisShardConsumer(clientId, localConfig, shardId);
    }

    @Override
    public SequenceNumber parsePointerFromString(String pointer) {
        return new SequenceNumber(pointer);
    }
}

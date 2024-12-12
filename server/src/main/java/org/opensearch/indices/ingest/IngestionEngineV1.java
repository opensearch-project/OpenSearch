/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.*;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.InternalEngine;

import java.io.IOException;

public class IngestionEngineV1 extends InternalEngine {

    protected StreamPoller streamPoller;

    public IngestionEngineV1(EngineConfig engineConfig) {
        super(engineConfig);

        IndexMetadata indexMetadata = engineConfig.getIndexSettings().getIndexMetadata();
        assert indexMetadata!=null;
        IngestionSourceConfig ingestionSourceConfig = indexMetadata.getIngestionSourceConfig();
        assert ingestionSourceConfig!=null;

        // todo: get IngestionConsumerFactory from the config
        KafkaConsumerFactory kafkaConsumerFactory = new KafkaConsumerFactory();
        kafkaConsumerFactory.initialize(ingestionSourceConfig);
        IngestionShardConsumer ingestionShardConsumer = kafkaConsumerFactory.createShardConsumer("clientId", 0);

        // todo: get pointer policy from the config
        KafkaOffset kafkaOffset = new KafkaOffset(0);
        // todo: support other kinds of stream pollers
        streamPoller = new DefaultStreamPoller(kafkaOffset, ingestionShardConsumer,
            new DocumentProcessor(this));
        streamPoller.start();
    }


    @Override
    public void close() throws IOException {
        if(streamPoller!=null) {
            streamPoller.close();
        }
        super.close();
    }
}

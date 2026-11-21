/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.TreeSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;

/**
 * Consumer factory for arrow format, to use it, user must specify proper configs in {@link
 * IngestionSource#params()}, see {@link ArrowIngestionConfig} for details
 */
public class ArrowConsumerFactory
        implements IngestionConsumerFactory<ArrowShardConsumer, ArrowOffset> {

    private static final Logger LOGGER = LogManager.getLogger(ArrowConsumerFactory.class);

    // Populated once by ArrowPlugin from every ArrowSourceFactory discovered via
    // ExtensiblePlugin#loadExtensions; unmodifiable and never mutated after construction.
    private final Map<String, ArrowSourceFactory> sourceFactories;

    /**
     * Ctor
     *
     * @param sourceFactories every {@link ArrowSourceFactory} registered with {@link ArrowPlugin},
     *     keyed by {@link ArrowSourceFactory#getType()}
     */
    public ArrowConsumerFactory(Map<String, ArrowSourceFactory> sourceFactories) {
        this.sourceFactories = sourceFactories;
    }

    @Override
    public ArrowShardConsumer createShardConsumer(String clientId, int shardId, IndexMetadata indexMetadata) {
        LOGGER.info("Initializing ArrowConsumerFactory with ingestion source");
        IngestionSource ingestionSource = indexMetadata.getIngestionSource();
        Map<String, Object> ingestionSourceParams = ingestionSource.params();
        ArrowIngestionConfig ingestionConfig = new ArrowIngestionConfig(ingestionSourceParams, indexMetadata);
        ArrowSourceFactory sourceFactory = sourceFactories.get(ingestionConfig.getSourceType());
        if (sourceFactory == null) {
            throw new IllegalArgumentException(
                    "Unknown "
                            + ArrowIngestionConfig.DATASET_TYPE_PROP_KEY
                            + ": '"
                            + ingestionConfig.getSourceType()
                            + "'. Registered types: "
                            + new TreeSet<>(sourceFactories.keySet()));
        }
        LOGGER.info("Creating ArrowSource of type: {}", ingestionConfig.getSourceType());
        ArrowSource arrowSource;
        try {
            arrowSource = sourceFactory.createArrowSource(ingestionSourceParams, ingestionConfig);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOGGER.info("Created ArrowSource, now creating ShardConsumer for shard: {}", shardId);
        return new ArrowShardConsumer(shardId, arrowSource, ingestionConfig);
    }

    @Override
    public ArrowOffset parsePointerFromString(String pointer) {
        return ArrowOffset.fromString(pointer);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.IngestionConsumerPlugin;
import org.opensearch.plugins.Plugin;

import java.util.List;
import java.util.Map;

/**
 * A plugin for ingestion source of Kafka. Plugins that extend it can adjust the Kafka client properties of each index
 * by providing a {@link KafkaClientConfigurator}.
 */
public class KafkaPlugin extends Plugin implements IngestionConsumerPlugin, ExtensiblePlugin {
    /**
     * The type of the ingestion source.
     */
    public static final String TYPE = "KAFKA";

    private List<KafkaClientConfigurator> clientConfigurators = List.of();

    /**
     * Constructor.
     */
    public KafkaPlugin() {}

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        clientConfigurators = List.copyOf(loader.loadExtensions(KafkaClientConfigurator.class));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, IngestionConsumerFactory> getIngestionConsumerFactories() {
        return Map.of(TYPE, new KafkaConsumerFactory(clientConfigurators));
    }

    @Override
    public String getType() {
        return TYPE;
    }
}

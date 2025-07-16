/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.plugins.IngestionConsumerPlugin;
import org.opensearch.plugins.Plugin;

import java.util.Map;

/**
 * A plugin for ingestion source of Kinesis.
 */
public class KinesisPlugin extends Plugin implements IngestionConsumerPlugin {
    /**
     * The type of the ingestion source.
     */
    public static final String TYPE = "KINESIS";

    /**
     * Constructor.
     */
    public KinesisPlugin() {}

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, IngestionConsumerFactory> getIngestionConsumerFactories() {
        return Map.of(TYPE, new KinesisConsumerFactory());
    }

    @Override
    public String getType() {
        return TYPE;
    }
}

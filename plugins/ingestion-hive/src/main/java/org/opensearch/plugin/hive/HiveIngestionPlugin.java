/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.plugins.IngestionConsumerPlugin;
import org.opensearch.plugins.Plugin;

import java.util.Map;

/**
 * OpenSearch plugin for pull-based ingestion from Hive tables.
 * Registers the "HIVE" ingestion source type with the Pull-Based Ingestion framework.
 */
public class HiveIngestionPlugin extends Plugin implements IngestionConsumerPlugin {

    /** The ingestion source type identifier. */
    public static final String TYPE = "HIVE";

    /** Creates a new HiveIngestionPlugin instance. */
    public HiveIngestionPlugin() {}

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, IngestionConsumerFactory> getIngestionConsumerFactories() {
        return Map.of(TYPE, new HiveConsumerFactory());
    }

    @Override
    public String getType() {
        return TYPE;
    }
}

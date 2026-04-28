/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.plugins.IngestionConsumerPlugin;
import org.opensearch.plugins.Plugin;

import java.util.Map;

/**
 * A plugin for file-based ingestion (used for local testing).
 */
public class FilePlugin extends Plugin implements IngestionConsumerPlugin {

    /**
     * The type of the ingestion source.
     */
    public static final String TYPE = "FILE";

    /**
     * Initialize FilePlugin for file-based indexing.
     */
    public FilePlugin() {}

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, IngestionConsumerFactory> getIngestionConsumerFactories() {
        return Map.of(TYPE, new FileConsumerFactory());
    }

    @Override
    public String getType() {
        return TYPE;
    }
}

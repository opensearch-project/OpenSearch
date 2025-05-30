/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemingestprocessor;

import org.opensearch.ingest.Processor;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;

import java.util.Map;

/**
 * Example plugin that implements a custom system ingest processor.
 */
public class ExampleSystemIngestProcessorPlugin extends Plugin implements IngestPlugin {
    /**
     * Constructs a new ExampleSystemIngestProcessorPlugin
     */
    public ExampleSystemIngestProcessorPlugin() {}

    @Override
    public Map<String, Processor.Factory> getSystemIngestProcessors(Processor.Parameters parameters) {
        return Map.of(ExampleSystemIngestProcessorFactory.TYPE, new ExampleSystemIngestProcessorFactory());
    }
}

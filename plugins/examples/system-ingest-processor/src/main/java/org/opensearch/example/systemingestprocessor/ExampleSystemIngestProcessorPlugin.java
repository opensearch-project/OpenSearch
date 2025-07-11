/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemingestprocessor;

import org.opensearch.common.settings.Setting;
import org.opensearch.ingest.Processor;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;

import java.util.List;
import java.util.Map;

/**
 * Example plugin that implements a custom system ingest processor.
 */
public class ExampleSystemIngestProcessorPlugin extends Plugin implements IngestPlugin {
    /**
     * Constructs a new ExampleSystemIngestProcessorPlugin
     */
    public ExampleSystemIngestProcessorPlugin() {}

    /**
     * A custom index setting which is used to control if we should create the example system ingest processor.
     */
    public static final Setting<Boolean> TRIGGER_SETTING = Setting.boolSetting(
        "index.example_system_ingest_processor_plugin.trigger_setting",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    @Override
    public Map<String, Processor.Factory> getSystemIngestProcessors(Processor.Parameters parameters) {
        return Map.of(ExampleSystemIngestProcessorFactory.TYPE, new ExampleSystemIngestProcessorFactory());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(TRIGGER_SETTING);
    }
}

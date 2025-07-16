/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemingestprocessor;

import org.opensearch.common.settings.Settings;
import org.opensearch.ingest.AbstractBatchingSystemProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.example.systemingestprocessor.ExampleSystemIngestProcessorPlugin.TRIGGER_SETTING;
import static org.opensearch.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_MAPPINGS;
import static org.opensearch.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_SETTINGS;
import static org.opensearch.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_TEMPLATE_MAPPINGS;
import static org.opensearch.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_TEMPLATE_SETTINGS;

/**
 * A factory to create the example system ingest processor
 */
public class ExampleSystemIngestProcessorFactory extends AbstractBatchingSystemProcessor.Factory {
    /**
     * Name for doc. Actions like create index and legacy create/update index template will have the
     * mapping properties under a _doc key.
     */
    public static final String DOC = "_doc";
    /**
     * Name for properties. An object field will define subfields as properties.
     */
    public static final String PROPERTIES = "properties";
    /**
     * The name of the trigger field.
     */
    public static final String TRIGGER_FIELD_NAME = "system_ingest_processor_trigger_field";
    /**
     * The type of the factory.
     */
    public static final String TYPE = "example_system_ingest_processor_factory";
    /**
     * A default batch size.
     */
    private static final int DEFAULT_BATCH_SIZE = 10;

    /**
     * Constructs a new ExampleSystemIngestProcessorFactory
     */
    protected ExampleSystemIngestProcessorFactory() {
        super(TYPE);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected AbstractBatchingSystemProcessor newProcessor(String tag, String description, Map<String, Object> config) {
        final List<Map<String, Object>> mappings = new ArrayList<>();
        final List<Settings> settings = new ArrayList<>();
        final Object mappingFromIndex = config.get(INDEX_MAPPINGS);
        final Object mappingFromTemplates = config.get(INDEX_TEMPLATE_MAPPINGS);
        final Object settingsFromIndex = config.get(INDEX_SETTINGS);
        final Object settingsFromTemplates = config.get(INDEX_TEMPLATE_SETTINGS);
        if (mappingFromTemplates instanceof List) {
            mappings.addAll((List<Map<String, Object>>) mappingFromTemplates);
        }
        if (mappingFromIndex instanceof Map) {
            mappings.add((Map<String, Object>) mappingFromIndex);
        }
        if (settingsFromTemplates instanceof List) {
            settings.addAll((Collection<? extends Settings>) settingsFromTemplates);
        }
        if (settingsFromIndex instanceof Settings) {
            settings.add((Settings) settingsFromIndex);
        }

        // If no config we are not able to create a processor so simply return a null to show no processor created
        if (mappings.isEmpty() && settings.isEmpty()) {
            return null;
        }

        if (description == null) {
            description =
                "This is an example system ingest processor. It will auto add a text field to the ingest doc if the mapping contains a trigger field.";
        }

        boolean isTriggerFieldFound = false;
        for (final Map<String, Object> mapping : mappings) {
            final Map<String, Object> properties = getProperties(mapping);
            // if there is no property in the mapping we simply skip it
            if (properties.isEmpty()) {
                continue;
            }

            if (properties.containsKey(TRIGGER_FIELD_NAME)) {
                isTriggerFieldFound = true;
            }
        }

        // If the trigger setting is configured then use it directly.
        // When we rely on the v1 template to create the index there can be multiple settings and the later one can
        // override the previous one so we need to loop through all the settings.
        for (final Settings setting : settings) {
            if (setting.hasValue(TRIGGER_SETTING.getKey())) {
                isTriggerFieldFound = TRIGGER_SETTING.get(setting);
            }
        }

        return isTriggerFieldFound ? new ExampleSystemIngestProcessor(tag, description, DEFAULT_BATCH_SIZE) : null;
    }

    /**
     * Help extract the properties from a mapping
     * @param mapping index mapping
     * @return properties of the mapping
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> getProperties(Map<String, Object> mapping) {
        if (mapping == null) {
            return new HashMap<>();
        }

        if (mapping.containsKey(DOC) && mapping.get(DOC) instanceof Map) {
            Map<String, Object> doc = (Map<String, Object>) mapping.get(DOC);
            if (doc.containsKey(PROPERTIES) && doc.get(PROPERTIES) instanceof Map) {
                return (Map<String, Object>) doc.get(PROPERTIES);
            }
        } else if (mapping.containsKey(PROPERTIES) && mapping.get(PROPERTIES) instanceof Map) {
            return (Map<String, Object>) mapping.get(PROPERTIES);
        }

        return new HashMap<>();
    }

}

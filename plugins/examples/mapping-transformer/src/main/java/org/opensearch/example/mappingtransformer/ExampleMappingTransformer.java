/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.mappingtransformer;

import org.opensearch.core.action.ActionListener;
import org.opensearch.index.mapper.MappingTransformer;

import java.util.HashMap;
import java.util.Map;

/**
 * This example mapping transformer will automatically add a text field to the mapping if it finds a
 * mapping_transform_trigger_field.
 */
public class ExampleMappingTransformer implements MappingTransformer {
    /**
     * Constructs a new ExampleMappingTransformer
     */
    public ExampleMappingTransformer() {}

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
     * The name of the trigger field. The trigger field will trigger the logic to transform the mapping.
     */
    public static final String TRIGGER_FIELD_NAME = "mapping_transform_trigger_field";
    /**
     * The name of the auto added field.
     */
    public static final String AUTO_ADDED_FIELD_NAME = "field_auto_added_by_example_mapping_transformer";

    @Override
    public void transform(Map<String, Object> mapping, TransformContext context, ActionListener<Void> listener) {
        Map<String, Object> properties = getProperties(mapping);
        if (properties.containsKey(TRIGGER_FIELD_NAME)) {
            properties.put(AUTO_ADDED_FIELD_NAME, Map.of("type", "text"));
        }
        listener.onResponse(null);
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.mappingtransformer;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.example.mappingtransformer.ExampleMappingTransformer.AUTO_ADDED_FIELD_NAME;
import static org.opensearch.example.mappingtransformer.ExampleMappingTransformer.DOC;
import static org.opensearch.example.mappingtransformer.ExampleMappingTransformer.PROPERTIES;
import static org.opensearch.example.mappingtransformer.ExampleMappingTransformer.TRIGGER_FIELD_NAME;

public class ExampleMappingTransformerTests extends OpenSearchTestCase {
    private final ExampleMappingTransformer transformer = new ExampleMappingTransformer();

    public void testExampleMappingTransformer_whenMappingWithoutDocLayer() {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        mapping.put(PROPERTIES, properties);
        properties.put("dummyField", Map.of("type", "text"));

        transformer.transform(mapping, null, new PlainActionFuture<>());

        // verify no field is auto-injected
        assertFalse(
            "No trigger field in the mapping so should not transform the mapping to inject the field.",
            properties.containsKey(AUTO_ADDED_FIELD_NAME)
        );

        properties.put(TRIGGER_FIELD_NAME, Map.of("type", "text"));
        transformer.transform(mapping, null, new PlainActionFuture<>());

        // verify the field should be auto added to the mapping
        assertTrue(
            "The mapping has the trigger field so should transform the mapping to auto inject a field.",
            properties.containsKey(AUTO_ADDED_FIELD_NAME)
        );
    }

    public void testExampleMappingTransformer_whenMappingWithDocLayer() {
        Map<String, Object> doc = new HashMap<>();
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        doc.put(DOC, mapping);
        mapping.put(PROPERTIES, properties);
        properties.put("dummyField", Map.of("type", "text"));

        transformer.transform(doc, null, new PlainActionFuture<>());

        // verify no field is auto-injected
        assertFalse(
            "No trigger field in the mapping so should not transform the mapping to inject the field.",
            properties.containsKey(AUTO_ADDED_FIELD_NAME)
        );

        properties.put(TRIGGER_FIELD_NAME, Map.of("type", "text"));
        transformer.transform(mapping, null, new PlainActionFuture<>());

        // verify the field should be auto added to the mapping
        assertTrue(
            "The mapping has the trigger field so should transform the mapping to auto inject a field.",
            properties.containsKey(AUTO_ADDED_FIELD_NAME)
        );
    }

    public void testExampleMappingTransformer_whenNoMapping_thenDoNothing() {
        transformer.transform(null, null, new PlainActionFuture<>());
    }

    public void testExampleMappingTransformer_whenEmptyMapping_thenDoNothing() {
        Map<String, Object> doc = new HashMap<>();
        transformer.transform(doc, null, new PlainActionFuture<>());
        assertEquals(new HashMap<>(), doc);
    }
}

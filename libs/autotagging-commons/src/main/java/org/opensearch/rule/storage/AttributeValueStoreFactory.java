/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory class for AttributeValueStore
 */
public class AttributeValueStoreFactory {
    private static Map<String, AttributeValueStore<String, String>> attributeValueStores;

    /**
     * This should be the first method to be invoked else the factory method will throw an exception
     * @param featureTypes  are the features which are using rule based auto tagging
     */
    public static void init(List<FeatureType> featureTypes) {
        attributeValueStores = new HashMap<>();
        for (FeatureType featureType : featureTypes) {
            for (Attribute attribute : featureType.getAllowedAttributesRegistry().values()) {
                attributeValueStores.put(attribute.getName(), new DefaultAttributeValueStore<>());
            }
        }
    }

    /**
     * Factory method which returns the {@link AttributeValueStore} for the given attribute
     * @param attribute
     * @return
     */
    public static AttributeValueStore<String, String> getAttributeValueStore(final Attribute attribute) {
        final String attributeName = attribute.getName();
        if (attributeValueStores == null) {
            throw new IllegalStateException("AttributeValueStoreFactory is not initialized yet.");
        }

        if (!attributeValueStores.containsKey(attributeName)) {
            throw new IllegalArgumentException("[" + attributeName + "] is not a valid attribute for enabled features.");
        }

        return attributeValueStores.get(attributeName);
    }
}

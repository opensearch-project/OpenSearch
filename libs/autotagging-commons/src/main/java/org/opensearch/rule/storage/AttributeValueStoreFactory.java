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
import java.util.Map;
import java.util.function.Supplier;

/**
 * Factory class for AttributeValueStore
 */
public class AttributeValueStoreFactory {
    private final static Map<String, AttributeValueStore<String, String>> attributeValueStores = new HashMap<>();

    /**
     * Making the class to be uninitializable
     */
    private AttributeValueStoreFactory() {}

    /**
     * This should be the first method to be invoked else the factory method will throw an exception
     * @param featureType  is the feature which are using rule based auto tagging
     * @param attributeValueStoreSupplier supplies the feature level AttributeValueStore instance
     */
    public static void init(FeatureType featureType, Supplier<AttributeValueStore<String, String>> attributeValueStoreSupplier) {
        for (Attribute attribute : featureType.getAllowedAttributesRegistry().values()) {
            attributeValueStores.put(attribute.getName(), attributeValueStoreSupplier.get());
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.collect.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 *  Registry for managing auto-tagging attributes and feature types.
 *  This class provides functionality to register and retrieve {@link Attribute} and {@link FeatureType} instances
 *  used for auto-tagging.
 *
 * @opensearch.experimental
 */
public class AutoTaggingRegistry {
    public static final Map<Tuple<String, String>, FeatureType> featureTypesRegistryMap = new HashMap<>();
    public static final Map<Tuple<String, String>, Attribute> attributeRegistryMap = new HashMap<>();

    public static void registerFeatureType(FeatureType featureType) {
        if (featureType == null) {
            throw new IllegalStateException("Feature type is not initialized and can't be registered");
        }
        featureTypesRegistryMap.put(new Tuple<>(featureType.getClass().getName(), featureType.getName()), featureType);
    }

    public static void registerAttribute(Attribute attribute) {
        if (attribute == null) {
            throw new IllegalStateException("Attribute is not initialized and can't be registered");
        }
        attributeRegistryMap.put(new Tuple<>(attribute.getClass().getName(), attribute.getName()), attribute);
    }

    /**
     * Retrieves the registered {@link FeatureType} instance based on class name and feature type name.
     * This method assumes that FeatureTypes are singletons, meaning that each unique
     * (className, featureTypeName) pair corresponds to a single, globally shared instance.
     *
     * @param className The name of the class associated with the feature type.
     * @param featureTypeName The name of the feature type.
     */
    public static FeatureType getFeatureType(String className, String featureTypeName) {
        FeatureType featureType = featureTypesRegistryMap.get(new Tuple<>(className, featureTypeName));
        if (featureType == null) {
            throw new ResourceNotFoundException(
                "Couldn't find a feature type with name: "
                    + featureTypeName
                    + " under the class: "
                    + className
                    + ". Make sure you have registered it."
            );
        }
        return featureType;
    }

    /**
     * Retrieves the registered {@link Attribute} instance based on the class name and attribute name.
     * This method assumes that Attributes are singletons, meaning that each unique
     * (className, attributeName) pair corresponds to a single, globally shared instance.
     *
     * @param className The name of the class associated with the attribute.
     * @param attributeName The name of the attribute.
     */
    public static Attribute getAttribute(String className, String attributeName) {
        Attribute attribute = attributeRegistryMap.get(new Tuple<>(className, attributeName));
        if (attribute == null) {
            throw new ResourceNotFoundException(
                "Couldn't find a attribute with name: "
                    + attributeName
                    + " under the class: "
                    + className
                    + ". Make sure you have registered it."
            );
        }
        return attribute;
    }
}

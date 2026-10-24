/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.autotagging;

import org.opensearch.ResourceNotFoundException;

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
    /**
     * featureTypesRegistryMap should be concurrently readable but not concurrently writable.
     * The registration of FeatureType should only be done during boot-up.
     */
    private final Map<String, FeatureType> featureTypesRegistryMap = new HashMap<>();
    /**
     * Max chars a feature type can assume
     */
    public static final int MAX_FEATURE_TYPE_NAME_LENGTH = 30;

    /**
     * Creates a registry owned by one node.
     */
    public AutoTaggingRegistry() {}

    /**
     * Registers the new feature type
     * @param featureType
     */
    public void registerFeatureType(FeatureType featureType) {
        validateFeatureType(featureType);
        String name = featureType.getName();
        if (featureTypesRegistryMap.containsKey(name) && featureTypesRegistryMap.get(name) != featureType) {
            throw new IllegalStateException("Feature type " + name + " is already registered. Duplicate feature type is not allowed.");
        }
        featureTypesRegistryMap.put(name, featureType);
    }

    private static void validateFeatureType(FeatureType featureType) {
        if (featureType == null) {
            throw new IllegalStateException("Feature type can't be null. Unable to register.");
        }
        String name = featureType.getName();
        if (name == null || name.isEmpty() || name.length() > MAX_FEATURE_TYPE_NAME_LENGTH) {
            throw new IllegalStateException(
                "Feature type name " + name + " should not be null, empty or have more than  " + MAX_FEATURE_TYPE_NAME_LENGTH + "characters"
            );
        }
        if (featureType.getOrderedAttributes() == null) {
            throw new IllegalStateException(
                "Function getOrderedAttributes() should not return null for feature type: " + featureType.getName()
            );
        }
        if (featureType.getFeatureValueValidator() == null) {
            throw new IllegalStateException("FeatureValueValidator is not defined for feature type " + name);
        }
    }

    /**
     * Retrieves the registered {@link FeatureType} instance by feature type name.
     * Each feature name resolves to the instance registered on this node.
     *
     * @param featureTypeName The name of the feature type.
     */
    public FeatureType getFeatureType(String featureTypeName) {
        FeatureType featureType = featureTypeName == null ? null : featureTypesRegistryMap.get(featureTypeName);
        if (featureType == null) {
            throw new ResourceNotFoundException(
                "Couldn't find a feature type with name: " + featureTypeName + ". Make sure you have registered it."
            );
        }
        return featureType;
    }
}

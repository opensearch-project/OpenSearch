/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

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
    public static final Map<String, FeatureType> featureTypesRegistryMap = new HashMap<>();
    public static final int MAX_FEATURE_TYPE_NAME_LENGTH = 30;

    public static void registerFeatureType(FeatureType featureType) {
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
    }

    /**
     * Retrieves the registered {@link FeatureType} instance based on class name and feature type name.
     * This method assumes that FeatureTypes are singletons, meaning that each unique
     * (className, featureTypeName) pair corresponds to a single, globally shared instance.
     *
     * @param featureTypeName The name of the feature type.
     */
    public static FeatureType getFeatureType(String featureTypeName) {
        FeatureType featureType = featureTypesRegistryMap.get(featureTypeName);
        if (featureType == null) {
            throw new ResourceNotFoundException(
                "Couldn't find a feature type with name: " + featureTypeName + ". Make sure you have registered it."
            );
        }
        return featureType;
    }
}

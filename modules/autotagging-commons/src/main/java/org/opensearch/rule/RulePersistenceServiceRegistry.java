/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.rule.autotagging.FeatureType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class manages implementations of {@link RulePersistenceService}
 */
public class RulePersistenceServiceRegistry {
    private final Map<String, RulePersistenceService> rulePersistenceServices = new ConcurrentHashMap<>();

    /**
     * default constructor
     */
    public RulePersistenceServiceRegistry() {}

    /**
     * This method is used to register the concrete implementations of RulePersistenceService
     * @param featureType
     * @param rulePersistenceService
     */
    public void register(FeatureType featureType, RulePersistenceService rulePersistenceService) {
        if (rulePersistenceServices.put(featureType.getName(), rulePersistenceService) != null) {
            throw new IllegalArgumentException("Duplicate rule persistence service: " + featureType.getName());
        }
    }

    /**
     * It is used to get feature type specific {@link RulePersistenceService} implementation
     * @param featureType - the type of feature to retrieve the persistence service for
     */
    public RulePersistenceService getRulePersistenceService(FeatureType featureType) {
        if (!rulePersistenceServices.containsKey(featureType.getName())) {
            throw new IllegalArgumentException("Unknown feature type for persistence service: " + featureType.getName());
        }
        return rulePersistenceServices.get(featureType.getName());
    }
}

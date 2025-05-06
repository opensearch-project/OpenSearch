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
            throw new IllegalArgumentException("Unknown feature type: " + featureType.getName());
        }
        return rulePersistenceServices.get(featureType.getName());
    }

    /**
     * This method should only be used when exactly one persistence service is registered.
     * It assumes either that all rules are handled by a single implementation or that
     * the caller does not need to distinguish by feature type.
     *
     * @return the single registered {@link RulePersistenceService} instance
     */
    public RulePersistenceService getDefaultRulePersistenceService() {
        if (rulePersistenceServices.size() == 1) {
            return rulePersistenceServices.values().iterator().next();
        }
        throw new IllegalStateException("Multiple rule persistence services registered, cannot pick default.");
    }
}

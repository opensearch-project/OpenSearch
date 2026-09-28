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
 * This class manages implementations of {@link RuleRoutingService}
 */
public class RuleRoutingServiceRegistry {
    private final Map<String, RuleRoutingService> ruleRoutingServices = new ConcurrentHashMap<>();

    /**
     * default constructor
     */
    public RuleRoutingServiceRegistry() {}

    /**
     * This method is used to register the concrete implementations of RuleRoutingService
     * @param featureType
     * @param ruleRoutingService
     */
    public void register(FeatureType featureType, RuleRoutingService ruleRoutingService) {
        if (ruleRoutingServices.put(featureType.getName(), ruleRoutingService) != null) {
            throw new IllegalArgumentException("Duplicate rule routing service: " + featureType.getName());
        }
    }

    /**
     * It is used to get feature type specific {@link RuleRoutingService} implementation
     * @param featureType - the type of feature to retrieve the routing service for
     */
    public RuleRoutingService getRuleRoutingService(FeatureType featureType) {
        if (!ruleRoutingServices.containsKey(featureType.getName())) {
            throw new IllegalArgumentException("Unknown feature type for routing service: " + featureType.getName());
        }
        return ruleRoutingServices.get(featureType.getName());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.SecurityAttribute;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.FeatureValueValidator;

import java.util.List;
import java.util.Map;

/**
 * Represents a feature type specific to the workload group feature
 * @opensearch.experimental
 */
public class WorkloadGroupFeatureType implements FeatureType {
    /**
     * Name for WorkloadGroupFeatureType
     */
    public static final String NAME = "workload_group";
    private static final int MAX_ATTRIBUTE_VALUES = 10;
    private static final int MAX_ATTRIBUTE_VALUE_LENGTH = 100;
    private static final Map<String, Attribute> ALLOWED_ATTRIBUTES = Map.of(
        SecurityAttribute.PRINCIPAL.getName(),
        SecurityAttribute.PRINCIPAL,
        RuleAttribute.INDEX_PATTERN.getName(),
        RuleAttribute.INDEX_PATTERN
    );
    private final FeatureValueValidator featureValueValidator;

    /**
     * constructor for WorkloadGroupFeatureType
     * @param featureValueValidator
     */
    public WorkloadGroupFeatureType(FeatureValueValidator featureValueValidator) {
        this.featureValueValidator = featureValueValidator;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getMaxNumberOfValuesPerAttribute() {
        return MAX_ATTRIBUTE_VALUES;
    }

    @Override
    public int getMaxCharLengthPerAttributeValue() {
        return MAX_ATTRIBUTE_VALUE_LENGTH;
    }

    @Override
    public Map<String, Attribute> getAllowedAttributesRegistry() {
        return ALLOWED_ATTRIBUTES;
    }

    @Override
    public List<Attribute> getPrioritizedAttributesList() {
        return List.of(SecurityAttribute.PRINCIPAL, RuleAttribute.INDEX_PATTERN);
    }

    @Override
    public FeatureValueValidator getFeatureValueValidator() {
        return featureValueValidator;
    }
}

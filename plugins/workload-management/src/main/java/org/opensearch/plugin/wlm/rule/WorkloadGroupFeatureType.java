/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.FeatureValueValidator;

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
    private final Map<Attribute, Integer> orderedAttributes;
    private final FeatureValueValidator featureValueValidator;

    /**
     * constructor for WorkloadGroupFeatureType
     * @param featureValueValidator
     * @param orderedAttributes
     */
    public WorkloadGroupFeatureType(FeatureValueValidator featureValueValidator, Map<Attribute, Integer> orderedAttributes) {
        this.featureValueValidator = featureValueValidator;
        orderedAttributes.put(RuleAttribute.INDEX_PATTERN, 2);
        this.orderedAttributes = orderedAttributes;
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
    public Map<Attribute, Integer> getOrderedAttributes() {
        return orderedAttributes;
    }

    @Override
    public FeatureValueValidator getFeatureValueValidator() {
        return featureValueValidator;
    }
}

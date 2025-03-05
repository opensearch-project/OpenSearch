/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.common.ValidationException;
import org.joda.time.Instant;

import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.metadata.QueryGroup.isValid;

/**
 * This is the validator for rule. It ensures that the rule has a valid description, label,
 * update time, attribute map, and the rule adheres to the feature type's constraints.
 *
 * @opensearch.experimental
 */
public class RuleValidator<T extends FeatureType> {
    private final String description;
    private final Map<Attribute, Set<String>> attributeMap;
    private final String label;
    private final String updatedAt;
    private final T featureType;
    private final ValidationException validationException = new ValidationException();

    public RuleValidator(String description, Map<Attribute, Set<String>> attributeMap, String label, String updatedAt, T featureType) {
        this.description = description;
        this.attributeMap = attributeMap;
        this.label = label;
        this.updatedAt = updatedAt;
        this.featureType = featureType;
    }

    public void validate() {
        validateRequiredFields();
        validateFeatureType();
        validateUpdatedAtEpoch();
        validateAttributeMap();
        if (!validationException.validationErrors().isEmpty()) {
            throw new IllegalArgumentException(validationException);
        }
    }

    private void validateRequiredFields() {
        requireNonNullOrEmpty(description, "Rule description can't be null or empty");
        requireNonNullOrEmpty(label, "Rule label can't be null or empty");
        requireNonNullOrEmpty(updatedAt, "Rule update time can't be null or empty");
    }

    private void validateFeatureType() {
        if (featureType == null) {
            validationException.addValidationError("Couldn't identify which feature the rule belongs to. Rule feature can't be null.");
        }
    }

    private void validateUpdatedAtEpoch() {
        if (updatedAt != null && !isValid(Instant.parse(updatedAt).getMillis())) {
            validationException.addValidationError("Rule update time is not a valid epoch");
        }
    }

    private void validateAttributeMap() {
        if (attributeMap == null || attributeMap.isEmpty()) {
            validationException.addValidationError("Rule should have at least 1 attribute requirement");
        }

        if (attributeMap != null && featureType != null) {
            for (Map.Entry<Attribute, Set<String>> entry : attributeMap.entrySet()) {
                Attribute attribute = entry.getKey();
                Set<String> attributeValues = entry.getValue();
                validateAttributeExistence(attribute);
                validateMaxAttributeValues(attribute, attributeValues);
                validateAttributeValuesLength(attributeValues);
            }
        }
    }

    private void validateAttributeExistence(Attribute attribute) {
        if (featureType.getAttributeFromName(attribute.getName()) == null) {
            validationException.addValidationError(
                attribute.getName() + " is not a valid attribute within the " + featureType.getName() + " feature."
            );
        }
    }

    private void validateMaxAttributeValues(Attribute attribute, Set<String> attributeValues) {
        int maxSize = featureType.getMaxNumberOfValuesPerAttribute();
        int actualSize = attributeValues.size();
        if (actualSize > maxSize) {
            validationException.addValidationError(
                "Each attribute can only have a maximum of "
                    + maxSize
                    + " values. The input attribute "
                    + attribute
                    + " has length "
                    + attributeValues.size()
                    + ", which exceeds this limit."
            );
        }
    }

    private void validateAttributeValuesLength(Set<String> attributeValues) {
        int maxValueLength = featureType.getMaxCharLengthPerAttributeValue();
        for (String attributeValue : attributeValues) {
            if (attributeValue.isEmpty() || attributeValue.length() > maxValueLength) {
                validationException.addValidationError(
                    "Attribute value [" + attributeValue + "] is invalid (empty or exceeds " + maxValueLength + " characters)"
                );
            }
        }
    }

    private void requireNonNullOrEmpty(String value, String errorMessage) {
        if (value == null || value.isEmpty()) {
            validationException.addValidationError(errorMessage);
        }
    }
}

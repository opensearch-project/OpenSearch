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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.cluster.metadata.QueryGroup.isValid;

/**
 * This is the validator for rule. It ensures that the rule has a valid description, feature value,
 * update time, attribute map, and the rule adheres to the feature type's constraints.
 *
 * @opensearch.experimental
 */
public class RuleValidator {
    private final String description;
    private final Map<Attribute, Set<String>> attributeMap;
    private final String featureValue;
    private final String updatedAt;
    private final FeatureType featureType;
    public static final int MAX_DESCRIPTION_LENGTH = 256;

    public RuleValidator(
        String description,
        Map<Attribute, Set<String>> attributeMap,
        String featureValue,
        String updatedAt,
        FeatureType featureType
    ) {
        this.description = description;
        this.attributeMap = attributeMap;
        this.featureValue = featureValue;
        this.updatedAt = updatedAt;
        this.featureType = featureType;
    }

    public void validate() {
        List<String> errorMessages = new ArrayList<>();
        errorMessages.addAll(validateStringFields());
        errorMessages.addAll(validateFeatureType());
        errorMessages.addAll(validateUpdatedAtEpoch());
        errorMessages.addAll(validateAttributeMap());
        if (!errorMessages.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(errorMessages);
            throw new IllegalArgumentException(validationException);
        }
    }

    private List<String> validateStringFields() {
        List<String> errors = new ArrayList<>();
        if (isNullOrEmpty(description)) {
            errors.add("Rule description can't be null or empty");
        } else if (description.length() > MAX_DESCRIPTION_LENGTH) {
            errors.add("Rule description cannot exceed " + MAX_DESCRIPTION_LENGTH + " characters.");
        }
        if (isNullOrEmpty(featureValue)) {
            errors.add("Rule featureValue can't be null or empty");
        }
        if (isNullOrEmpty(updatedAt)) {
            errors.add("Rule update time can't be null or empty");
        }
        return errors;
    }

    private boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    private List<String> validateFeatureType() {
        if (featureType == null) {
            return List.of("Couldn't identify which feature the rule belongs to. Rule feature can't be null.");
        }
        return new ArrayList<>();
    }

    private List<String> validateUpdatedAtEpoch() {
        if (updatedAt != null && !isValid(Instant.parse(updatedAt).getMillis())) {
            return List.of("Rule update time is not a valid epoch");
        }
        return new ArrayList<>();
    }

    private List<String> validateAttributeMap() {
        List<String> errors = new ArrayList<>();
        if (attributeMap == null || attributeMap.isEmpty()) {
            errors.add("Rule should have at least 1 attribute requirement");
        }

        if (attributeMap != null && featureType != null) {
            for (Map.Entry<Attribute, Set<String>> entry : attributeMap.entrySet()) {
                Attribute attribute = entry.getKey();
                Set<String> attributeValues = entry.getValue();
                errors.addAll(validateAttributeExistence(attribute));
                errors.addAll(validateMaxAttributeValues(attribute, attributeValues));
                errors.addAll(validateAttributeValuesLength(attributeValues));
            }
        }
        return errors;
    }

    private List<String> validateAttributeExistence(Attribute attribute) {
        if (featureType.getAttributeFromName(attribute.getName()) == null) {
            return List.of(attribute.getName() + " is not a valid attribute within the " + featureType.getName() + " feature.");
        }
        return new ArrayList<>();
    }

    private List<String> validateMaxAttributeValues(Attribute attribute, Set<String> attributeValues) {
        List<String> errors = new ArrayList<>();
        String attributeName = attribute.getName();
        if (attributeValues.isEmpty()) {
            errors.add("Attribute values for " + attributeName + " cannot be empty.");
        }
        int maxSize = featureType.getMaxNumberOfValuesPerAttribute();
        int actualSize = attributeValues.size();
        if (actualSize > maxSize) {
            errors.add(
                "Each attribute can only have a maximum of "
                    + maxSize
                    + " values. The input attribute "
                    + attributeName
                    + " has length "
                    + attributeValues.size()
                    + ", which exceeds this limit."
            );
        }
        return errors;
    }

    private List<String> validateAttributeValuesLength(Set<String> attributeValues) {
        int maxValueLength = featureType.getMaxCharLengthPerAttributeValue();
        for (String attributeValue : attributeValues) {
            if (attributeValue.isEmpty() || attributeValue.length() > maxValueLength) {
                return List.of("Attribute value [" + attributeValue + "] is invalid (empty or exceeds " + maxValueLength + " characters)");
            }
        }
        return new ArrayList<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RuleValidator that = (RuleValidator) o;
        return Objects.equals(description, that.description)
            && Objects.equals(attributeMap, that.attributeMap)
            && Objects.equals(featureValue, that.featureValue)
            && Objects.equals(updatedAt, that.updatedAt)
            && Objects.equals(featureType, that.featureType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, attributeMap, featureValue, updatedAt, featureType);
    }
}

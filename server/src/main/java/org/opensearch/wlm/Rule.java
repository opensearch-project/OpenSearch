/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.ValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.cluster.metadata.QueryGroup.isValid;

/**
 * Represents a rule schema used for automatic query tagging in the system.
 * This class encapsulates the criteria (defined through attributes) for automatically applying relevant
 * tags to queries based on matching attribute patterns. This class provides an in-memory representation
 * of a rule. The indexed view may differ in representation.
 * {
 *     "_id": "fwehf8302582mglfio349==",
 *     "index_pattern": ["logs123", "user*"],
 *     "query_group": "dev_query_group_id",
 *     "updated_at": "01-10-2025T21:23:21.456Z"
 * }
 * @opensearch.experimental
 */
public class Rule implements Writeable, ToXContentObject {
    private final Map<RuleAttribute, Set<String>> attributeMap;
    private final Feature feature;
    private final String label;
    private final String updatedAt;
    public static final String _ID_STRING = "_id";
    public static final String UPDATED_AT_STRING = "updated_at";
    public static final int MAX_NUMBER_OF_VALUES_PER_ATTRIBUTE = 10;
    public static final int MAX_CHARACTER_LENGTH_PER_ATTRIBUTE_VALUE_STRING = 100;

    public Rule(Map<RuleAttribute, Set<String>> attributeMap, String label, String updatedAt, Feature feature) {
        ValidationException validationException = new ValidationException();
        validateRuleInputs(attributeMap, label, updatedAt, feature, validationException);
        if (!validationException.validationErrors().isEmpty()) {
            throw new IllegalArgumentException(validationException);
        }

        this.attributeMap = attributeMap;
        this.feature = feature;
        this.label = label;
        this.updatedAt = updatedAt;
    }

    public Rule(StreamInput in) throws IOException {
        this(
            in.readMap((i) -> RuleAttribute.fromName(i.readString()), i -> new HashSet<>(i.readStringList())),
            in.readString(),
            in.readString(),
            Feature.fromName(in.readString())
        );
    }

    public static void requireNonNullOrEmpty(String value, String message, ValidationException validationException) {
        if (value == null || value.isEmpty()) {
            validationException.addValidationError(message);
        }
    }

    public static void validateRuleInputs(
        Map<RuleAttribute, Set<String>> attributeMap,
        String label,
        String updatedAt,
        Feature feature,
        ValidationException validationException
    ) {
        if (feature == null) {
            validationException.addValidationError("Couldn't identify which feature the rule belongs to. Rule feature name can't be null.");
        }
        requireNonNullOrEmpty(label, "Rule label can't be null or empty", validationException);
        requireNonNullOrEmpty(updatedAt, "Rule update time can't be null or empty", validationException);
        if (attributeMap == null || attributeMap.isEmpty()) {
            validationException.addValidationError("Rule should have at least 1 attribute requirement");
        }
        if (updatedAt != null && !isValid(Instant.parse(updatedAt).getMillis())) {
            validationException.addValidationError("Rule update time is not a valid epoch");
        }
        if (attributeMap != null && feature != null) {
            validateAttributeMap(attributeMap, feature, validationException);
        }
    }

    public static void validateAttributeMap(
        Map<RuleAttribute, Set<String>> attributeMap,
        Feature feature,
        ValidationException validationException
    ) {
        for (Map.Entry<RuleAttribute, Set<String>> entry : attributeMap.entrySet()) {
            RuleAttribute ruleAttribute = entry.getKey();
            Set<String> attributeValues = entry.getValue();
            if (!feature.isValidAttribute(ruleAttribute)) {
                validationException.addValidationError(
                    ruleAttribute.getName() + " is not a valid attribute name under the feature: " + feature.getName()
                );
            }
            if (attributeValues.size() > MAX_NUMBER_OF_VALUES_PER_ATTRIBUTE) {
                validationException.addValidationError(
                    "Each attribute can only have a maximum of 10 values. The input attribute " + ruleAttribute + " exceeds this limit."
                );
            }
            for (String attributeValue : attributeValues) {
                if (attributeValue.isEmpty() || attributeValue.length() > MAX_CHARACTER_LENGTH_PER_ATTRIBUTE_VALUE_STRING) {
                    validationException.addValidationError(
                        "Attribute value [" + attributeValue + "] is invalid (empty or exceeds 100 characters)"
                    );
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(attributeMap, RuleAttribute::writeTo, StreamOutput::writeStringCollection);
        out.writeString(label);
        out.writeString(updatedAt);
        out.writeString(feature.getName());
    }

    public static Rule fromXContent(final XContentParser parser) throws IOException {
        return Builder.fromXContent(parser).build();
    }

    public String getLabel() {
        return label;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public Feature getFeature() {
        return feature;
    }

    public Map<RuleAttribute, Set<String>> getAttributeMap() {
        return attributeMap;
    }

    /**
     * This enum enumerates the features that can use the Rule Based Auto-tagging
     * @opensearch.experimental
     */
    public enum Feature {
        QUERY_GROUP("query_group", Set.of(RuleAttribute.INDEX_PATTERN));

        private final String name;
        private final Set<RuleAttribute> allowedAttributes;

        Feature(String name, Set<RuleAttribute> allowedAttributes) {
            this.name = name;
            this.allowedAttributes = allowedAttributes;
        }

        public String getName() {
            return name;
        }

        public Set<RuleAttribute> getAllowedAttributes() {
            return allowedAttributes;
        }

        public boolean isValidAttribute(RuleAttribute attribute) {
            return allowedAttributes.contains(attribute);
        }

        public static boolean isValidFeature(String s) {
            return Arrays.stream(values()).anyMatch(feature -> feature.getName().equalsIgnoreCase(s));
        }

        public static Feature fromName(String s) {
            return Arrays.stream(values())
                .filter(feature -> feature.getName().equalsIgnoreCase(s))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid value for Feature: " + s));
        }
    }

    /**
     * This RuleAttribute enum contains the attribute names for a rule.
     * @opensearch.experimental
     */
    public enum RuleAttribute {
        INDEX_PATTERN("index_pattern");

        private final String name;

        RuleAttribute(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static void writeTo(StreamOutput out, RuleAttribute ruleAttribute) throws IOException {
            out.writeString(ruleAttribute.getName());
        }

        public static RuleAttribute fromName(String s) {
            for (RuleAttribute attribute : values()) {
                if (attribute.getName().equalsIgnoreCase(s)) return attribute;

            }
            throw new IllegalArgumentException("Invalid value for RuleAttribute: " + s);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        String id = params.param(_ID_STRING);
        if (id != null) {
            builder.field(_ID_STRING, id);
        }
        for (Map.Entry<RuleAttribute, Set<String>> entry : attributeMap.entrySet()) {
            builder.array(entry.getKey().getName(), entry.getValue().toArray(new String[0]));
        }
        builder.field(feature.getName(), label);
        builder.field(UPDATED_AT_STRING, updatedAt);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rule that = (Rule) o;
        return Objects.equals(label, that.label)
            && Objects.equals(feature, that.feature)
            && Objects.equals(attributeMap, that.attributeMap)
            && Objects.equals(updatedAt, that.updatedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, feature, attributeMap, updatedAt);
    }

    /**
     * builder method for the {@link Rule}
     * @return Builder object
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for {@link Rule}
     * @opensearch.experimental
     */
    public static class Builder {
        private Map<RuleAttribute, Set<String>> attributeMap;
        private Feature feature;
        private String label;
        private String updatedAt;

        private Builder() {}

        public static Builder fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == null) {
                parser.nextToken();
            }
            Builder builder = builder();
            XContentParser.Token token = parser.currentToken();

            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT token but found [" + parser.currentName() + "]");
            }
            Map<RuleAttribute, Set<String>> attributeMap1 = new HashMap<>();
            String fieldName = "";
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (Feature.isValidFeature(fieldName)) {
                        builder.feature(fieldName);
                        builder.label(parser.text());
                    } else if (fieldName.equals(UPDATED_AT_STRING)) {
                        builder.updatedAt(parser.text());
                    } else {
                        throw new IllegalArgumentException(fieldName + " is not a valid field in Rule");
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    fromXContentParseArray(parser, fieldName, attributeMap1);
                }
            }
            return builder.attributeMap(attributeMap1);
        }

        public static void fromXContentParseArray(XContentParser parser, String fieldName, Map<RuleAttribute, Set<String>> attributeMap)
            throws IOException {
            RuleAttribute ruleAttribute = RuleAttribute.fromName(fieldName);
            Set<String> attributeValueSet = new HashSet<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                    attributeValueSet.add(parser.text());
                } else {
                    throw new XContentParseException("Unexpected token in array: " + parser.currentToken());
                }
            }
            attributeMap.put(ruleAttribute, attributeValueSet);
        }

        public Builder label(String label) {
            this.label = label;
            return this;
        }

        public Builder attributeMap(Map<RuleAttribute, Set<String>> attributeMap) {
            this.attributeMap = attributeMap;
            return this;
        }

        public Builder feature(String feature) {
            this.feature = Feature.fromName(feature);
            return this;
        }

        public Builder updatedAt(String updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Rule build() {
            return new Rule(attributeMap, label, updatedAt, feature);
        }

        public String getLabel() {
            return label;
        }

        public Map<RuleAttribute, Set<String>> getAttributeMap() {
            return attributeMap;
        }
    }
}

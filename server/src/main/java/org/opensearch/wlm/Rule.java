/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.cluster.metadata.QueryGroup.isValid;

/**
 * Class to define the Rule schema
 * {
 *     "_id": "fwehf8302582mglfio349==",
 *     "index_pattern": ["logs123", "user*"],
 *     "query_group": "dev_query_group_id",
 *     "updated_at": "01-10-2025T21:23:21.456Z"
 * }
 */
@ExperimentalApi
public class Rule extends AbstractDiffable<Rule> implements ToXContentObject {
    private final String _id;
    private final Map<RuleAttribute, Set<String>> attributeMap;
    private final Feature feature;
    private final String label;
    private final String updatedAt;
    public static final Map<Feature, Set<RuleAttribute>> featureAlloedAttributesMap = Map.of(
        Feature.QUERY_GROUP,
        Set.of(RuleAttribute.INDEX_PATTERN)
    );

    public Rule(String _id, Map<RuleAttribute, Set<String>> attributeMap, String label, String updatedAt, Feature feature) {
        requireNonNullOrEmpty(_id, "Rule _id can't be null or empty");
        Objects.requireNonNull(feature, "Couldn't identify which feature the rule belongs to. Rule feature name can't be null.");
        requireNonNullOrEmpty(label, feature.getName() + " value can't be null or empty");
        requireNonNullOrEmpty(updatedAt, "Rule update time can't be null or empty");
        if (attributeMap == null || attributeMap.isEmpty()) {
            throw new IllegalArgumentException("Rule should have at least 1 attribute requirement");
        }
        if (!isValid(Instant.parse(updatedAt).getMillis())) {
            throw new IllegalArgumentException("Rule update time is not a valid epoch");
        }
        validatedAttributeMap(attributeMap, feature);

        this._id = _id;
        this.attributeMap = attributeMap;
        this.feature = feature;
        this.label = label;
        this.updatedAt = updatedAt;
    }

    public Rule(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readMap((i) -> RuleAttribute.fromName(i.readString()), i -> new HashSet<>(i.readStringList())),
            in.readString(),
            in.readString(),
            Feature.fromName(in.readString())
        );
    }

    public static void requireNonNullOrEmpty(String value, String message) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void validatedAttributeMap(Map<RuleAttribute, Set<String>> attributeMap, Feature feature) {
        if (!featureAlloedAttributesMap.containsKey(feature)) {
            throw new IllegalArgumentException("Couldn't find any valid attribute name under the feature: " + feature.getName());
        }
        Set<RuleAttribute> ValidAttributesForFeature = featureAlloedAttributesMap.get(feature);
        for (Map.Entry<RuleAttribute, Set<String>> entry : attributeMap.entrySet()) {
            RuleAttribute ruleAttribute = entry.getKey();
            Set<String> attributeValues = entry.getValue();
            if (!ValidAttributesForFeature.contains(ruleAttribute)) {
                throw new IllegalArgumentException(
                    ruleAttribute.getName() + " is not a valid attribute name under the feature: " + feature.getName()
                );
            }
            if (attributeValues.size() > 10) {
                throw new IllegalArgumentException(
                    "Each attribute can only have a maximum of 10 values. The input attribute " + ruleAttribute + " exceeds this limit."
                );
            }
            for (String attributeValue : attributeValues) {
                if (attributeValue.isEmpty()) {
                    throw new IllegalArgumentException("Attribute value should not be an empty string");
                }
                if (attributeValue.length() > 100) {
                    throw new IllegalArgumentException(
                        "Attribute value can only have a maximum of 100 characters. The input " + attributeValue + " exceeds this limit."
                    );
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(_id);
        out.writeMap(attributeMap, RuleAttribute::writeTo, StreamOutput::writeStringCollection);
        out.writeString(label);
        out.writeString(updatedAt);
        out.writeString(feature.getName());
    }

    public static Rule fromXContent(final XContentParser parser) throws IOException {
        return Builder.fromXContent(parser).build();
    }

    public String get_id() {
        return _id;
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
     * This Feature enum contains the different feature names for each rule.
     * For example, if we're creating a rule for WLM/QueryGroup, the rule will contain the line
     * "query_group": "query_group_id",
     * so the feature name would be "query_group" in this case.
     */
    @ExperimentalApi
    public enum Feature {
        QUERY_GROUP("query_group");

        private final String name;

        Feature(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static boolean isValidFeature(String s) {
            for (Feature feature : values()) {
                if (feature.getName().equalsIgnoreCase(s)) {
                    return true;
                }
            }
            return false;
        }

        public static Feature fromName(String s) {
            for (Feature feature : values()) {
                if (feature.getName().equalsIgnoreCase(s)) return feature;

            }
            throw new IllegalArgumentException("Invalid value for Feature: " + s);
        }
    }

    /**
     * This RuleAttribute enum contains the attribute names for a rule.
     */
    @ExperimentalApi
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
        builder.field("_id", _id);
        for (Map.Entry<RuleAttribute, Set<String>> entry : attributeMap.entrySet()) {
            builder.array(entry.getKey().getName(), entry.getValue().toArray(new String[0]));
        }
        builder.field(feature.getName(), label);
        builder.field("updated_at", updatedAt);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentWithoutId(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<RuleAttribute, Set<String>> entry : attributeMap.entrySet()) {
            builder.array(entry.getKey().getName(), entry.getValue().toArray(new String[0]));
        }
        builder.field(feature.getName(), label);
        builder.field("updated_at", updatedAt);
        builder.endObject();
        return builder;
    }

    public static Diff<Rule> readDiff(final StreamInput in) throws IOException {
        return readDiffFrom(Rule::new, in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rule that = (Rule) o;
        return Objects.equals(_id, that._id)
            && Objects.equals(label, that.label)
            && Objects.equals(feature, that.feature)
            && Objects.equals(attributeMap, that.attributeMap)
            && Objects.equals(updatedAt, that.updatedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_id, label, feature, attributeMap, updatedAt);
    }

    /**
     * empty builder method for the {@link Rule}
     * @return Builder object
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * builder method for the {@link Rule}
     * @return Builder object
     */
    public Builder builderFromRule() {
        return new Builder()._id(_id).label(label).feature(feature.getName()).updatedAt(updatedAt).attributeMap(attributeMap);
    }

    /**
     * Builder class for {@link Rule}
     */
    @ExperimentalApi
    public static class Builder {
        private String _id;
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
                    if (fieldName.equals("_id")) {
                        builder._id(parser.text());
                    } else if (Feature.isValidFeature(fieldName)) {
                        builder.feature(fieldName);
                        builder.label(parser.text());
                    } else if (fieldName.equals("updated_at")) {
                        builder.updatedAt(parser.text());
                    } else {
                        throw new IllegalArgumentException(fieldName + " is not a valid field in Rule");
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    RuleAttribute ruleAttribute = RuleAttribute.fromName(fieldName);
                    Set<String> indexPatternList = new HashSet<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                            indexPatternList.add(parser.text());
                        } else {
                            throw new XContentParseException("Unexpected token in array: " + parser.currentToken());
                        }
                    }
                    attributeMap1.put(ruleAttribute, indexPatternList);
                }
            }
            return builder.attributeMap(attributeMap1);
        }

        public Builder _id(String _id) {
            this._id = _id;
            return this;
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
            return new Rule(_id, attributeMap, label, updatedAt, feature);
        }

        public String getLabel() {
            return label;
        }
    }
}

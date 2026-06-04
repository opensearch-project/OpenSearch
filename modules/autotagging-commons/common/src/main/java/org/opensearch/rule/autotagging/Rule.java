/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.autotagging;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rule.RuleUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a rule schema used for automatic query tagging in the system.
 * This class encapsulates the criteria (defined through attributes) for automatically applying relevant
 * tags to queries based on matching attribute patterns. This class provides an in-memory representation
 * of a rule. The indexed view may differ in representation.
 * {
 *     "id": "fwehf8302582mglfio349==",
 *     "description": "Assign Workload Group for Index Logs123"
 *     "index_pattern": ["logs123"],
 *     "workload_group": "dev_workload_group_id",
 *     "updated_at": "01-10-2025T21:23:21.456Z"
 * }
 * @opensearch.experimental
 */
public class Rule implements Writeable, ToXContentObject {
    private final String id;
    private final String description;
    private final FeatureType featureType;
    private final Map<Attribute, Set<String>> attributeMap;
    private final String featureValue;
    private final String updatedAt;
    private final RuleValidator ruleValidator;
    /**
     * id field
     */
    public static final String ID_STRING = "id";
    /**
     * description field
     */
    public static final String DESCRIPTION_STRING = "description";
    /**
     * updated_at field
     */
    public static final String UPDATED_AT_STRING = "updated_at";

    /**
     * Main constructor
     * @param id
     * @param description
     * @param attributeMap
     * @param featureType
     * @param featureValue
     * @param updatedAt
     */
    public Rule(
        String id,
        String description,
        Map<Attribute, Set<String>> attributeMap,
        FeatureType featureType,
        String featureValue,
        String updatedAt
    ) {
        this.id = id;
        this.description = description;
        this.featureType = featureType;
        this.attributeMap = attributeMap;
        this.featureValue = featureValue;
        this.updatedAt = updatedAt;
        this.ruleValidator = new RuleValidator(id, description, attributeMap, featureValue, updatedAt, featureType);
        this.ruleValidator.validate();
    }

    /**
     * constructor to init the object from StreamInput
     * @param in
     * @throws IOException
     */
    public Rule(StreamInput in) throws IOException {
        id = in.readString();
        description = in.readString();
        featureType = FeatureType.from(in);
        attributeMap = in.readMap(i -> Attribute.from(i, featureType), i -> new HashSet<>(i.readStringList()));
        featureValue = in.readString();
        updatedAt = in.readString();
        this.ruleValidator = new RuleValidator(id, description, attributeMap, featureValue, updatedAt, featureType);
        this.ruleValidator.validate();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(description);
        featureType.writeTo(out);
        out.writeMap(attributeMap, (output, attribute) -> attribute.writeTo(output), StreamOutput::writeStringCollection);
        out.writeString(featureValue);
        out.writeString(updatedAt);
    }

    /**
     * static utility method to parse the Rule object
     * @param parser
     * @param featureType
     * @return
     * @throws IOException
     */
    public static Rule fromXContent(final XContentParser parser, FeatureType featureType) throws IOException {
        return Builder.fromXContent(parser, featureType).build();
    }

    /**
     * id getter
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     * description getter
     * @return
     */
    public String getDescription() {
        return description;
    }

    /**
     * feature value getter
     * @return
     */
    public String getFeatureValue() {
        return featureValue;
    }

    /**
     * updatedAt getter
     * @return
     */
    public String getUpdatedAt() {
        return updatedAt;
    }

    /**
     * FeatureType getter
     * @return
     */
    public FeatureType getFeatureType() {
        return featureType;
    }

    /**
     * attribute map getter
     * @return
     */
    public Map<Attribute, Set<String>> getAttributeMap() {
        return attributeMap;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(ID_STRING, id);
        builder.field(DESCRIPTION_STRING, description);
        for (Map.Entry<Attribute, Set<String>> entry : attributeMap.entrySet()) {
            entry.getKey().toXContentWriteAttributeValues(builder, entry.getValue());
        }
        builder.field(featureType.getName(), featureValue);
        builder.field(UPDATED_AT_STRING, updatedAt);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rule that = (Rule) o;
        return Objects.equals(description, that.description)
            && Objects.equals(featureValue, that.featureValue)
            && Objects.equals(featureType, that.featureType)
            && Objects.equals(attributeMap, that.attributeMap)
            && Objects.equals(ruleValidator, that.ruleValidator)
            && Objects.equals(updatedAt, that.updatedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, featureValue, featureType, attributeMap, updatedAt);
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
        private String id;
        private String description;
        private Map<Attribute, Set<String>> attributeMap;
        private FeatureType featureType;
        private String featureValue;
        private String updatedAt;

        private Builder() {}

        /**
         * Parses the Rule object from XContentParser
         * @param parser
         * @param featureType
         * @return
         * @throws IOException
         */
        public static Builder fromXContent(XContentParser parser, FeatureType featureType) throws IOException {
            if (parser.currentToken() == null) {
                parser.nextToken();
            }
            Builder builder = builder();
            XContentParser.Token token = parser.currentToken();

            if (token != XContentParser.Token.START_OBJECT) {
                throw new XContentParseException("Expected START_OBJECT token but found [" + parser.currentName() + "]");
            }
            Map<Attribute, Set<String>> attributeMap1 = new HashMap<>();
            String fieldName = "";
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (fieldName.equals(ID_STRING)) {
                        builder.id(parser.text());
                    } else if (fieldName.equals(DESCRIPTION_STRING)) {
                        builder.description(parser.text());
                    } else if (fieldName.equals(UPDATED_AT_STRING)) {
                        builder.updatedAt(parser.text());
                    } else if (fieldName.equals(featureType.getName())) {
                        builder.featureType(featureType);
                        builder.featureValue(parser.text());
                    }
                } else if (token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.START_OBJECT) {
                    fromXContentParseAttribute(parser, fieldName, featureType, attributeMap1);
                }
            }
            return builder.attributeMap(attributeMap1);
        }

        private static void fromXContentParseAttribute(
            XContentParser parser,
            String fieldName,
            FeatureType featureType,
            Map<Attribute, Set<String>> attributeMap
        ) throws IOException {
            Attribute attribute = featureType.getAttributeFromName(fieldName);
            if (attribute == null) {
                throw new XContentParseException(fieldName + " is not a valid attribute within the " + featureType.getName() + " feature.");
            }
            attributeMap.put(attribute, attribute.fromXContentParseAttributeValues(parser));
        }

        /**
         * Sets the id
         * @param id
         * @return
         */
        public Builder id(String id) {
            this.id = id;
            return this;
        }

        /**
         * sets the id based on description, featureType, attributeMap, and featureValue
         * @return
         */
        public Builder id() {
            if (description == null || featureType == null || attributeMap == null || featureValue == null) {
                throw new IllegalStateException("Cannot compute ID: required fields are missing.");
            }
            this.id = RuleUtils.computeRuleHash(description, featureType, attributeMap, featureValue);
            return this;
        }

        /**
         * sets the description
         * @param description
         * @return
         */
        public Builder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * sets the feature value
         * @param featureValue
         * @return
         */
        public Builder featureValue(String featureValue) {
            this.featureValue = featureValue;
            return this;
        }

        /**
         * Sets the attribute map
         * @param attributeMap
         * @return
         */
        public Builder attributeMap(Map<Attribute, Set<String>> attributeMap) {
            this.attributeMap = attributeMap;
            return this;
        }

        /**
         * sets the feature type
         * @param featureType
         * @return
         */
        public Builder featureType(FeatureType featureType) {
            this.featureType = featureType;
            return this;
        }

        /**
         * sets the updatedAt
         * @param updatedAt
         * @return
         */
        public Builder updatedAt(String updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        /**
         * Builds the Rule object
         * @return
         */
        public Rule build() {
            return new Rule(id, description, attributeMap, featureType, featureValue, updatedAt);
        }

        /**
         * Returns feature type for Rule
         * @return
         */
        public String getFeatureValue() {
            return featureValue;
        }

        /**
         * Returns attribute map
         * @return
         */
        public Map<Attribute, Set<String>> getAttributeMap() {
            return attributeMap;
        }

        /**
         * Returns description
         * @return
         */
        public String getDescription() {
            return description;
        }
    }
}

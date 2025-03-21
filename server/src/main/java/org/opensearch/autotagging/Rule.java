/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;

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
 *     "_id": "fwehf8302582mglfio349==",
 *     "description": "Assign Query Group for Index Logs123"
 *     "index_pattern": ["logs123"],
 *     "query_group": "dev_query_group_id",
 *     "updated_at": "01-10-2025T21:23:21.456Z"
 * }
 * @opensearch.experimental
 */
public class Rule implements Writeable, ToXContentObject {
    private final String description;
    private final FeatureType featureType;
    private final Map<Attribute, Set<String>> attributeMap;
    private final String featureValue;
    private final String updatedAt;
    private final RuleValidator ruleValidator;
    public static final String _ID_STRING = "_id";
    public static final String DESCRIPTION_STRING = "description";
    public static final String UPDATED_AT_STRING = "updated_at";

    public Rule(
        String description,
        Map<Attribute, Set<String>> attributeMap,
        FeatureType featureType,
        String featureValue,
        String updatedAt
    ) {
        this.description = description;
        this.featureType = featureType;
        this.attributeMap = attributeMap;
        this.featureValue = featureValue;
        this.updatedAt = updatedAt;
        this.ruleValidator = new RuleValidator(description, attributeMap, featureValue, updatedAt, featureType);
        this.ruleValidator.validate();
    }

    public Rule(StreamInput in) throws IOException {
        description = in.readString();
        featureType = FeatureType.from(in);
        attributeMap = in.readMap(i -> Attribute.from(i, featureType), i -> new HashSet<>(i.readStringList()));
        featureValue = in.readString();
        updatedAt = in.readString();
        this.ruleValidator = new RuleValidator(description, attributeMap, featureValue, updatedAt, featureType);
        this.ruleValidator.validate();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(description);
        featureType.writeTo(out);
        out.writeMap(attributeMap, (output, attribute) -> attribute.writeTo(output), StreamOutput::writeStringCollection);
        out.writeString(featureValue);
        out.writeString(updatedAt);
    }

    public static Rule fromXContent(final XContentParser parser, FeatureType featureType) throws IOException {
        return Builder.fromXContent(parser, featureType).build();
    }

    public String getDescription() {
        return description;
    }

    public String getFeatureValue() {
        return featureValue;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public FeatureType getFeatureType() {
        return featureType;
    }

    public Map<Attribute, Set<String>> getAttributeMap() {
        return attributeMap;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        String id = params.param(_ID_STRING);
        if (id != null) {
            builder.field(_ID_STRING, id);
        }
        builder.field(DESCRIPTION_STRING, description);
        for (Map.Entry<Attribute, Set<String>> entry : attributeMap.entrySet()) {
            builder.array(entry.getKey().getName(), entry.getValue().toArray(new String[0]));
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
        private String description;
        private Map<Attribute, Set<String>> attributeMap;
        private FeatureType featureType;
        private String featureValue;
        private String updatedAt;

        private Builder() {}

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
                    if (fieldName.equals(DESCRIPTION_STRING)) {
                        builder.description(parser.text());
                    } else if (fieldName.equals(UPDATED_AT_STRING)) {
                        builder.updatedAt(parser.text());
                    } else if (fieldName.equals(featureType.getName())) {
                        builder.featureType(featureType);
                        builder.featureValue(parser.text());
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    fromXContentParseArray(parser, fieldName, featureType, attributeMap1);
                }
            }
            return builder.attributeMap(attributeMap1);
        }

        public static void fromXContentParseArray(
            XContentParser parser,
            String fieldName,
            FeatureType featureType,
            Map<Attribute, Set<String>> attributeMap
        ) throws IOException {
            Attribute attribute = featureType.getAttributeFromName(fieldName);
            if (attribute == null) {
                throw new XContentParseException(fieldName + " is not a valid attribute within the " + featureType.getName() + " feature.");
            }
            Set<String> attributeValueSet = new HashSet<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                    attributeValueSet.add(parser.text());
                } else {
                    throw new XContentParseException("Unexpected token in array: " + parser.currentToken());
                }
            }
            attributeMap.put(attribute, attributeValueSet);
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder featureValue(String featureValue) {
            this.featureValue = featureValue;
            return this;
        }

        public Builder attributeMap(Map<Attribute, Set<String>> attributeMap) {
            this.attributeMap = attributeMap;
            return this;
        }

        public Builder featureType(FeatureType featureType) {
            this.featureType = featureType;
            return this;
        }

        public Builder updatedAt(String updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Rule build() {
            return new Rule(description, attributeMap, featureType, featureValue, updatedAt);
        }

        public String getFeatureValue() {
            return featureValue;
        }

        public Map<Attribute, Set<String>> getAttributeMap() {
            return attributeMap;
        }
    }
}

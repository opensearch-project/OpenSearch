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
public class Rule<T extends FeatureType> implements Writeable, ToXContentObject {
    private final String description;
    private final Map<Attribute, Set<String>> attributeMap;
    private final T featureType;
    private final String label;
    private final String updatedAt;
    public static final String _ID_STRING = "_id";
    public static final String DESCRIPTION_STRING = "description";
    public static final String UPDATED_AT_STRING = "updated_at";

    public Rule(String description, Map<Attribute, Set<String>> attributeMap, T featureType, String label, String updatedAt) {
        RuleValidator<T> validator = new RuleValidator<>(description, attributeMap, label, updatedAt, featureType);
        validator.validate();

        this.description = description;
        this.attributeMap = attributeMap;
        this.featureType = featureType;
        this.label = label;
        this.updatedAt = updatedAt;
    }

    @SuppressWarnings("unchecked")
    public Rule(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readMap(Attribute::from, i -> new HashSet<>(i.readStringList())),
            (T) FeatureType.from(in),
            in.readString(),
            in.readString()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(description);
        out.writeMap(attributeMap, (output, attribute) -> attribute.writeTo(output), StreamOutput::writeStringCollection);
        featureType.writeTo(out);
        out.writeString(label);
        out.writeString(updatedAt);
    }

    public static <T extends FeatureType> Rule<T> fromXContent(final XContentParser parser, T featureType) throws IOException {
        return Builder.fromXContent(parser, featureType).build();
    }

    public String getDescription() {
        return description;
    }

    public String getLabel() {
        return label;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public T getFeatureType() {
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
        builder.field(featureType.getName(), label);
        builder.field(UPDATED_AT_STRING, updatedAt);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rule<?> that = (Rule<?>) o;
        return Objects.equals(description, that.description)
            && Objects.equals(label, that.label)
            && Objects.equals(featureType, that.featureType)
            && Objects.equals(attributeMap, that.attributeMap)
            && Objects.equals(updatedAt, that.updatedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, label, featureType, attributeMap, updatedAt);
    }

    /**
     * builder method for the {@link Rule}
     * @return Builder object
     */
    public static <T extends FeatureType> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Builder class for {@link Rule}
     * @opensearch.experimental
     */
    public static class Builder<T extends FeatureType> {
        private String description;
        private Map<Attribute, Set<String>> attributeMap;
        private T featureType;
        private String label;
        private String updatedAt;

        private Builder() {}

        public static <T extends FeatureType> Builder<T> fromXContent(XContentParser parser, T featureType) throws IOException {
            if (parser.currentToken() == null) {
                parser.nextToken();
            }
            Builder<T> builder = builder();
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
                        builder.label(parser.text());
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    fromXContentParseArray(parser, fieldName, featureType, attributeMap1);
                }
            }
            return builder.attributeMap(attributeMap1);
        }

        public static <T extends FeatureType> void fromXContentParseArray(
            XContentParser parser,
            String fieldName,
            T featureType,
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

        public Builder<T> description(String description) {
            this.description = description;
            return this;
        }

        public Builder<T> label(String label) {
            this.label = label;
            return this;
        }

        public Builder<T> attributeMap(Map<Attribute, Set<String>> attributeMap) {
            this.attributeMap = attributeMap;
            return this;
        }

        public Builder<T> featureType(T featureType) {
            this.featureType = featureType;
            return this;
        }

        public Builder<T> updatedAt(String updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Rule<T> build() {
            return new Rule<>(description, attributeMap, featureType, label, updatedAt);
        }

        public String getLabel() {
            return label;
        }

        public Map<Attribute, Set<String>> getAttributeMap() {
            return attributeMap;
        }
    }
}

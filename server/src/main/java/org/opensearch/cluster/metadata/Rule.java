/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.wlm.ResourceType;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@ExperimentalApi
public class Rule extends AbstractDiffable<Rule> implements ToXContentObject {
    private final String _id;
    private final Map<RuleAttribute, List<String>> attributeMap;
    private final String label;
    private final String updatedAt;
    private final Feature feature;

    public Rule(String _id, Map<RuleAttribute, List<String>> attributeMap, String label, String updatedAt, Feature feature) {
        this._id = _id;
        this.attributeMap = attributeMap;
        this.label = label;
        this.updatedAt = updatedAt;
        this.feature = feature;
    }

    public Rule(StreamInput in) throws IOException {
        this(in.readString(),
            in.readMap((i) -> RuleAttribute.fromName(i.readString()), StreamInput::readStringList),
            in.readString(), in.readString(),
            Feature.fromName(in.readString()));
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

    public Map<RuleAttribute, List<String>> getAttributeMap() {
        return attributeMap;
    }

    @ExperimentalApi
    public enum Feature {
        WLM("WLM");

        private final String name;

        Feature(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static Feature fromName(String s) {
            for (Feature feature : values()) {
                if (feature.getName().equalsIgnoreCase(s)) return feature;

            }
            throw new IllegalArgumentException("Invalid value for Feature: " + s);
        }
    }

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
        for (Map.Entry<RuleAttribute, List<String>> entry : attributeMap.entrySet()) {
            builder.array(entry.getKey().getName(), entry.getValue().toArray(new String[0]));
        }
        builder.field("label", label);
        builder.field("feature", feature);
        builder.field("updated_at", updatedAt);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentWithoutId(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<RuleAttribute, List<String>> entry : attributeMap.entrySet()) {
            builder.array(entry.getKey().getName(), entry.getValue().toArray(new String[0]));
        }
        builder.field("label", label);
        builder.field("feature", feature);
        builder.field("updated_at", updatedAt);
        builder.endObject();
        return builder;
    }

    public static Diff<Rule> readDiff(final StreamInput in) throws IOException {
        return readDiffFrom(Rule::new, in);
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
     */
    @ExperimentalApi
    public static class Builder {
        private String _id;
        private Map<RuleAttribute, List<String>> attributeMap;
        private String label;
        private String updatedAt;
        private Feature feature;

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

            Map<RuleAttribute, List<String>> attributeMap1 = new HashMap<>();
            String fieldName = "";
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (fieldName.equals("_id")) {
                        builder._id(parser.text());
                    } else if (fieldName.equals("label")) {
                        builder.label(parser.text());
                    } else if (fieldName.equals("feature")) {
                        builder.feature(parser.text());
                    } else if (fieldName.equals("updated_at")) {
                        builder.updatedAt(parser.text());
                    } else {
                        throw new IllegalArgumentException(fieldName + " is not a valid field in Rule");
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    RuleAttribute ruleAttribute = RuleAttribute.fromName(fieldName);
                    List<String> indexPatternList = new ArrayList<>();
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

        public Builder attributeMap(Map<RuleAttribute, List<String>> attributeMap) {
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
    }
}

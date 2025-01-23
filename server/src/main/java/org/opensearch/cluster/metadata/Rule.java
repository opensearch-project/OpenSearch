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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ExperimentalApi
public class Rule extends AbstractDiffable<Rule> implements ToXContentObject {
    private final String _id;
    private final Map<String, List<String>> attributeMap;
    private final String label;
    private final String updatedAt;
    private final Feature feature;

    public Rule(String _id, Map<String, List<String>> attributeMap, String label, String updatedAt, Feature feature) {
        this._id = _id;
        this.attributeMap = attributeMap;
        this.label = label;
        this.updatedAt = updatedAt;
        this.feature = feature;
    }

    public Rule(StreamInput in) throws IOException {
        this(in.readString(), in.readMap(StreamInput::readString, StreamInput::readStringList), in.readString(), in.readString(), Feature.fromName(in.readString()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(attributeMap, StreamOutput::writeString, StreamOutput::writeStringCollection);
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

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("_id", _id);
        builder.field("label", label);
        builder.field("feature", feature);
        builder.field("updated_at", updatedAt);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentWithoutId(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
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
        private Map<String, List<String>> attributeMap;
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

            Map<String, List<String>> attributeMap1 = new HashMap<>();
            String fieldName = "";
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (fieldName.equals("index_pattern")) {
                        if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                            List<String> indexPatternList = parser.list().stream()
                                .map(value -> (String) value)
                                .collect(Collectors.toList());
                            attributeMap1.put("index_pattern", indexPatternList);
                        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                            attributeMap1.put("index_pattern", Collections.singletonList(parser.text()));
                        } else {
                            throw new XContentParseException("Unexpected token for index_pattern: " + parser.currentToken());
                        }
                    } else if (fieldName.equals("_id")) {
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

        public Builder attributeMap(Map<String, List<String>> attributeMap) {
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

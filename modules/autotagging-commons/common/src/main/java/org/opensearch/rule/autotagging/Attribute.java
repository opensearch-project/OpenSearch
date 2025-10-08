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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rule.MatchLabel;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.storage.AttributeValueStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents an attribute within the auto-tagging feature. Attributes define characteristics that can
 * be used for tagging and classification. Implementations must ensure that attributes
 * are uniquely identifiable by their name. Attributes should be singletons and managed centrally to
 * avoid duplicates.
 *
 * @opensearch.experimental
 */
public interface Attribute extends Writeable {
    /**
     * Returns the attribute string representation
     * @return
     */
    String getName();

    /**
     * Returns a map of subfields with its weight, which is used to calculate the match score for the attribute.
     */
    default Map<String, Float> getWeightedSubfields() {
        return new HashMap<>();
    }

    /**
     * Ensure that `validateAttribute` is called in the constructor of attribute implementations
     * to prevent potential serialization issues.
     */
    default void validateAttribute() {
        String name = getName();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Attribute name cannot be null or empty");
        }
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        out.writeString(getName());
    }

    /**
     * Parses attribute values for specific attributes. This default function takes in parser
     * and returns a set of string.
     * For example, ["index1", "index2"] will be parsed to a set with values "index1" and "index2"
     * @param parser the XContent parser
     */
    default Set<String> fromXContentParseAttributeValues(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "Expected START_ARRAY token for " + getName() + " attribute but got " + parser.currentToken()
            );
        }
        Set<String> attributeValueSet = new HashSet<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                attributeValueSet.add(parser.text());
            } else {
                throw new XContentParseException("Unexpected token in array: " + parser.currentToken());
            }
        }
        return attributeValueSet;
    }

    /**
     * Writes a set of attribute values for a specific attribute
     * @param builder the XContent builder
     * @param values the set of string values to write
     */
    default void toXContentWriteAttributeValues(XContentBuilder builder, Set<String> values) throws IOException {
        builder.array(getName(), values.toArray(new String[0]));
    }

    /**
     * Retrieves an attribute from the given feature type based on its name.
     * Implementations of `FeatureType.getAttributeFromName` must be thread-safe as this method
     * may be called concurrently.
     * @param in - the {@link StreamInput} from which the attribute name is read
     * @param featureType - the FeatureType used to look up the attribute
     */
    static Attribute from(StreamInput in, FeatureType featureType) throws IOException {
        String attributeName = in.readString();
        Attribute attribute = featureType.getAttributeFromName(attributeName);
        if (attribute == null) {
            throw new IllegalStateException(attributeName + " is not a valid attribute under feature type " + featureType.getName());
        }
        return attribute;
    }

    /**
     * Evaluates the matching labels for the attribute
     * @param attributeExtractor the extractor to get the attribute
     * @param attributeValueStore in-memory value store for the attribute
     */
    default List<MatchLabel<String>> findAttributeMatches(
        AttributeExtractor<String> attributeExtractor,
        AttributeValueStore<String, String> attributeValueStore
    ) {
        Map<String, Float> scoreMap = new HashMap<>();

        for (String value : attributeExtractor.extract()) {
            List<MatchLabel<String>> matches = attributeValueStore.getMatches(value);
            for (MatchLabel<String> entry : matches) {
                scoreMap.merge(entry.getFeatureValue(), entry.getMatchScore(), Float::sum);
            }
        }
        return scoreMap.entrySet()
            .stream()
            .map(e -> new MatchLabel<>(e.getKey(), e.getValue()))
            .sorted((a, b) -> Float.compare(b.getMatchScore(), a.getMatchScore()))
            .collect(Collectors.toList());
    }
}

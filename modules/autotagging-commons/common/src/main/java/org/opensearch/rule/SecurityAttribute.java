/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rule.autotagging.Attribute;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Security attribute for the rules. Example:
 * principal: {
 *   "username": ["alice", "bob"],
 *   "role": ["admin"]
 * }
 * @opensearch.experimental
 */
public enum SecurityAttribute implements Attribute {
    /**
     * Represents the principal attribute
     */
    PRINCIPAL("principal");

    /**
     * Represents the username subfield
     */
    public static final String USERNAME = "username";
    /**
     * Represents the role subfield
     */
    public static final String ROLE = "role";
    private static final TreeMap<Integer, String> PRIORITIZED_SUBFIELDS = new TreeMap<>(Map.of(1, USERNAME, 2, ROLE));
    private final String name;

    SecurityAttribute(String name) {
        this.name = name;
        validateAttribute();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TreeMap<Integer, String> getPrioritizedSubfields() {
        return PRIORITIZED_SUBFIELDS;
    }

    /**
     * Parses the attribute values for security attribute
     * Example:
     * {
     *     "username": ["alice"],
     *     "role": ["all_access"]
     * }
     * will be parsed into a set with values "username|alice" and "role|all_access"
     * @param parser the XContent parser
     */
    @Override
    public Set<String> fromXContentParseAttributeValues(XContentParser parser) throws IOException {
        Set<String> resultSet = new HashSet<>();

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "Expected START_OBJECT token for " + getName() + " attribute but got " + parser.currentToken()
            );
        }
        List<String> allowedSubfieldsName = PRIORITIZED_SUBFIELDS.values().stream().toList();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String subFieldName = parser.currentName();
            parser.nextToken();
            if (!allowedSubfieldsName.contains(subFieldName)) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "Invalid field: " + subFieldName + ". Allowed fields are: " + String.join(", ", allowedSubfieldsName)
                );
            }
            resultSet.addAll(
                Attribute.super.fromXContentParseAttributeValues(parser).stream()
                    .map(s -> subFieldName + '|' + s)
                    .collect(Collectors.toSet())
            );
        }

        return resultSet;
    }

    @Override
    public void toXContentWriteAttributeValues(XContentBuilder builder, Set<String> values) throws IOException {
        builder.startObject(getName());
        Map<String, Set<String>> grouped = new HashMap<>();
        // For each string in the values set, split it into two parts using the first '|' as delimiter:
        // parts[0] is the prefix (e.g., "username" or "role")
        // parts[1] is the actual value (e.g., "name1", "role1")
        for (String value : values) {
            String[] parts = value.split("\\|", 2);
            if (parts.length == 2) {
                grouped.computeIfAbsent(parts[0], k -> new HashSet<>()).add(parts[1]);
            }
        }
        for (Map.Entry<String, Set<String>> entry : grouped.entrySet()) {
            builder.array(entry.getKey(), entry.getValue().toArray(new String[0]));
        }
        builder.endObject();
    }
}

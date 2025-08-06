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
     * Represents the index_pattern attribute in RuleAttribute
     */
    PRINCIPAL("principal");

    /**
     * Key representing the username subfield.
     */
    public static final String USERNAME = "username";
    /**
     * Key representing the role subfield.
     */
    public static final String ROLE = "role";
    private final String name;
    private static final List<String> ALLOWED_SUBFIELDS = List.of(USERNAME, ROLE);

    SecurityAttribute(String name) {
        this.name = name;
        validateAttribute();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<String> getPrioritizedSubfields() {
        return ALLOWED_SUBFIELDS;
    }

    @Override
    public Set<String> fromXContentParseAttributeValues(XContentParser parser) throws IOException {
        Set<String> resultSet = new HashSet<>();

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "Expected START_OBJECT token for " + getName() + " attribute but got " + parser.currentToken()
            );
        }
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String subFieldName = parser.currentName();
            parser.nextToken();
            if (!ALLOWED_SUBFIELDS.contains(subFieldName)) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "Invalid field: " + subFieldName + ". Allowed fields are: " + String.join(", ", ALLOWED_SUBFIELDS)
                );
            }
            if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "Expected array for field: " + subFieldName + " but got " + parser.currentToken()
                );
            }
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                    // prefix each value with the subFieldName (e.g., "username_name1")
                    resultSet.add(String.join("_", subFieldName, parser.text()));
                } else {
                    throw new XContentParseException(
                        parser.getTokenLocation(),
                        "Expected string value in array under '" + subFieldName + "', but got " + parser.currentToken()
                    );
                }
            }
        }

        return resultSet;
    }

    @Override
    public void toXContentWriteAttributeValues(XContentBuilder builder, Set<String> values) throws IOException {
        builder.startObject(getName());
        Map<String, Set<String>> grouped = new HashMap<>();

        // For each string in the values set, split it into two parts using the first underscore as delimiter:
        // parts[0] is the prefix (e.g., "username" or "role")
        // parts[1] is the actual value (e.g., "name1", "role1")
        for (String value : values) {
            String[] parts = value.split("_", 2);
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

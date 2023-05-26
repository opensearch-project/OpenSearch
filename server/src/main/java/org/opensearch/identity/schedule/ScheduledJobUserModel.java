/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.schedule;

import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class that represents a Scheduled Job User
 */
public class ScheduledJobUserModel implements ToXContentObject {
    private final String username;
    private final Map<String, String> attributes;

    public ScheduledJobUserModel(String username, Map<String, String> attributes) {
        this.username = username;
        this.attributes = attributes;
    }

    public String getUsername() {
        return username;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public static ScheduledJobUserModel parse(XContentParser parser) throws IOException {
        String username = null;
        Map<String, String> attributes = new HashMap<>();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        while(!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case "username":
                    username = parser.text();
                    break;
                default:
                    String attribute = parser.text();
                    attributes.put(fieldName, attribute);
                    break;
            }
        }

        return new ScheduledJobUserModel((String) Objects.requireNonNull(username, "username cannot be null"), (Map<String, String>)Objects.requireNonNull(attributes, "attributes cannot be null"));
    }
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("username", this.username).field("attributes", this.attributes).endObject();
        return builder;
    }
}

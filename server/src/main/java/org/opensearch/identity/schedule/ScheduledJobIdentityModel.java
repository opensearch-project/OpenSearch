/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.schedule;

import java.io.IOException;

import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Class that represents a Scheduled Job Identity
 */
public class ScheduledJobIdentityModel implements ToXContentObject {
    public static final String USER = "user";

    public static final String TOKEN = "token";
    private final ScheduledJobUserModel user;
    private final String authToken;

    public ScheduledJobIdentityModel(final ScheduledJobUserModel user, final String authToken) {
        this.user = user;
        this.authToken = authToken;
    }

    public ScheduledJobUserModel getUser() {
        return this.user;
    }

    public String getAuthToken() {
        return this.authToken;
    }

    public static ScheduledJobIdentityModel parse(final XContentParser parser) throws IOException {
        ScheduledJobUserModel user = null;
        String authToken = null;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case TOKEN:
                    authToken = parser.currentToken() == XContentParser.Token.VALUE_NULL ? null : parser.text();
                    break;
                case USER:
                    user = parser.currentToken() == XContentParser.Token.VALUE_NULL ? null : ScheduledJobUserModel.parse(parser);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field " + fieldName);
            }
        }
        if (user == null && authToken == null) {
            throw new IllegalArgumentException("Either user or token needs to be present.");
        }
        return new ScheduledJobIdentityModel(user, authToken);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOKEN, authToken);
        builder.field(USER, user);
        builder.endObject();
        return builder;
    }

}


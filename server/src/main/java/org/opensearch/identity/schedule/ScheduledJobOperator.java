/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.schedule;

import java.io.IOException;
import java.util.Objects;

import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Class that represents a Scheduled Job Operator
 */
public class ScheduledJobOperator implements ToXContentObject {
    public static final String OPERATOR = "operator";
    private final ScheduledJobIdentityModel identity;

    public ScheduledJobOperator(final ScheduledJobIdentityModel identity) {
        this.identity = identity;
    }

    public ScheduledJobIdentityModel getIdentity() {
        return this.identity;
    }

    public static ScheduledJobOperator parse(final XContentParser parser) throws IOException {
        ScheduledJobIdentityModel identity = null;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case OPERATOR:
                    identity = ScheduledJobIdentityModel.parse(parser);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field " + fieldName);
            }
        }
        return new ScheduledJobOperator((ScheduledJobIdentityModel) Objects.requireNonNull(identity, "operator cannot be null"));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(OPERATOR, identity);
        builder.endObject();
        return builder;
    }

}


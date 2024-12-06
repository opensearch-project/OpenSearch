/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * This class contains information on the creator of a resource.
 * Creator can either be a user or a backend_role.
 *
 * @opensearch.experimental
 */
public class CreatedBy implements ToXContentFragment, NamedWriteable {

    private String user;

    public CreatedBy(String user) {
        this.user = user;
    }

    public CreatedBy(StreamInput in) throws IOException {
        this(in.readString());
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public String toString() {
        return "CreatedBy {" + "user='" + user + '\'' + '}';
    }

    @Override
    public String getWriteableName() {
        return "created_by";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(user);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("user", user).endObject();
    }

    public static CreatedBy fromXContent(XContentParser parser) throws IOException {
        String user = null;
        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("user".equals(currentFieldName)) {
                    user = parser.text();
                }
            }
        }

        if (user == null) {
            throw new IllegalArgumentException("user field is required");
        }

        return new CreatedBy(user);
    }

}

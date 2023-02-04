/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.get.single;

import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Transport response for user get request
 * Represents a user object to be returned
 */
public class GetUserResponseInfo extends TransportResponse implements Writeable, ToXContent {
    private final String username;
    private final Map<String, String> attributes;
    private final Set<String> permissions;

    public GetUserResponseInfo(String username, Map<String, String> attributes, Set<String> permissions) {
        this.attributes = attributes;
        this.permissions = permissions;
        this.username = username;
    }

    public GetUserResponseInfo(StreamInput in) throws IOException {
        super(in);
        username = in.readString();
        attributes = in.readMap(StreamInput::readString, StreamInput::readString);
        permissions = in.readSet(StreamInput::readString);
    }

    public String getUsername() {
        return username;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Set<String> getPermissions() {
        return permissions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(username);
        out.writeMap(attributes, StreamOutput::writeString, StreamOutput::writeString);
        out.writeStringCollection(permissions);
    }

    private static final ConstructingObjectParser<Map<String, String>, Void> ATTRIBUTE_PARSER = new ConstructingObjectParser<>(
        "attributes",
        true,
        args -> Map.of((String) args[0], (String) args[1])
    );

    private static final ConstructingObjectParser<List<String>, Void> PERMISSIONS_PARSER = new ConstructingObjectParser<>(
        "permissions",
        true,
        args -> List.of((String) args[0])
    );

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetUserResponseInfo, Void> PARSER = new ConstructingObjectParser<>(
        "get_user_response_info",
        true,
        args -> new GetUserResponseInfo((String) args[0], (Map<String, String>) args[1], (Set<String>) args[2])
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("username"));
        PARSER.declareObject(constructorArg(), ATTRIBUTE_PARSER, new ParseField("attributes"));
        PARSER.declareObject(constructorArg(), PERMISSIONS_PARSER, new ParseField("permissions"));
    }
    private static final ParseField USERNAME = new ParseField("username");
    private static final ParseField ATTRIBUTES = new ParseField("attributes");
    private static final ParseField PERMISSIONS = new ParseField("permissions");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(USERNAME.getPreferredName(), username);
        builder.field(ATTRIBUTES.getPreferredName(), attributes);
        builder.field(PERMISSIONS.getPreferredName(), permissions);
        builder.endObject();
        return builder;
    }

}

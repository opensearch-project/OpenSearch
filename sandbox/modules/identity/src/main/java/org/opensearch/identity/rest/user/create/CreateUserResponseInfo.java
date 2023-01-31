/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.create;

import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * This class captures if creation of user is successful along with its username
 */
public class CreateUserResponseInfo extends TransportResponse implements Writeable, ToXContent {
    private final boolean successful;
    private final String username;

    public CreateUserResponseInfo(boolean successful, String username) {
        this.successful = successful;
        this.username = username;
    }

    public CreateUserResponseInfo(StreamInput in) throws IOException {
        successful = in.readBoolean();
        username = in.readString();

    }

    public boolean isSuccessful() {
        return successful;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(successful);
        out.writeString(username);
    }

    static final ConstructingObjectParser<CreateUserResponseInfo, Void> PARSER = new ConstructingObjectParser<>(
        "create_user_response_info",
        true,
        args -> new CreateUserResponseInfo((boolean) args[0], (String) args[1])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("successful"));
        PARSER.declareString(constructorArg(), new ParseField("username"));
    }

    private static final ParseField SUCCESSFUL = new ParseField("successful");
    private static final ParseField USERNAME = new ParseField("username");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESSFUL.getPreferredName(), successful);
        builder.field(USERNAME.getPreferredName(), username);
        builder.endObject();
        return builder;
    }

}
